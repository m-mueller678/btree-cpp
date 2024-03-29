#include "btree2020.hpp"
#include "head.hpp"

Tag AnyNode::tag()
{
   Tag t=_tag_and_dirty.tag();
   ASSUME(t == Tag::Inner || t == Tag::Leaf || t == Tag::Dense || t == Tag::Hash || t == Tag::Head4 || t == Tag::Head8 || t == Tag::Dense2);
   ASSUME(enableDense || t != Tag::Dense);
   ASSUME((enableDense2 && !enableHash) || t != Tag::Dense2);
   ASSUME(enableHash || t != Tag::Hash);
   ASSUME(!enableHash || enableHashAdapt || t != Tag::Leaf);
   ASSUME(enableHeadNode || t != Tag::Head4);
   ASSUME(enableHeadNode || t != Tag::Head8);
   return t;
}

AnyNode::AnyNode(BTreeNode basic) : AnyNode()
{
   _basic_node = basic;
}

AnyNode::AnyNode()
{
   // COUNTER(anynode_new,1,1<<5);
}

void AnyNode::destroy()
{
   switch (tag()) {
      case Tag::Leaf:
      case Tag::Inner:
         return basic()->destroy();
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         return dealloc();
      case Tag::Head4:
         return head4()->destroy();
      case Tag::Head8:
         return head8()->destroy();
   }
}

void AnyNode::dealloc()
{
   std::free(this);
}

AnyNode* AnyNode::allocLeaf()
{
   return reinterpret_cast<AnyNode*>(std::aligned_alloc(alignof(AnyNode), pageSizeLeaf));
}
AnyNode* AnyNode::allocInner()
{
   return reinterpret_cast<AnyNode*>(std::aligned_alloc(alignof(AnyNode), pageSizeInner));
}

bool AnyNode::isAnyInner()
{
   switch (tag()) {
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         return false;
      case Tag::Inner:
      case Tag::Head4:
      case Tag::Head8:
         return true;
   }
}

BTreeNode* AnyNode::basic()
{
   Tag t=_tag_and_dirty.tag();
   ASSUME(t == Tag::Leaf || t == Tag::Inner);
   return reinterpret_cast<BTreeNode*>(this);
}

DenseNode* AnyNode::dense()
{
   Tag t=_tag_and_dirty.tag();
   ASSUME(t == Tag::Dense || t == Tag::Dense2);
   return reinterpret_cast<DenseNode*>(this);
}

HashNode* AnyNode::hash()
{
   Tag t=_tag_and_dirty.tag();
   ASSUME(t == Tag::Hash);
   return reinterpret_cast<HashNode*>(this);
}

HeadNode<uint32_t>* AnyNode::head4()
{
   Tag t=_tag_and_dirty.tag();
   ASSUME(t == Tag::Head4);
   return reinterpret_cast<HeadNode<uint32_t>*>(this);
}

HeadNode<uint64_t>* AnyNode::head8()
{
   Tag t=_tag_and_dirty.tag();
   ASSUME(t == Tag::Head8);
   return reinterpret_cast<HeadNode<uint64_t>*>(this);
}

bool AnyNode::insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child)
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->insertChild(key, keyLength, child);
      case Tag::Head4:
         return head4()->insertChild(key, keyLength, child);
      case Tag::Head8:
         return head8()->insertChild(key, keyLength, child);
      case Tag::Leaf:
      case Tag::Hash:
      case Tag::Dense:
      case Tag::Dense2:
         ASSUME(false);
   }
}

bool AnyNode::innerRequestSpaceFor(unsigned keyLen)
{
   switch (tag()) {
      case Tag::Inner: {
         bool succ = basic()->requestSpaceFor(basic()->spaceNeeded(keyLen, sizeof(AnyNode*)));
         if (enableHeadNode && !succ) {
            return HeadNodeHead::requestChildConvertFromBasic(basic(), keyLen);
         }
         return succ;
      }
      case Tag::Head4:
         return head4()->requestSpaceFor(keyLen);
      case Tag::Head8:
         return head8()->requestSpaceFor(keyLen);
      case Tag::Leaf:
      case Tag::Hash:
      case Tag::Dense:
      case Tag::Dense2:
         ASSUME(false);
   }
}

AnyNode* AnyNode::lookupInner(uint8_t* key, unsigned keyLength)
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->lookupInner(key, keyLength);
      case Tag::Head4:
         return head4()->lookupInner(key, keyLength);
      case Tag::Head8:
         return head8()->lookupInner(key, keyLength);
      case Tag::Leaf:
      case Tag::Hash:
      case Tag::Dense:
      case Tag::Dense2:
         ASSUME(false);
   }
}

AnyNode* AnyNode::makeRoot(AnyNode* child)
{
   AnyNode* ptr = AnyNode::allocInner();
   if (enableHeadNode) {
      ptr->_head4.init(nullptr, 0, nullptr, 0);
      storeUnaligned(ptr->head4()->children(), child);
      return ptr;
   } else {
      ptr->_basic_node.init(false,RangeOpCounter{});
      ptr->basic()->upper = child;
      return ptr;
   }
}

void AnyNode::print()
{
   switch (tag()) {
      case Tag::Inner:
      case Tag::Leaf:
         return basic()->print();
      case Tag::Head8:
         return head8()->print();
      case Tag::Head4:
         return head4()->print();
      case Tag::Hash:
         return hash()->print();
      case Tag::Dense:
      case Tag::Dense2:
         return dense()->print();
   }
}

unsigned AnyNode::lookupInnerIndex(uint8_t* key, unsigned keyLength)
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->lowerBound(key, keyLength);
      case Tag::Head4:
         return head4()->lookupInnerIndex(key, keyLength);
      case Tag::Head8:
         return head8()->lookupInnerIndex(key, keyLength);
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         ASSUME(false);
   }
}

unsigned AnyNode::innerCount()
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->count;
      case Tag::Head4:
         return head4()->count;
      case Tag::Head8:
         return head8()->count;
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         ASSUME(false);
   }
}

AnyNode* AnyNode::getChild(unsigned index)
{
   switch (tag()) {
      case Tag::Inner: {
         if (index == basic()->count)
            return basic()->upper;
         return basic()->getChild(index);
      }
      case Tag::Head4:
         return loadUnaligned<AnyNode*>(head4()->children() + index);
      case Tag::Head8:
         return loadUnaligned<AnyNode*>(head8()->children() + index);
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         ASSUME(false);
   }
}

void AnyNode::innerRestoreKey(uint8_t* keyOut, unsigned len, unsigned index)
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->restoreKey(keyOut, len, index);
      case Tag::Head4:
         return head4()->restoreKey(keyOut, len, index);
      case Tag::Head8:
         return head8()->restoreKey(keyOut, len, index);
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         ASSUME(false);
   }
}

void AnyNode::innerRemoveSlot(unsigned int slotId)
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->removeSlot(slotId);
      case Tag::Head4:
         return head4()->removeSlot(slotId);
      case Tag::Head8:
         return head8()->removeSlot(slotId);
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         ASSUME(false);
   }
}

unsigned AnyNode::innerKeyLen(unsigned index)
{
   switch (tag()) {
      case Tag::Inner:
         return basic()->slot[index].keyLen + basic()->prefixLength;
      case Tag::Head4:
         return head4()->getKeyLength(index);
      case Tag::Head8:
         return head8()->getKeyLength(index);
      case Tag::Leaf:
      case Tag::Dense:
      case Tag::Dense2:
      case Tag::Hash:
         ASSUME(false);
   }
}

bool AnyNode::splitNodeWithParent(AnyNode* parent, uint8_t* key, unsigned keyLength)
{
   Tag tag=this->tag();
   switch (tag) {
      case Tag::Leaf:
         if(enableDensifySplit){
            uint8_t sepBuffer[BTreeNode::maxKVSize];
            auto sep = DenseNode::densifySplit(sepBuffer,basic());
            if(sep.lowerCount!=0){
               if (parent->innerRequestSpaceFor(sep.fenceLen)){
                  bool found;
                  unsigned index = basic()->lowerBound(sepBuffer,sep.fenceLen,found);
                  assert(sep.lowerCount == index+found);
                  basic()->splitNode(parent,sep.lowerCount-1,sepBuffer,sep.fenceLen);
                  return true;
               }else{
                  return false;
               }
            }
         }
         // continue with normal node split
      case Tag::Inner: {
         BTreeNode::SeparatorInfo sepInfo = basic()->findSeparator();
         if (parent->innerRequestSpaceFor(sepInfo.length)) {  // is there enough space in the parent for the separator?
            uint8_t sepKey[sepInfo.length];
            basic()->getSep(sepKey, sepInfo);
            basic()->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
            return true;
         } else {
            return false;
         }
      }
      case Tag::Dense:
      case Tag::Dense2: {
         if (parent->innerRequestSpaceFor(dense()->fullKeyLen)) {  // is there enough space in the parent for the separator?
            if (tag == Tag::Dense)
               dense()->splitNode1(parent, key, keyLength);
            else
               dense()->splitNode2(parent, key, keyLength);
            return true;
         } else {
            return false;
         }
         break;
      }
      case Tag::Hash: {
         hash()->sort();
         BTreeNode::SeparatorInfo sepInfo = hash()->findSeparator();
         if (parent->innerRequestSpaceFor(sepInfo.length)) {  // is there enough space in the parent for the separator?
            uint8_t sepKey[sepInfo.length];
            hash()->getSep(sepKey, sepInfo);
            hash()->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
            return true;
         } else {
            return false;
         }
         break;
      }
      case Tag::Head4: {
         BTreeNode::SeparatorInfo sepInfo = head4()->findSeparator();
         if (parent->innerRequestSpaceFor(sepInfo.length)) {  // is there enough space in the parent for the separator?
            uint8_t sepKey[sepInfo.length];
            head4()->getSep(sepKey, sepInfo);
            head4()->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
            return true;
         } else {
            return false;
         }
         break;
      }
      case Tag::Head8: {
         BTreeNode::SeparatorInfo sepInfo = head8()->findSeparator();
         if (parent->innerRequestSpaceFor(sepInfo.length)) {  // is there enough space in the parent for the separator?
            uint8_t sepKey[sepInfo.length];
            head8()->getSep(sepKey, sepInfo);
            head8()->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
            return true;
         } else {
            return false;
         }
         break;
      }
   }
}

void AnyNode::nodeCount(unsigned counts[int(Tag::_last) + 2])
{
   int key_count_index = int(Tag::_last) + 1;
   counts[unsigned(tag())] += 1;
   switch (tag()) {
      case Tag::Inner:
         for (int i = 0; i < basic()->count; ++i) {
            basic()->getChild(i)->nodeCount(counts);
         }
         basic()->upper->nodeCount(counts);
         break;
      case Tag::Head4:
         for (int i = 0; i <= head4()->count; ++i) {
            head4()->children()[i]->nodeCount(counts);
         }
         break;
      case Tag::Head8:
         for (int i = 0; i <= head8()->count; ++i) {
            head8()->children()[i]->nodeCount(counts);
         }
         break;
      case Tag::Leaf:
         counts[key_count_index] += basic()->count;
         break;
      case Tag::Dense:
         counts[key_count_index] += dense()->occupiedCount;
         break;
      case Tag::Dense2:
         counts[key_count_index] += dense()->occupiedCount;
         break;
      case Tag::Hash:
         counts[key_count_index] += hash()->count;
         break;
   }
}

const char* tag_name(Tag tag)
{
   switch (tag) {
#define T(L)    \
   case Tag::L: \
      return #L;
      T(Leaf)
      T(Inner)
      T(Dense)
      T(Hash)
      T(Head4)
      T(Head8)
      T(Dense2)
#undef T
   }
}