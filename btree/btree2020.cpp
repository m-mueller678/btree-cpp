#include "btree2020.hpp"
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <functional>

BTreeNodeHeader::BTreeNodeHeader(bool isLeaf,RangeOpCounter roc) : _tag(isLeaf ? Tag::Leaf : Tag::Inner), rangeOpCounter(roc), dataOffset(isLeaf ? pageSizeLeaf : pageSizeInner) {}

uint8_t* BTreeNode::ptr()
{
   return reinterpret_cast<uint8_t*>(this);
}
bool BTreeNode::isInner()
{
   switch (_tag) {
      case Tag::Inner:
         return true;
      case Tag::Leaf:
         return false;
      default:
         ASSUME(false);
   }
}
bool BTreeNode::isLeaf()
{
   switch (_tag) {
      case Tag::Inner:
         return false;
      case Tag::Leaf:
         return true;
      default:
         ASSUME(false);
   }
}
uint8_t* BTreeNode::getLowerFence()
{
   return ptr() + lowerFence.offset;
}
uint8_t* BTreeNode::getUpperFence()
{
   return ptr() + upperFence.offset;
}
uint8_t* BTreeNode::getPrefix()
{
   return ptr() + lowerFence.offset;
}  // any key on page is ok

unsigned BTreeNode::freeSpace()
{
   return dataOffset - (reinterpret_cast<uint8_t*>(slot + count) - ptr());
}
unsigned BTreeNode::freeSpaceAfterCompaction()
{
   return (isLeaf() ? pageSizeLeaf : pageSizeInner) - (reinterpret_cast<uint8_t*>(slot + count) - ptr()) - spaceUsed;
}

bool BTreeNode::requestSpaceFor(unsigned spaceNeeded)
{
   if (spaceNeeded <= freeSpace())
      return true;
   if (spaceNeeded <= freeSpaceAfterCompaction()) {
      compactify();
      return true;
   }
   return false;
}

void BTreeNode::init(bool isLeaf,RangeOpCounter roc)
{
   auto * header=static_cast<BTreeNodeHeader*>(this);
   *header= BTreeNodeHeader(isLeaf,roc);
}

AnyNode* BTreeNode::makeLeaf()
{
   AnyNode* r = AnyNode::allocLeaf();
   r->_basic_node.init(true,RangeOpCounter{});
   return r;
}

uint8_t* BTreeNode::getKey(unsigned slotId)
{
   return ptr() + slot[slotId].offset;
}
uint8_t* BTreeNode::getPayload(unsigned slotId)
{
   return ptr() + slot[slotId].offset + slot[slotId].keyLen;
}

AnyNode* BTreeNode::getChild(unsigned slotId)
{
   assert(isInner());
   return loadUnaligned<AnyNode*>(getPayload(slotId));
}

// How much space would inserting a new key of length "getKeyLength" require?
unsigned BTreeNode::spaceNeeded(unsigned keyLength, unsigned payloadLength)
{
   ASSUME(enablePrefix || prefixLength == 0);
   ASSUME(keyLength >= prefixLength);  // fence key logic makes it impossible to insert a key that is shorter than prefix
   return sizeof(Slot) + (keyLength - prefixLength) + payloadLength;
}

void BTreeNode::makeHint()
{
   unsigned dist = count / (hintCount + 1);
   for (unsigned i = 0; i < hintCount; i++)
      hint[i] = slot[dist * (i + 1)].head[0];
}

void BTreeNode::updateHint(unsigned slotId)
{
   unsigned dist = count / (hintCount + 1);
   unsigned begin = 0;
   if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
   for (unsigned i = begin; i < hintCount; i++)
      hint[i] = slot[dist * (i + 1)].head[0];
}

void BTreeNode::searchHint(uint32_t keyHead, unsigned& lowerOut, unsigned& upperOut)
{
   if (hintCount == 0)
      return;
   if (count > hintCount * 2) {
      unsigned dist = upperOut / (hintCount + 1);
      unsigned pos, pos2;
      for (pos = 0; pos < hintCount; pos++)
         if (hint[pos] >= keyHead)
            break;
      for (pos2 = pos; pos2 < hintCount; pos2++)
         if (hint[pos2] != keyHead)
            break;
      lowerOut = pos * dist;
      if (pos2 < hintCount)
         upperOut = (pos2 + 1) * dist;
   }
}

// lower bound search, foundOut indicates if there is an exact match, returns slotId
unsigned BTreeNode::lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut)
{
   // validateHint();
   foundOut = false;

   // skip prefix
   ASSUME(enablePrefix || prefixLength == 0);
   assert(memcmp(key, getPrefix(), min(keyLength, prefixLength)) == 0);
   key += prefixLength;
   keyLength -= prefixLength;

   // check hint
   unsigned lower = 0;
   unsigned upper = count;
   uint32_t keyHead = head(key, keyLength);
   searchHint(keyHead, lower, upper);

   // binary search on remaining range
   while (lower < upper) {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (enableBasicHead && keyHead < slot[mid].head[0]) {
         upper = mid;
      } else if (enableBasicHead && keyHead > slot[mid].head[0]) {
         lower = mid + 1;
      } else {  // head is equal, check full key
         int cmp = memcmp(key, getKey(mid), min(keyLength, slot[mid].keyLen));
         if (cmp < 0) {
            upper = mid;
         } else if (cmp > 0) {
            lower = mid + 1;
         } else {
            if (keyLength < slot[mid].keyLen) {  // key is shorter
               upper = mid;
            } else if (keyLength > slot[mid].keyLen) {  // key is longer
               lower = mid + 1;
            } else {
               foundOut = true;
               return mid;
            }
         }
      }
   }
   return lower;
}

// lowerBound wrapper ignoring exact match argument (for convenience)
unsigned BTreeNode::lowerBound(uint8_t* key, unsigned keyLength)
{
   bool ignore;
   return lowerBound(key, keyLength, ignore);
}

bool BTreeNode::insertChild(uint8_t* key, unsigned keyLength, AnyNode* child)
{
   return insert(key, keyLength, reinterpret_cast<uint8_t*>(&child), sizeof(AnyNode*));
}

bool BTreeNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   if (!requestSpaceFor(spaceNeeded(keyLength, payloadLength))) {
      AnyNode tmp;
      bool densify1 = enableDense && _tag == Tag::Leaf && keyLength - prefixLength == slot[0].keyLen && payloadLength == slot[0].payloadLen;
      bool densify2 = enableDense2 && _tag == Tag::Leaf && keyLength - prefixLength == slot[0].keyLen;
      if ((densify1 || densify2) && tmp._dense.try_densify(this)) {
         memcpy(this, &tmp, pageSizeLeaf);
         return this->any()->dense()->insert(key, keyLength, payload, payloadLength);
      }
      return false;  // no space, insert fails
   }
   bool found;
   unsigned slotId = lowerBound(key, keyLength, found);
   if (found) {
      abort(); // this does not update spaceUsed properly
      storeKeyValue(slotId, key, keyLength, payload, payloadLength);
   } else {
      memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
      storeKeyValue(slotId, key, keyLength, payload, payloadLength);
      count++;
      updateHint(slotId);
   }
   return true;
}

void BTreeNode::removeSlot(unsigned slotId)
{
   spaceUsed -= slot[slotId].keyLen;
   spaceUsed -= slot[slotId].payloadLen;
   memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   count--;
   makeHint();
}

bool BTreeNode::remove(uint8_t* key, unsigned keyLength)
{
   bool found;
   unsigned slotId = lowerBound(key, keyLength, found);
   if (!found)
      return false;
   removeSlot(slotId);
   return true;
}

void BTreeNode::compactify()
{
   unsigned should = freeSpaceAfterCompaction();
   static_cast<void>(should);
   TmpBTreeNode tmp;
   tmp.node.init(isLeaf(),rangeOpCounter);
   tmp.node.setFences(getLowerFence(), lowerFence.length, getUpperFence(), upperFence.length);
   copyKeyValueRange(&tmp.node, 0, 0, count);
   tmp.node.upper = upper;
   memcpy(reinterpret_cast<char*>(this), &tmp.node, isLeaf() ? pageSizeLeaf : pageSizeInner);
   makeHint();
   assert(freeSpace() == should);
}

// merge "this" into "right" via "tmp"
bool BTreeNode::mergeNodes(unsigned slotId, AnyNode* parent, BTreeNode* right)
{
   TmpBTreeNode tmp;
   tmp.node.init(isLeaf(),rangeOpCounter);
   tmp.node.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
   assert(right->isLeaf() == isLeaf());
   ASSUME(enablePrefix || prefixLength == 0);
   ASSUME(enablePrefix || right->prefixLength == 0);
   ASSUME(enablePrefix || tmp.node.prefixLength == 0);
   unsigned leftGrow = (prefixLength - tmp.node.prefixLength) * count;
   unsigned rightGrow = (right->prefixLength - tmp.node.prefixLength) * right->count;
   if (isLeaf()) {
      unsigned spaceUpperBound =
          spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
      if (spaceUpperBound > pageSizeLeaf)
         return false;
      copyKeyValueRange(&tmp.node, 0, 0, count);
      right->copyKeyValueRange(&tmp.node, count, 0, right->count);
      parent->innerRemoveSlot(slotId);
      memcpy(reinterpret_cast<uint8_t*>(right), &tmp, pageSizeLeaf);
      right->makeHint();
      return true;
   } else {
      unsigned extraKeyLength = parent->innerKeyLen(slotId);
      unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow +
                                 rightGrow + tmp.node.spaceNeeded(extraKeyLength, sizeof(BTreeNode*));
      if (spaceUpperBound > pageSizeInner)
         return false;
      copyKeyValueRange(&tmp.node, 0, 0, count);
      uint8_t extraKey[extraKeyLength];
      parent->innerRestoreKey(extraKey, extraKeyLength, slotId);
      AnyNode* child = parent->getChild(slotId);
      storeKeyValue(count, extraKey, extraKeyLength, reinterpret_cast<uint8_t*>(&child), sizeof(AnyNode*));
      count++;
      right->copyKeyValueRange(&tmp.node, count, 0, right->count);
      parent->innerRemoveSlot(slotId);
      memcpy(reinterpret_cast<uint8_t*>(right), &tmp, pageSizeInner);
      return true;
   }
}

// store key/value pair at slotId
void BTreeNode::storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   ASSUME(enablePrefix || prefixLength == 0);
   // slot
   key += prefixLength;
   keyLength -= prefixLength;
   if (enableBasicHead) {
      slot[slotId].head[0] = head(key, keyLength);
   }
   slot[slotId].keyLen = keyLength;
   slot[slotId].payloadLen = payloadLength;
   // key
   unsigned space = keyLength + payloadLength;
   dataOffset -= space;
   spaceUsed += space;
   slot[slotId].offset = dataOffset;
   assert(getKey(slotId) >= reinterpret_cast<uint8_t*>(&slot[slotId + 1]));
   memcpy(getKey(slotId), key, keyLength);
   memcpy(getPayload(slotId), payload, payloadLength);
}

void BTreeNode::copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount)
{
   if (enablePrefix && prefixLength <= dst->prefixLength) {  // prefix grows
      unsigned diff = dst->prefixLength - prefixLength;
      for (unsigned i = 0; i < srcCount; i++) {
         unsigned newKeyLength = slot[srcSlot + i].keyLen - diff;
         unsigned space = newKeyLength + slot[srcSlot + i].payloadLen;
         dst->dataOffset -= space;
         dst->spaceUsed += space;
         dst->slot[dstSlot + i].offset = dst->dataOffset;
         uint8_t* key = getKey(srcSlot + i) + diff;
         memcpy(dst->getKey(dstSlot + i), key, space);
         if (enableBasicHead)
            dst->slot[dstSlot + i].head[0] = head(key, newKeyLength);
         dst->slot[dstSlot + i].keyLen = newKeyLength;
         dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
      }
   } else {
      for (unsigned i = 0; i < srcCount; i++)
         copyKeyValue(srcSlot + i, dst, dstSlot + i);
   }
   dst->count += srcCount;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

void BTreeNode::copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot)
{
   ASSUME(enablePrefix || prefixLength == 0);
   unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
   uint8_t key[fullLength];
   memcpy(key, getPrefix(), prefixLength);
   memcpy(key + prefixLength, getKey(srcSlot), slot[srcSlot].keyLen);
   dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), slot[srcSlot].payloadLen);
}

void BTreeNode::insertFence(FenceKeySlot& fk, uint8_t* key, unsigned keyLength)
{
   assert(freeSpace() >= keyLength);
   dataOffset -= keyLength;
   spaceUsed += keyLength;
   fk.offset = dataOffset;
   fk.length = keyLength;
   memcpy(ptr() + dataOffset, key, keyLength);
}

void BTreeNode::setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen)
{
   insertFence(lowerFence, lowerKey, lowerLen);
   insertFence(upperFence, upperKey, upperLen);
   for (prefixLength = 0; enablePrefix && (prefixLength < min(lowerLen, upperLen)) && (lowerKey[prefixLength] == upperKey[prefixLength]);
        prefixLength++)
      ;
}

bool BTreeNode::hasBadHeads()
{
   unsigned threshold = count / 16;
   unsigned collisionCount = 0;
   for (unsigned i = 1; i < count; ++i) {
      if (slot[i - 1].head[0] == slot[i].head[0]) {
         collisionCount += 1;
         if (collisionCount > threshold)
            break;
      }
   }
   return collisionCount > threshold;
}

void BTreeNode::copyKeyValueRangeToHash(HashNode* dst, unsigned dstSlot, unsigned srcSlot, unsigned srcCount)
{
   for (unsigned i = 0; i < srcCount; i++) {
      unsigned fullLength = slot[srcSlot + i].keyLen + prefixLength;
      uint8_t key[fullLength];
      memcpy(key, getLowerFence(), prefixLength);
      memcpy(key + prefixLength, getKey(srcSlot + i), slot[srcSlot + i].keyLen);
      dst->storeKeyValue(dstSlot + i, key, fullLength, getPayload(srcSlot + i), slot[srcSlot + i].payloadLen,
                         HashNode::compute_hash(key + dst->prefixLength, fullLength - dst->prefixLength));
   }
   dst->count += srcCount;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

void BTreeNode::splitToHash(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   HashNode* nodeLeft = &(AnyNode::allocLeaf())->_hash;
   unsigned capacity = count;
   nodeLeft->init(getLowerFence(), lowerFence.length, sepKey, sepLength, capacity,rangeOpCounter);
   HashNode right;
   right.init(sepKey, sepLength, getUpperFence(), upperFence.length, capacity,rangeOpCounter);
   bool succ = parent->insertChild(sepKey, sepLength, nodeLeft->any());
   assert(succ);
   copyKeyValueRangeToHash(nodeLeft, 0, 0, sepSlot + 1);
   copyKeyValueRangeToHash(&right, 0, nodeLeft->count, count - nodeLeft->count);
   nodeLeft->sortedCount = nodeLeft->count;
   right.sortedCount = right.count;
   nodeLeft->validate();
   right.validate();
   memcpy(this, &right, pageSizeLeaf);
}

bool BTreeNode::tryConvertToHash(){
   if(spaceUsed + count*(1+sizeof(HashSlot))+sizeof(HashNodeHeader) > pageSizeLeaf){
      return false;
   }
   unsigned capacity;
   {
      unsigned available = pageSizeLeaf - sizeof(HashNodeHeader) - upperFence.length - lowerFence.length;
      unsigned entrySpaceUse = spaceUsed - upperFence.length - lowerFence.length+ count * sizeof(HashSlot);
      // equivalent to `available / (entrySpaceUse/count +1)`
      capacity = count == 0 ? pageSizeLeaf / 64 : available * count / (entrySpaceUse + count);
      ASSUME(capacity >= count);
   }
   HashNode tmp;
   tmp.init(getLowerFence(), lowerFence.length, getUpperFence(), upperFence.length, capacity,rangeOpCounter);
   copyKeyValueRangeToHash(&tmp, 0, 0, count);
   tmp.sortedCount = tmp.count;
//   printf("### toHash");
//   print();
//   tmp.print();
   tmp.validate();
   memcpy(this, &tmp, pageSizeLeaf);
   return true;
}

void BTreeNode::splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   // split this node into nodeLeft and nodeRight
   assert(sepSlot > 0);
   assert(sepSlot < ((isLeaf() ? pageSizeLeaf : pageSizeInner) / sizeof(BTreeNode*)));
   BTreeNode* nodeLeft;
   if (isLeaf()) {
      if (enableHashAdapt){
         bool badHeads = hasBadHeads();
         if(badHeads){
            rangeOpCounter.setBadHeads(rangeOpCounter.count);
         }else{
            rangeOpCounter.setGoodHeads();
         }
         if(badHeads && rangeOpCounter.isLowRange())
            return splitToHash(parent, sepSlot, sepKey, sepLength);
      }
      nodeLeft = &AnyNode::allocLeaf()->_basic_node;
      nodeLeft->init(true,rangeOpCounter);
   } else {
      AnyNode* r = AnyNode::allocInner();
      r->_basic_node.init(false,rangeOpCounter);
      nodeLeft = r->basic();
   }
   nodeLeft->setFences(getLowerFence(), lowerFence.length, sepKey, sepLength);

   TmpBTreeNode tmp;
   tmp.node.init(isLeaf(),rangeOpCounter);
   BTreeNode* nodeRight = &tmp.node;
   nodeRight->setFences(sepKey, sepLength, getUpperFence(), upperFence.length);
   bool succ = parent->insertChild(sepKey, sepLength, nodeLeft->any());
   ASSUME(succ);
   if (isLeaf()) {
      copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
   } else {
      // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
      copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
      nodeLeft->upper = getChild(nodeLeft->count);
      nodeRight->upper = upper;
      // validate_child_fences();
   }
   nodeLeft->makeHint();
   nodeRight->makeHint();
   memcpy(reinterpret_cast<char*>(this), nodeRight, isLeaf() ? pageSizeLeaf : pageSizeInner);
}

void BTreeNode::validate_child_fences()
{
#ifdef NDEBUG
   abort();
#endif
   if (!isInner()) {
      return;
   }
   uint8_t buffer[maxKVSize];
   unsigned bufferLen = lowerFence.length;
   memcpy(buffer, getLowerFence(), bufferLen);
   for (int i = 0; i <= count; ++i) {
      BTreeNode* child = (i == count ? upper : getChild(i))->basic();
      assert(child->lowerFence.length == bufferLen);
      assert(memcmp(child->getLowerFence(), buffer, bufferLen) == 0);
      if (i == count) {
         bufferLen = upperFence.length;
         memcpy(buffer, getUpperFence(), bufferLen);
      } else {
         memcpy(buffer + prefixLength, getKey(i), slot[i].keyLen);
         bufferLen = prefixLength + slot[i].keyLen;
      }
      assert(child->upperFence.length == bufferLen);
      assert(memcmp(child->getUpperFence(), buffer, bufferLen) == 0);
   }
}

void BTreeNode::print()
{
   printf("# BTreeNode\n");
   printf("lower fence: ");
   printKey(getLowerFence(), lowerFence.length);
   printf("\nupper fence: ");
   printKey(getUpperFence(), upperFence.length);
   printf("\n");
   for (unsigned i = 0; i < count; ++i) {
      printf("%d: ", i);
      printKey(getKey(i), slot[i].keyLen);
      if (isInner()) {
         printf("-> %p\n", reinterpret_cast<void*>(getChild(i)));
      } else {
         printf("\n");
      }
   }
   if (isInner()) {
      printf("upper -> %p\n", reinterpret_cast<void*>(upper));
   }
}

unsigned BTreeNode::commonPrefix(unsigned slotA, unsigned slotB)
{
   assert(slotA < count);
   unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
   uint8_t *a = getKey(slotA), *b = getKey(slotB);
   unsigned i;
   for (i = 0; i < limit; i++)
      if (a[i] != b[i])
         break;
   return i;
}

// splits after sepSlot
BTreeNode::SeparatorInfo BTreeNode::findSeparator()
{
   constexpr bool USE_ORIGINAL = false;
   if (USE_ORIGINAL) {
      ASSUME(count > 1);
      ASSUME(enablePrefix || prefixLength == 0);
      if (isInner()) {
         // inner nodes are split in the middle
         unsigned slotId = count / 2;
         return BTreeNode::SeparatorInfo{static_cast<unsigned>(prefixLength + slot[slotId].keyLen), slotId, false};
      }

      // find good separator slot
      unsigned bestPrefixLength, bestSlot;
      if (count > 16) {
         unsigned lower = (count / 2) - (count / 16);
         unsigned upper = (count / 2);

         bestPrefixLength = commonPrefix(lower, 0);
         bestSlot = lower;

         if (bestPrefixLength != commonPrefix(upper - 1, 0))
            for (bestSlot = lower + 1; (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLength); bestSlot++)
               ;
      } else {
         bestSlot = (count - 1) / 2;
         bestPrefixLength = commonPrefix(bestSlot, 0);
      }

      // try to truncate separator
      unsigned common = commonPrefix(bestSlot, bestSlot + 1);
      if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) && (slot[bestSlot + 1].keyLen > (common + 1)))
         return SeparatorInfo{prefixLength + common + 1, bestSlot, true};

      return SeparatorInfo{static_cast<unsigned>(prefixLength + slot[bestSlot].keyLen), bestSlot, false};
   } else {
      ASSUME(count > 1);
      ASSUME(enablePrefix || prefixLength == 0);
      if (isInner()) {
         // inner nodes are split in the middle
         unsigned slotId = count / 2 - 1;
         return BTreeNode::SeparatorInfo{static_cast<unsigned>(prefixLength + slot[slotId].keyLen), slotId, false};
      }

      // find good separator slot
      unsigned lower = count / 2 - count / 32;
      unsigned upper = lower + count / 16;

      unsigned rangeCommonPrefix = commonPrefix(lower, upper);
      if (slot[lower].keyLen == rangeCommonPrefix) {
         return BTreeNode::SeparatorInfo{prefixLength + rangeCommonPrefix, lower, false};
      }
      for (unsigned i = lower + 1;; ++i) {
         assert(i < upper + 1);
         if (getKey(i)[rangeCommonPrefix] != getKey(lower)[rangeCommonPrefix]) {
            if (slot[i].keyLen == rangeCommonPrefix + 1)
               return BTreeNode::SeparatorInfo{prefixLength + rangeCommonPrefix + 1, i, false};
            else
               return BTreeNode::SeparatorInfo{prefixLength + rangeCommonPrefix + 1, i - 1, true};
         }
      }
      ASSUME(false);
   }
}

void BTreeNode::restoreKey(uint8_t* sepKeyOut, unsigned len, unsigned index)
{
   ASSUME(enablePrefix || prefixLength == 0);
   memcpy(sepKeyOut, getPrefix(), prefixLength);
   memcpy(sepKeyOut + prefixLength, getKey(index), len - prefixLength);
}

void BTreeNode::getSep(uint8_t* sepKeyOut, SeparatorInfo info)
{
   restoreKey(sepKeyOut, info.length, info.slot + info.isTruncated);
}

AnyNode* BTreeNode::lookupInner(uint8_t* key, unsigned keyLength)
{
   // validate_child_fences();
   unsigned pos = lowerBound(key, keyLength);
   if (pos == count)
      return upper;
   return getChild(pos);
}

void BTreeNode::destroy()
{
   if (isInner()) {
      for (unsigned i = 0; i < count; i++)
         getChild(i)->destroy();
      upper->destroy();
   }
   this->any()->dealloc();
}

// take isInt to have same interface as in memory structures, but ignore it.
BTree::BTree(bool isInt) : root((enableHash && !enableHashAdapt) ? HashNode::makeRootLeaf() : BTreeNode::makeLeaf())
{
#ifndef NDEBUG
   // prevent print from being optimized out. It is otherwise never called, but nice for debugging
   if (getenv("oMEeHAobn4"))
      root->print();
#endif
}

BTree::~BTree()
{
   root->destroy();
}

// point lookup
uint8_t* BTree::lookupImpl(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   AnyNode* node = root;
   while (node->isAnyInner())
      node = node->lookupInner(key, keyLength);
   // COUNTER(is_basic_lookup,node->tag == Tag::Leaf,1<<20)
   switch (node->tag()) {
      case Tag::Leaf: {
         BTreeNode* basicNode = node->basic();
         if(basicNode->rangeOpCounter.point_op() && basicNode->tryConvertToHash()){
            return node->hash()->lookup(key,keyLength,payloadSizeOut);
         }

         bool found;
         unsigned pos = basicNode->lowerBound(key, keyLength, found);
         if (!found)
            return nullptr;

         // key found, copy payload
         assert(pos < basicNode->count);
         payloadSizeOut = basicNode->slot[pos].payloadLen;
         return basicNode->getPayload(pos);
      }
      case Tag::Dense:
      case Tag::Dense2: {
         return node->dense()->lookup(key, keyLength, payloadSizeOut);
      }
      case Tag::Hash: {
         return node->hash()->lookup(key, keyLength, payloadSizeOut);
      }
      case Tag::Head4:
      case Tag::Head8:
      case Tag::Inner:
         ASSUME(false);
   }
}

void DataStructureWrapper::testing_update_payload(uint8_t* key, unsigned keyLength, uint8_t* payload)
{
#ifdef CHECK_TREE_OPS
   auto found = std_map.find(toByteVector(key, keyLength));
   memcpy(found->second.data(), payload, found->second.size());
#endif
}

// point lookup
uint8_t* DataStructureWrapper::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   uint8_t* payload = impl.lookupImpl(key, keyLength, payloadSizeOut);
#ifdef CHECK_TREE_OPS
   auto found = std_map.find(toByteVector(key, keyLength));
   if (found == std_map.end()) {
      assert(payload == nullptr);
   } else {
      assert(payload != nullptr);
      assert(found->second.size() == payloadSizeOut);
      assert(memcmp(found->second.data(), payload, payloadSizeOut) == 0);
   }
#endif
   return payload;
}

void BTree::splitNode(AnyNode* node, AnyNode* parent, uint8_t* key, unsigned keyLength)
{
   // create new root if necessary
   if (!parent) {
      parent = AnyNode::makeRoot(node);
      root = parent;
   }

   if (!node->splitNodeWithParent(parent, key, keyLength))
      // must split parent first to make space for separator, restart from root to do this
      ensureSpace(parent, key, keyLength);
}

void BTree::ensureSpace(AnyNode* toSplit, uint8_t* key, unsigned keyLength)
{
   AnyNode* node = root;
   AnyNode* parent = nullptr;
   while (node->isAnyInner() && (node != toSplit)) {
      parent = node;
      node = parent->lookupInner(key, keyLength);
   }
   splitNode(toSplit, parent, key, keyLength);
}

void DataStructureWrapper::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
#ifdef CHECK_TREE_OPS
   std_map[toByteVector(key, keyLength)] = toByteVector(payload, payloadLength);
#endif
   return impl.insertImpl(key, keyLength, payload, payloadLength);
}

void BTree::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   assert((keyLength + payloadLength) <= BTreeNode::maxKVSize);
   AnyNode* node = root;
   AnyNode* parent = nullptr;
   while (node->isAnyInner()) {
      parent = node;
      node = parent->lookupInner(key, keyLength);
   }
   // COUNTER(is_basic_insert,node->tag == Tag::Leaf,1<<20)
   switch (node->tag()) {
      case Tag::Leaf: {
         if(node->basic()->rangeOpCounter.point_op() && node->basic()->tryConvertToHash()){
            if(node->hash()->insert(key,keyLength,payload,payloadLength))
               return;
         }else{
            if (node->basic()->insert(key, keyLength, payload, payloadLength))
               return;
         }
         break;
      }
      case Tag::Dense:
      case Tag::Dense2: {
         if (node->dense()->insert(key, keyLength, payload, payloadLength)) {
            return;
         }
         break;
      }
      case Tag::Hash: {
         if (node->hash()->insert(key, keyLength, payload, payloadLength)) {
            return;
         }
         break;
      }
      case Tag::Inner:
      case Tag::Head4:
      case Tag::Head8:
         ASSUME(false);
   }
   // node is full: split and restart
   splitNode(node, parent, key, keyLength);
   insertImpl(key, keyLength, payload, payloadLength);
}

bool BTreeNode::is_underfull()
{
   return freeSpaceAfterCompaction() >= (isLeaf() ? underFullSizeLeaf : underFullSizeInner);
}

bool DataStructureWrapper::remove(uint8_t* key, unsigned keyLength)
{
   bool removed = impl.removeImpl(key, keyLength);
#ifdef CHECK_TREE_OPS
   auto found = std_map.find(toByteVector(key, keyLength));
   if (found == std_map.end()) {
      assert(!removed);
   } else {
      assert(removed);
      std_map.erase(found);
   }
#endif
   return removed;
}

bool BTree::removeImpl(uint8_t* key, unsigned keyLength) const
{
   AnyNode* node = root;
   AnyNode* parent = nullptr;
   unsigned pos = 0;
   while (node->isAnyInner()) {
      parent = node;
      pos = parent->lookupInnerIndex(key, keyLength);
      node = parent->getChild(pos);
   }
   switch (node->tag()) {
      case Tag::Leaf: {
         if (!node->basic()->remove(key, keyLength))
            return false;  // key not found

         // merge if underfull
         if (node->basic()->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSizeLeaf && parent) {
            // find neighbor and merge
            unsigned parentCount = parent->innerCount();
            if (parentCount >= 2 && ((pos + 1) < parentCount)) {
               AnyNode* right = parent->getChild(pos + 1);
               if (right->tag() == Tag::Leaf) {
                  if (right->basic()->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSizeLeaf) {
                     if (node->basic()->mergeNodes(pos, parent, right->basic()))
                        node->dealloc();
                  }
               }
            }
         }
         return true;
      }
      case Tag::Dense:
      case Tag::Dense2: {
         if (!node->dense()->remove(key, keyLength))
            return false;
         if (parent) {
            unsigned parentCount = parent->innerCount();
            if (parentCount >= 2 && (pos + 1) < parentCount && node->dense()->is_underfull()) {
               node->dense()->convertToBasic();
               AnyNode* right = parent->getChild(pos + 1);
               if (right->tag() == Tag::Leaf) {
                  if (right->basic()->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSizeLeaf) {
                     if (node->basic()->mergeNodes(pos, parent, right->basic()))
                        node->dealloc();
                  }
               }
            }
         }
         return true;
      }
      case Tag::Hash: {
         if (!node->hash()->remove(key, keyLength))
            return false;  // key not found

         // merge if underfull
         if (node->hash()->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSizeLeaf && parent) {
            unsigned parentCount = parent->innerCount();
            // find neighbor and merge
            if (parentCount >= 2 && ((pos + 1) < parentCount)) {
               AnyNode* right = parent->getChild(pos + 1);
               if (right->_tag == Tag::Hash && right->hash()->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSizeLeaf) {
                  if (node->hash()->mergeNodes(pos, parent, right->hash()))
                     node->dealloc();
               }
            }
         }
         return true;
      }
      case Tag::Inner:
      case Tag::Head4:
      case Tag::Head8: {
         ASSUME(false)
      }
   }
}

bool BTreeNode::range_lookup(uint8_t* key,
                             unsigned keyLen,
                             uint8_t* keyOut,
                             // called with keylen and value
                             // scan continues if callback returns true
                             const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   rangeOpCounter.range_op();
   ASSUME(enablePrefix || prefixLength == 0);
   for (unsigned i = (key == nullptr) ? 0 : lowerBound(key, keyLen); i < count; ++i) {
      memcpy(keyOut + prefixLength, getKey(i), slot[i].keyLen);
      if (!found_record_cb(slot[i].keyLen + prefixLength, getPayload(i), slot[i].payloadLen)) {
         return false;
      }
   }
   return true;
}

void DataStructureWrapper::range_lookup(uint8_t* key,
                                        unsigned keyLen,
                                        uint8_t* keyOut,
                                        // called with keylen and value
                                        // scan continues if callback returns true
                                        const std::function<bool(unsigned, uint8_t*, unsigned)>& found_record_cb)
{
#ifdef CHECK_TREE_OPS
   bool shouldContinue = true;
   auto keyVec = toByteVector(key, keyLen);
   auto std_iterator = std_map.lower_bound(keyVec);
   impl.range_lookupImpl(key, keyLen, keyOut, [&](unsigned keyLen, uint8_t* payload, unsigned payloadLen) {
      assert(shouldContinue);
      assert(std_iterator != std_map.end());
      uint64_t std_size =std_iterator->first.size();
      const unsigned char* std_key=std_iterator->first.data();
      uint64_t std_v_size = std_iterator->second.size();
      const unsigned char* std_val = std_iterator->second.data();
      assert(std_size == keyLen);
      assert(memcmp(std_key, keyOut, keyLen) == 0);
      assert(std_v_size == payloadLen);
      assert(memcmp(std_val, payload, payloadLen) == 0);
      shouldContinue = found_record_cb(keyLen, payload, payloadLen);
      ++std_iterator;
      return shouldContinue;
   });
   if (shouldContinue) {
      assert(std_iterator == std_map.end());
   }
#else
   impl.range_lookupImpl(key, keyLen, keyOut, std::move(found_record_cb));
#endif
}

// keyOut must be able to hold (longest key length) +1 bytes.
void BTree::range_lookupImpl(uint8_t* startKey,
                             unsigned keyLen,
                             uint8_t* keyOut,
                             // called with keylen and value
                             // scan continues if callback returns true
                             const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   uint8_t* leafKey = startKey;
   memcpy(keyOut, startKey, keyLen);
   while (true) {
      AnyNode* node = root;
      while (node->isAnyInner()) {
         node = node->lookupInner(keyOut, keyLen);
      }
      switch (node->tag()) {
         case Tag::Leaf: {
            if (!node->basic()->range_lookup(leafKey, keyLen, keyOut, found_record_cb))
               return;
            if(!enableAdaptOp){
               keyLen = node->basic()->upperFence.length;
               leafKey = nullptr;
               memcpy(keyOut + node->basic()->prefixLength, node->basic()->getUpperFence() + node->basic()->prefixLength,
                      keyLen - node->basic()->prefixLength);
               keyOut[keyLen] = 0;
               keyLen += 1;
            }
            break;
         }
         case Tag::Dense: {
            if (!node->dense()->range_lookup1(leafKey, keyLen, keyOut, found_record_cb))
               return;
            keyLen = node->dense()->upperFenceLen;
            leafKey = nullptr;
            memcpy(keyOut + node->dense()->prefixLength, node->dense()->getUpperFence() + node->dense()->prefixLength,
                   keyLen - node->dense()->prefixLength);
            keyOut[keyLen] = 0;
            keyLen += 1;
            break;
         }
         case Tag::Dense2: {
            if (!node->dense()->range_lookup2(leafKey, keyLen, keyOut, found_record_cb))
               return;
            keyLen = node->dense()->upperFenceLen;
            leafKey = nullptr;
            memcpy(keyOut + node->dense()->prefixLength, node->dense()->getUpperFence() + node->dense()->prefixLength,
                   keyLen - node->dense()->prefixLength);
            keyOut[keyLen] = 0;
            keyLen += 1;
            break;
         }
         case Tag::Hash: {
            if (!node->hash()->range_lookup(leafKey, keyLen, keyOut, found_record_cb))
               return;
            if(!enableAdaptOp) {
               keyLen = node->hash()->upperFenceLen;
               leafKey = nullptr;
               memcpy(keyOut + node->hash()->prefixLength, node->hash()->getUpperFence() + node->hash()->prefixLength,
                      keyLen - node->hash()->prefixLength);
               keyOut[keyLen] = 0;
               keyLen += 1;
            }
            break;
         }
         case Tag::Inner:
         case Tag::Head4:
         case Tag::Head8:
            ASSUME(false)
      }
      if(enableAdaptOp){
         switch(node->tag()){
            case Tag::Leaf: {
               keyLen = node->basic()->upperFence.length;
               leafKey = nullptr;
               memcpy(keyOut + node->basic()->prefixLength, node->basic()->getUpperFence() + node->basic()->prefixLength,
                      keyLen - node->basic()->prefixLength);
               keyOut[keyLen] = 0;
               keyLen += 1;
               break;
            }
            case Tag::Hash: {
               keyLen = node->hash()->upperFenceLen;
               leafKey = nullptr;
               memcpy(keyOut + node->hash()->prefixLength, node->hash()->getUpperFence() + node->hash()->prefixLength,
                      keyLen - node->hash()->prefixLength);
               keyOut[keyLen] = 0;
               keyLen += 1;
               break;
            }
            default:
               break;
         }
      }
      if (keyLen == 1) {
         // encountered empty upper fence
         break;
      }
   }
}

bool BTreeNode::range_lookup_desc(uint8_t* key,
                                  unsigned keyLen,
                                  uint8_t* keyOut,
                                  // called with keylen and value
                                  // scan continues if callback returns true
                                  const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   ASSUME(enablePrefix || prefixLength == 0);
   memcpy(keyOut, key, prefixLength);
   bool found;
   int lb = lowerBound(key, keyLen, found);
   for (int i = lb - !found; i >= 0; --i) {
      memcpy(keyOut + prefixLength, getKey(i), slot[i].keyLen);
      if (!found_record_cb(slot[i].keyLen + prefixLength, getPayload(i), slot[i].payloadLen)) {
         return false;
      }
   }
   return true;
}

void DataStructureWrapper::range_lookup_desc(uint8_t* key,
                                             unsigned keyLen,
                                             uint8_t* keyOut,
                                             // called with keylen and value
                                             // scan continues if callback returns true
                                             const std::function<bool(unsigned, uint8_t*, unsigned)>& found_record_cb)
{
#ifdef CHECK_TREE_OPS
   bool shouldContinue = true;
   auto keyVec = toByteVector(key, keyLen);
   auto std_iterator = std_map.lower_bound(keyVec);
   if (std_iterator != std_map.end() && std_iterator != std_map.begin() && std_iterator->first > keyVec) {
      --std_iterator;
   }
   bool stdExhausted = std_iterator == std_map.end();
   impl.range_lookup_descImpl(key, keyLen, keyOut, [&](unsigned keyLen, uint8_t* payload, unsigned payloadLen) {
      assert(shouldContinue);
      assert(!stdExhausted);
      assert(std_iterator->first.size() == keyLen);
      assert(memcmp(std_iterator->first.data(), keyOut, keyLen) == 0);
      assert(std_iterator->second.size() == payloadLen);
      assert(memcmp(std_iterator->second.data(), payload, payloadLen) == 0);
      shouldContinue = found_record_cb(keyLen, payload, payloadLen);
      if (std_iterator == std_map.begin()) {
         stdExhausted = true;
      } else {
         --std_iterator;
      }
      return shouldContinue;
   });
   if (shouldContinue) {
      assert(stdExhausted);
   }
#else
   impl.range_lookup_descImpl(key, keyLen, keyOut, std::move(found_record_cb));
#endif
}

void BTree::range_lookup_descImpl(uint8_t* key,
                                  unsigned keyLen,
                                  uint8_t* keyOut,
                                  // called with keylen and value
                                  // scan continues if callback returns true
                                  const std::function<bool(unsigned, uint8_t*, unsigned)>& found_record_cb)
{
   uint8_t startKeyBuffer[BTreeNode::maxKVSize];
   while (true) {
      AnyNode* node = root;
      while (node->isAnyInner()) {
         node = node->lookupInner(key, keyLen);
      }
      switch (node->tag()) {
         case Tag::Leaf: {
            if (!node->basic()->range_lookup_desc(key, keyLen, keyOut, found_record_cb))
               return;
            keyLen = node->basic()->lowerFence.length;
            key = startKeyBuffer;
            memcpy(key, node->basic()->getLowerFence(), keyLen);
            break;
         }
         case Tag::Dense:
         case Tag::Dense2: {
            if (!node->dense()->range_lookup_desc(key, keyLen, keyOut, found_record_cb))
               return;
            keyLen = node->dense()->lowerFenceLen;
            key = startKeyBuffer;
            memcpy(key, node->dense()->getLowerFence(), keyLen);
            break;
         }
         case Tag::Hash: {
            if (!node->hash()->range_lookup_desc(key, keyLen, keyOut, found_record_cb))
               return;
            keyLen = node->hash()->lowerFenceLen;
            key = startKeyBuffer;
            memcpy(key, node->hash()->getLowerFence(), keyLen);
            break;
         }
         case Tag::Inner:
         case Tag::Head4:
         case Tag::Head8:
            ASSUME(false)
      }
      if (keyLen == 0) {
         break;
      }
   }
}

void BTreeNode::validateHint()
{
#ifdef NDEBUG
   abort();
#endif
   if (count > 0) {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         assert(hint[i] == slot[dist * (i + 1)].head[0]);
   }
}

DataStructureWrapper::DataStructureWrapper(bool isInt)
    :
#ifdef CHECK_TREE_OPS
      std_map(),
#endif
      impl(isInt){};

void printKey(uint8_t* key, unsigned length)
{
   if (length <= 4) {
      for (unsigned i = 0; i < length; ++i) {
         printf("%3d, ", key[i]);
      }
      printf("|  ");
   }
   for (unsigned i = 0; i < length; ++i) {
      if (key[i] >= ' ' && key[i] <= '~') {
         putchar(key[i]);
      } else {
         printf("\\%02x", key[i]);
      }
   }
}

std::bernoulli_distribution RangeOpCounter::range_dist{0.15};
std::bernoulli_distribution RangeOpCounter::point_dist{0.05};
std::minstd_rand RangeOpCounter::rng{42};