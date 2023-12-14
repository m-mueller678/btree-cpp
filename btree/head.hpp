#include "btree2020.hpp"

template <class T>
void HeadNode<T>::destroy()
{
   for (unsigned i = 0; i < count + 1; i++)
      loadUnaligned<AnyNode*>(children() + i)->destroy();
}

template <class T>
bool HeadNode<T>::makeSepHead(uint8_t* key, unsigned keyLen, T* out)
{
   uint8_t* bytes = reinterpret_cast<uint8_t*>(out);
   memset(bytes, 0, sizeof(T));
   if (keyLen < sizeof(T)) {
      memcpy(bytes, key, keyLen);
      bytes[sizeof(T) - 1] = keyLen;
      *out = byteswap(*out);
      return true;
   } else {
      return false;
   }
}

template <class T>
void HeadNode<T>::makeNeedleHead(uint8_t* key, unsigned keyLen, T* out)
{
   uint8_t* bytes = reinterpret_cast<uint8_t*>(out);
   memset(bytes, 0, sizeof(T));
   if (keyLen < sizeof(T)) {
      memcpy(bytes, key, keyLen);
      bytes[sizeof(T) - 1] = keyLen;
   } else {
      memcpy(bytes, key, sizeof(T) - 1);
      bytes[sizeof(T) - 1] = sizeof(T);
   }
   *out = byteswap(*out);
}

template <class T>
void HeadNode<T>::splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   ASSUME(sepSlot > 0);
   ASSUME(sepSlot < (pageSizeInner / sizeof(BTreeNode*)));
   HeadNode<T>* nodeLeft = reinterpret_cast<HeadNode<T>*>(AnyNode::allocInner());
   nodeLeft->init(getLowerFence(), lowerFenceLen, sepKey, sepLength);
   HeadNode<T> tmp;
   HeadNode<T>* nodeRight = &tmp;
   nodeRight->init(sepKey, sepLength, getUpperFence(), upperFenceLen);
   bool succ = parent->insertChild(sepKey, sepLength, nodeLeft->any());
   ASSUME(succ);

   // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
   copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
   copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
   memcpy(nodeLeft->children() + nodeLeft->count, children() + nodeLeft->count, sizeof(AnyNode*));
   memcpy(nodeRight->children() + nodeRight->count, children() + count, sizeof(AnyNode*));
   nodeLeft->makeHint();
   nodeRight->makeHint();
   memcpy(this, nodeRight, pageSizeInner);
}

template <class T>
void HeadNode<T>::copyKeyValueRange(HeadNode<T>* dst, unsigned dstSlot, unsigned srcSlot, unsigned srcCount)
{
   if (prefixLength <= dst->prefixLength) {  // prefix grows
      unsigned diff = dst->prefixLength - prefixLength;
      if (diff == 0) {
         memcpy(dst->keys + dstSlot, keys + srcSlot, srcCount * sizeof(AnyNode*));
      } else {
         for (unsigned i = 0; i < srcCount; i++) {
            dst->keys[dstSlot + i] = ((keys[srcSlot + i] & ~static_cast<T>(255)) << (diff * 8)) | (getKeyLength(srcSlot + i) - diff);
         }
      }
      memcpy(dst->children() + dstSlot, children() + srcSlot, srcCount * sizeof(AnyNode*));
   } else {
      for (unsigned i = 0; i < srcCount; i++)
         // copyKeyValue(srcSlot + i, dst, dstSlot + i);
         abort();
   }
   dst->count += srcCount;
   ASSUME(dst->count <= dst->keyCapacity);
}

template <class T>
unsigned HeadNode<T>::getKeyLength(unsigned i)
{
   return keys[i] % 256;
}

template <class T>
void HeadNode<T>::copyKeyValueRangeToBasic(BTreeNode* dst, unsigned srcStart, unsigned srcEnd)
{
   ASSUME(dst->prefixLength >= prefixLength);
   ASSUME(dst->count == 0);
   unsigned prefixDiff = dst->prefixLength - prefixLength;
   unsigned outSlot = 0;
   for (unsigned i = srcStart; i < srcEnd; i++) {
      unsigned keyLength = getKeyLength(i);
      unsigned newKeyLength = keyLength - prefixDiff;
      unsigned space = newKeyLength + sizeof(AnyNode*);
      dst->dataOffset -= space;
      dst->spaceUsed += space;
      dst->slot[outSlot].offset = dst->dataOffset;
      dst->slot[outSlot].keyLen = newKeyLength;
      dst->slot[outSlot].payloadLen = sizeof(AnyNode*);
      T keyHead = byteswap(keys[i]);
      uint8_t* keyBytes = reinterpret_cast<uint8_t*>(&keyHead);
      memcpy(dst->getPayload(outSlot), &children()[i], sizeof(AnyNode*));
      memcpy(dst->getKey(outSlot), keyBytes + prefixDiff, keyLength - prefixDiff);
      if (enableBasicHead)
         dst->slot[outSlot].head[0] = head(dst->getKey(outSlot), dst->slot[outSlot].keyLen);
      outSlot += 1;
   }
   dst->count = outSlot;
   dst->makeHint();
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

template <class T>
bool HeadNode<T>::insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child)
{
   key += prefixLength;
   keyLength -= prefixLength;
   T head;
   if (makeSepHead(key, keyLength, &head)) {
      bool found;
      unsigned index = lowerBound(head, found);
      ASSUME(!found);
      insertAt(index, head, child);
      updateHint(index);
      return true;
   } else {
      // this is already handled in space request
      ASSUME(false);
   }
}

template <class T>
bool HeadNode<T>::convertToHead8WithSpace()
{
   // TODO could do less copying for either conversion direction
   assert(sizeof(T) == 4);
   unsigned newKeyOffset = any()->_head8.data - ptr();
   unsigned newKeyCapacity = (fencesOffset() - newKeyOffset - sizeof(AnyNode*)) / (8 + sizeof(AnyNode*));
   if (count + 1 > newKeyCapacity)
      return false;

   HeadNode8 tmp;
   tmp.init(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
   ASSUME(tmp.keyCapacity == newKeyCapacity);
   for (unsigned i = 0; i < count; ++i) {
      tmp.keys[i] = static_cast<uint64_t>(keys[i] & ~static_cast<uint32_t>(255)) << 32 | static_cast<uint64_t>(keys[i] & 255);
   }
   memcpy(tmp.children(), children(), sizeof(AnyNode*) * (count + 1));
   tmp.count = count;
   tmp.makeHint();
   memcpy(this, &tmp, pageSizeInner);
   return true;
}

template <class T>
bool HeadNode<T>::convertToHead4WithSpace()
{
   assert(sizeof(T) == 8);
   for (unsigned i = 0; i < count; ++i)
      if (getKeyLength(i) >= 4)
         return false;

   HeadNode4 tmp;
   tmp.init(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
   for (unsigned i = 0; i < count; ++i) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshift-count-overflow"
      // this is never runs for T = uint32_t, where the warning is generated
      tmp.keys[i] = static_cast<uint32_t>(keys[i] >> 32) | static_cast<uint32_t>(keys[i] & 255);
#pragma clang diagnostic pop
   }
   memcpy(tmp.children(), children(), sizeof(AnyNode*) * (count + 1));
   tmp.count = count;
   tmp.makeHint();
   memcpy(this, &tmp, pageSizeInner);
   return true;
}

template <class T>
bool HeadNode<T>::convertToBasicWithSpace(unsigned truncatedKeyLen)
{
   unsigned space_lower_bound = lowerFenceLen + upperFenceLen + (count + 1) * (sizeof(AnyNode*) + sizeof(BTreeNode::Slot)) + truncatedKeyLen;
   if (space_lower_bound + sizeof(BTreeNodeHeader) > pageSizeInner) {
      return false;
   } else {
      for (int i = 0; i < count; ++i) {
         space_lower_bound += getKeyLength(i);
      }
      if (space_lower_bound + sizeof(BTreeNodeHeader) > pageSizeInner) {
         return false;
      }
      TmpBTreeNode tmp;
      tmp.node.init(false,RangeOpCounter{});
      tmp.node.setFences(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
      copyKeyValueRangeToBasic(&tmp.node, 0, count);
      tmp.node.upper = loadUnaligned<AnyNode*>(children() + count);
      memcpy(this, &tmp.node, pageSizeInner);
      return true;
   }
}

template <class T>
void HeadNode<T>::insertAt(unsigned index, T head, AnyNode* child)
{
   ASSUME(count < keyCapacity);
   memmove(keys + index + 1, keys + index, (count - index) * sizeof(T));
   keys[index] = head;
   memmove(children() + index + 1, children() + index, (count + 1 - index) * sizeof(AnyNode*));
   storeUnaligned(children() + index, child);
   count += 1;
}

template <class T>
void HeadNode<T>::clone_from_basic(BTreeNode* src)
{
   init(src->getLowerFence(), src->lowerFence.length, src->getUpperFence(), src->upperFence.length);
   ASSUME(src->count <= keyCapacity);
   count = src->count;
   for (unsigned i = 0; i < src->count; ++i) {
      T head;
      bool succ = makeSepHead(src->getKey(i), src->slot[i].keyLen, &head);
      ASSUME(succ);
      keys[i] = head;
      memcpy(children() + i, src->getPayload(i), sizeof(AnyNode*));
   }
   makeHint();
   storeUnaligned(children() + count, src->upper);
}

inline bool HeadNodeHead::requestChildConvertFromBasic(BTreeNode* node, unsigned newKeyLength)
{
   newKeyLength -= node->prefixLength;
   if (newKeyLength >= 8) {
      return false;
   }
   bool shortKeys = newKeyLength < 4;
   for (unsigned i = 0; i < node->count; ++i) {
      if (node->slot[i].keyLen >= 8) {
         return false;
      }
      shortKeys &= node->slot[i].keyLen < 4;
   }
   AnyNode tmp;
   if (shortKeys) {
      tmp._head4.clone_from_basic(node);
      ASSUME(tmp._head4.count < tmp._head4.keyCapacity);
   } else {
      tmp._head8.clone_from_basic(node);
      ASSUME(tmp._head8.count < tmp._head8.keyCapacity);
   }
   memcpy(node, &tmp, pageSizeInner);
   return true;
}

inline uint8_t* HeadNodeHead::getLowerFence()
{
   return ptr() + pageSizeInner - lowerFenceLen;
}

inline uint8_t* HeadNodeHead::getUpperFence()
{
   return ptr() + fencesOffset();
}

inline unsigned HeadNodeHead::fencesOffset()
{
   return pageSizeInner - lowerFenceLen - upperFenceLen;
}

inline void HeadNodeHead::updatePrefixLength()
{
   uint8_t* uf = getUpperFence();
   uint8_t* lf = getLowerFence();
   prefixLength = 0;
   while (prefixLength < upperFenceLen && prefixLength < lowerFenceLen && lf[prefixLength] == uf[prefixLength]) {
      prefixLength += 1;
   }
}

template <class T>
void HeadNode<T>::init(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen)
{
   assert(enablePrefix);
   assert(sizeof(HeadNode<T>) == pageSizeInner);
   _tag = sizeof(T) == 4 ? Tag::Head4 : Tag::Head8;
   count = 0;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   unsigned keyOffset = data - ptr();
   keyCapacity = (fencesOffset() - keyOffset - sizeof(AnyNode*)) / (sizeof(T) + sizeof(AnyNode*));
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   updatePrefixLength();
}

template <class T>
AnyNode** HeadNode<T>::children()
{
   return reinterpret_cast<AnyNode**>(keys + keyCapacity);
}

inline uint8_t* HeadNodeHead::ptr()
{
   return reinterpret_cast<uint8_t*>(this);
}

template <class T>
unsigned HeadNode<T>::lowerBound(T head, bool& foundOut)
{
   validateHint();
   foundOut = false;
   // check hint
   unsigned lower = 0;
   unsigned upper = count;
   searchHint(head, lower, upper);
   if (lower > 0) {
      assert(head > keys[lower]);
   }
   if (upper < count) {
      assert(head <= keys[upper]);
   }
   while (lower < upper) {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (head <= keys[mid]) {
         upper = mid;
      } else if (head > keys[mid]) {
         lower = mid + 1;
      } else {
         foundOut = true;
         return mid;
      }
   }
   return lower;
}

template <class T>
bool HeadNode<T>::requestSpaceFor(unsigned keyLen)
{
   keyLen -= prefixLength;
   if (keyLen >= sizeof(T)) {
      if (sizeof(T) == 4 && keyLen < 8) {
         return convertToHead8WithSpace() || convertToBasicWithSpace(keyLen);
      } else {
         return convertToBasicWithSpace(keyLen);
      }
   } else if (count < keyCapacity) {
      return true;
   } else if (sizeof(T) == 8 && keyLen < 4) {
      return convertToHead4WithSpace();
   } else {
      return false;
   }
}

template <class T>
void HeadNode<T>::getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info)
{
   memcpy(sepKeyOut, getLowerFence(), prefixLength);
   T keyHead = byteswap(keys[info.slot]);
   uint8_t* keyBytes = reinterpret_cast<uint8_t*>(&keyHead);
   memcpy(sepKeyOut + prefixLength, keyBytes, info.length - prefixLength);
}

template <class T>
AnyNode* HeadNode<T>::lookupInner(uint8_t* key, unsigned keyLength)
{
   return loadUnaligned<AnyNode*>(children() + lookupInnerIndex(key, keyLength));
}

template <class T>
unsigned HeadNode<T>::lookupInnerIndex(uint8_t* key, unsigned keyLength)
{
   key += prefixLength;
   keyLength -= prefixLength;
   T head;
   makeNeedleHead(key, keyLength, &head);
   bool found;
   return lowerBound(head, found);
}

template <class T>
BTreeNode::SeparatorInfo HeadNode<T>::findSeparator()
{
   ASSUME(count > 1);
   unsigned slotId = count / 2;
   return BTreeNode::SeparatorInfo{static_cast<unsigned>(prefixLength + getKeyLength(slotId)), slotId, false};
}

template <class T>
void HeadNode<T>::print()
{
   printf("# HeadNode%lu\n", sizeof(T));
   printf("lower fence: ");
   for (unsigned i = 0; i < lowerFenceLen; ++i) {
      printf("%d, ", getLowerFence()[i]);
   }
   printf("\nupper fence: ");
   for (unsigned i = 0; i < upperFenceLen; ++i) {
      printf("%d, ", getUpperFence()[i]);
   }
   printf("\n");
   for (unsigned i = 0; i < count; ++i) {
      printf("%d: %#018lx |", i, static_cast<uint64_t>(keys[i]));
      T keyHead = byteswap(keys[i]);
      for (unsigned j = 0; j < getKeyLength(i); ++j) {
         printf("%d, ", reinterpret_cast<uint8_t*>(&keyHead)[j]);
      }
      printf("-> %p\n", reinterpret_cast<void*>(loadUnaligned<AnyNode*>(children() + i)));
   }
   printf("upper -> %p\n, ", reinterpret_cast<void*>(loadUnaligned<AnyNode*>(children() + count)));
}

template <class T>
void HeadNode<T>::restoreKey(uint8_t* sepKeyOut, unsigned len, unsigned index)
{
   memcpy(sepKeyOut, getLowerFence(), prefixLength);
   T keyHead = byteswap(keys[index]);
   memcpy(sepKeyOut + prefixLength, &keyHead, len - prefixLength);
}

template <class T>
void HeadNode<T>::removeSlot(unsigned int index)
{
   ASSUME(count > 0);
   memmove(keys + index, keys + index + 1, (count - index - 1) * sizeof(T));
   memmove(children() + index, children() + index + 1, (count - index) * sizeof(AnyNode*));
   count -= 1;
   makeHint();
}

template <class T>
void HeadNode<T>::makeHint()
{
   unsigned dist = count / (hintCount + 1);
   for (unsigned i = 0; i < hintCount; i++)
      hint[i] = static_cast<uint32_t>(keys[dist * (i + 1)] >> (sizeof(T) * 8 - 32));
}

template <class T>
void HeadNode<T>::updateHint(unsigned slotId)
{
   unsigned dist = count / (hintCount + 1);
   unsigned begin = 0;
   if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
   for (unsigned i = begin; i < hintCount; i++)
      hint[i] = static_cast<uint32_t>(keys[dist * (i + 1)] >> (sizeof(T) * 8 - 32));
}

template <class T>
void HeadNode<T>::validateHint()
{
   if (count > 0) {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         assert(hint[i] == static_cast<uint32_t>(keys[dist * (i + 1)] >> (sizeof(T) * 8 - 32)));
   }
}

template <class T>
void HeadNode<T>::searchHint(T fullKeyHead, unsigned& lowerOut, unsigned& upperOut)
{
   uint32_t keyHead = static_cast<uint32_t>(fullKeyHead >> (sizeof(T) * 8 - 32));
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
