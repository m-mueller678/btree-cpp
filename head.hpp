#include "btree2020.hpp"

template <class T>
void HeadNode<T>::destroy()
{
   abort();
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
   abort();
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
      dst->slot[outSlot].head = head(dst->getKey(outSlot), dst->slot[outSlot].keyLen);
      outSlot += 1;
   }
   dst->count = outSlot;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

template <class T>
bool HeadNode<T>::insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child)
{
   T head;
   if (makeSepHead(key, keyLength, &head)) {
      bool found;
      unsigned index = lowerBound(head, found);
      ASSUME(!found);
      insertAt(index, head, child);
   } else {
      unsigned space_lower_bound = lowerFenceLen + upperFenceLen + (count + 1) * (sizeof(AnyNode*) + sizeof(BTreeNode::Slot)) + keyLength;
      if (space_lower_bound + sizeof(BTreeNodeHeader) > pageSize) {
         return false;
      } else {
         for (int i = 0; i < count; ++i) {
            space_lower_bound += getKeyLength(i);
         }
         if (space_lower_bound + sizeof(BTreeNodeHeader) > pageSize) {
            return false;
         }
         BTreeNode tmp{false};
         tmp.setFences(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
         copyKeyValueRangeToBasic(&tmp, 0, count);
         any()->_basic_node = tmp;
         return any()->basic()->insertChild(key, keyLength, child);
      }
   }
}

template <class T>
void HeadNode<T>::insertAt(unsigned index, T head, AnyNode* child)
{
   memmove(keys + index + 1, keys + index, (count - index) * sizeof(T));
   keys[index] = head;
   memmove(children() + index + 1, children() + index, (count - index) * sizeof(AnyNode*));
   storeUnaligned(children() + index, child);
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
   storeUnaligned(children() + count, src->upper);
}

bool HeadNodeHead::fromBasicInsert(BTreeNode* node, uint8_t* key, unsigned keyLength, AnyNode* child)
{
   if (keyLength >= 8) {
      return false;
   }
   bool shortKeys = keyLength < 4;
   for (unsigned i = 0; i < node->count; ++i) {
      if (node->slot[i].keyLen >= 8) {
         return false;
      }
      shortKeys &= node->slot[i].keyLen < 4;
   }
   AnyNode tmp;
   if (shortKeys) {
      tmp._head4.clone_from_basic(node);
      return tmp._head4.insertChild(key, keyLength, child);
   } else {
      tmp._head8.clone_from_basic(node);
      return tmp._head8.insertChild(key, keyLength, child);
   }
}

uint8_t* HeadNodeHead::getLowerFence()
{
   return ptr() + pageSize - lowerFenceLen;
}

uint8_t* HeadNodeHead::getUpperFence()
{
   return ptr() + fencesOffset();
}

unsigned HeadNodeHead::fencesOffset()
{
   return pageSize - lowerFenceLen - upperFenceLen;
}

void HeadNodeHead::updatePrefixLength()
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
   assert(sizeof(HeadNode<T>) == pageSize);
   _tag = sizeof(T) == 4 ? Tag::Head4 : Tag::Head8;
   count = 0;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   unsigned keyOffset = data - ptr();
   keyCapacity = (fencesOffset() - keyOffset - sizeof(AnyNode*)) / sizeof(sizeof(T) + sizeof(AnyNode*));
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   updatePrefixLength();
}

template <class T>
AnyNode** HeadNode<T>::children()
{
   return reinterpret_cast<AnyNode**>(keys + keyCapacity);
}

uint8_t* HeadNodeHead::ptr()
{
   return reinterpret_cast<uint8_t*>(this);
}

template <class T>
unsigned HeadNode<T>::lowerBound(T head, bool& foundOut)
{
   foundOut = false;
   // check hint
   unsigned lower = 0;
   unsigned upper = count;
   // searchHint(keyHead, lower, upper);
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
   abort();
}
template <class T>
void HeadNode<T>::getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info)
{
   abort();
}
template <class T>
AnyNode* HeadNode<T>::lookupInner(uint8_t* key, unsigned keyLength)
{
   abort();
}