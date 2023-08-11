#include <algorithm>
#include <string_view>
#include "btree2020.hpp"

uint8_t HashNode::compute_hash(uint8_t* key, unsigned keyLength)
{
   // TODO benchmark hash function
   std::hash<std::string_view> hasher;
   return hasher(std::string_view{reinterpret_cast<const char*>(key), keyLength});
}

uint8_t* HashNode::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   int index = findIndex(key, keyLength);
   if (index >= 0) {
      payloadSizeOut = slot[index].payloadLen;
      return getPayload(index);
   }
   return nullptr;
}

void HashNode::print()
{
   printf("# HashNode\n");
   for (unsigned i = 0; i < count; ++i) {
      printf("%4d: [%3d] ", i, hashes()[i]);
      for (unsigned j = 0; j < slot[i].keyLen; ++j) {
         printf("%3d, ", getKey(i)[j]);
      }
      printf("\n");
   }
}

int HashNode::findIndex(uint8_t* key, unsigned keyLength)
{
   if (hashUseSimd) {
      return findIndexSimd(key, keyLength);
   } else {
      return findIndexNoSimd(key, keyLength);
   }
}

int HashNode::findIndexNoSimd(uint8_t* key, unsigned keyLength)
{
   key += prefixLength;
   keyLength -= prefixLength;
   uint8_t needle = compute_hash(key, keyLength);
   for (unsigned i = 0; i < count; ++i) {
      if (hashes()[i] == needle && keyLength == slot[i].keyLen && memcmp(getKey(i), key, keyLength) == 0) {
         return i;
      }
   }
   return -1;
}

typedef uint8_t HashSimdVecByte __attribute__((vector_size(hashSimdWidth)));
// this requires clang version >= 15
typedef bool HashSimdVecBool __attribute__((ext_vector_type(hashSimdWidth)));

inline HashSimdBitMask hashSimdEq(HashSimdVecByte* a, HashSimdVecByte* b)
{
   HashSimdVecBool equality = __builtin_convertvector(*a == *b, HashSimdVecBool);
   HashSimdBitMask equality_bits;
   memcpy(&equality_bits, &equality, sizeof(HashSimdBitMask));
   return equality_bits;
}

int HashNode::findIndexSimd(uint8_t* key, unsigned keyLength)
{
   ASSUME(__builtin_is_aligned(this, alignof(HashSimdVecByte)));
   key += prefixLength;
   keyLength -= prefixLength;
   int hashMisalign = hashOffset % alignof(HashSimdVecByte);
   HashSimdVecByte* haystack_ptr = reinterpret_cast<HashSimdVecByte*>(hashes() - hashMisalign);
   HashSimdVecByte needle = compute_hash(key, keyLength) - HashSimdVecByte{};
   unsigned shift = hashMisalign;
   HashSimdBitMask matches = hashSimdEq(haystack_ptr, &needle) >> shift;
   unsigned shift_limit = shift + count;
   while (shift < shift_limit) {
      unsigned trailing_zeros = std::__countr_zero(matches);
      if (trailing_zeros == hashSimdWidth) {
         shift = shift - shift % hashSimdWidth + hashSimdWidth;
         haystack_ptr += 1;
         matches = hashSimdEq(haystack_ptr, &needle);
      } else {
         shift += trailing_zeros;
         matches >>= trailing_zeros;
         matches -= 1;
         if (shift >= shift_limit) {
            return -1;
         }
         unsigned elementIndex = shift - hashMisalign;
         if (slot[elementIndex].keyLen == keyLength && memcmp(getKey(elementIndex), key, keyLength) == 0) {
            return elementIndex;
         }
      }
   }
   return -1;
}

AnyNode* HashNode::makeRootLeaf()
{
   AnyNode* ptr = new AnyNode;
   ptr->_hash.init(nullptr, 0, nullptr, 0, 64);
   return ptr;
}

uint8_t* HashNode::getLowerFence()
{
   return ptr() + pageSize - lowerFenceLen;
}

uint8_t* HashNode::getUpperFence()
{
   return ptr() + pageSize - lowerFenceLen - upperFenceLen;
}

void HashNode::updatePrefixLength()
{
   uint8_t* uf = getUpperFence();
   uint8_t* lf = getLowerFence();
   prefixLength = 0;
   while (prefixLength < upperFenceLen && prefixLength < lowerFenceLen && lf[prefixLength] == uf[prefixLength]) {
      prefixLength += 1;
   }
}

void HashNode::init(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen, unsigned hashCapacity)
{
   assert(sizeof(HashNode) == pageSize);
   _tag = Tag::Hash;
   count = 0;
   sortedCount = 0;
   spaceUsed = upperFenceLen + lowerFenceLen + hashCapacity;
   dataOffset = pageSize - spaceUsed;
   hashOffset = dataOffset;
   this->hashCapacity = hashCapacity;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   updatePrefixLength();
}

uint8_t* HashNode::hashes()
{
   return ptr() + hashOffset;
}

uint8_t* HashNode::ptr()
{
   return reinterpret_cast<uint8_t*>(this);
}

uint8_t* HashNode::getKey(unsigned slotId)
{
   return ptr() + slot[slotId].offset;
}
uint8_t* HashNode::getPayload(unsigned slotId)
{
   return ptr() + slot[slotId].offset + slot[slotId].keyLen;
}

unsigned HashNode::freeSpace()
{
   return dataOffset - (reinterpret_cast<uint8_t*>(slot + count) - ptr());
}

unsigned HashNode::freeSpaceAfterCompaction()
{
   return pageSize - (reinterpret_cast<uint8_t*>(slot + count) - ptr()) - spaceUsed + hashCapacity;
}

bool HashNode::requestSlotAndSpace(unsigned kvSize)
{
   int onCompactHashCapacity = count * 2;
   if (count < hashCapacity) {
      if (kvSize + sizeof(HashSlot) <= freeSpace()) {
         return true;
      } else if (onCompactHashCapacity + kvSize + sizeof(HashSlot) > freeSpaceAfterCompaction()) {
         return false;
      }
   } else {
      if (hashCapacity * 2 + kvSize + sizeof(HashSlot) <= freeSpace()) {
         dataOffset -= hashCapacity * 2;
         memcpy(ptr() + dataOffset, hashes(), count);
         hashOffset = dataOffset;
         spaceUsed += hashCapacity;
         hashCapacity *= 2;
         return true;
      } else if (onCompactHashCapacity + kvSize + sizeof(HashSlot) > freeSpaceAfterCompaction()) {
         return false;
      }
   }
   compactify(onCompactHashCapacity);
   return true;
}

void HashNode::compactify(unsigned newHashCapacity)
{
   unsigned should = freeSpaceAfterCompaction() - newHashCapacity;
   HashNode tmp;
   tmp.init(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen, newHashCapacity);
   memcpy(tmp.hashes(), hashes(), count);
   memcpy(tmp.slot, slot, sizeof(HashSlot) * count);
   copyKeyValueRange(&tmp, 0, 0, count);
   tmp.sortedCount = sortedCount;
   assert(tmp.freeSpace() == should);
   *this = tmp;
}

bool HashNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert(freeSpace() < pageSize);
   assert(keyLength >= prefixLength);
   if (!requestSlotAndSpace(keyLength - prefixLength + payloadLength)) {
      assert(freeSpace() < pageSize);
      return false;
   }
   int index = findIndex(key, keyLength);
   if (index < 0) {
      storeKeyValue(count, key, keyLength, payload, payloadLength);
      count += 1;
   } else {
      storeKeyValue(index, key, keyLength, payload, payloadLength);
   }
   assert(freeSpace() < pageSize);
   return true;
}

static HashNode* sortNode;
struct SlotProxy {
   HashSlot slot;

   friend bool operator<(const SlotProxy& l, const SlotProxy& r)
   {
      uint8_t* lptr = sortNode->ptr() + l.slot.offset;
      uint8_t* rptr = sortNode->ptr() + r.slot.offset;
      return std::lexicographical_compare(lptr, lptr + l.slot.keyLen, rptr, rptr + r.slot.keyLen);
   }
};

void HashNode::sort()
{
   if (sortedCount == count)
      return;
   // TODO could preserve hashes with some effort
   SlotProxy* slotProxy = reinterpret_cast<SlotProxy*>(slot);
   sortNode = this;
   std::sort(slotProxy + sortedCount, slotProxy + count);
   std::inplace_merge(slotProxy, slotProxy + sortedCount, slotProxy + count);
   for (unsigned i = 0; i < count; ++i) {
      updateHash(i);
   }
   sortedCount = count;
}
void HashNode::updateHash(unsigned int i)
{
   hashes()[i] = compute_hash(getKey(i), slot[i].keyLen);
}

unsigned HashNode::commonPrefix(unsigned slotA, unsigned slotB)
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

BTreeNode::SeparatorInfo HashNode::findSeparator()
{
   assert(count > 1);

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
      return BTreeNode::SeparatorInfo{prefixLength + common + 1, bestSlot, true};

   return BTreeNode::SeparatorInfo{static_cast<unsigned>(prefixLength + slot[bestSlot].keyLen), bestSlot, false};
}

void HashNode::getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info)
{
   memcpy(sepKeyOut, getLowerFence(), prefixLength);
   memcpy(sepKeyOut + prefixLength, getKey(info.slot + info.isTruncated), info.length - prefixLength);
}

void HashNode::splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   // split this node into nodeLeft and nodeRight
   assert(sepSlot > 0);
   HashNode* nodeLeft = &(new AnyNode())->_hash;
   unsigned leftCount = sepSlot + 1;
   nodeLeft->init(getLowerFence(), lowerFenceLen, sepKey, sepLength, leftCount);
   HashNode right;
   right.init(sepKey, sepLength, getUpperFence(), upperFenceLen, count - leftCount);
   bool succ = parent->insertChild(sepKey, sepLength, nodeLeft->any());
   assert(succ);
   copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
   copyKeyValueRange(&right, 0, nodeLeft->count, count - nodeLeft->count);
   *this = right;
}

void HashNode::copyKeyValueRange(HashNode* dst, unsigned dstSlot, unsigned srcSlot, unsigned srcCount)
{
   if (prefixLength <= dst->prefixLength) {  // prefix grows
      unsigned diff = dst->prefixLength - prefixLength;
      for (unsigned i = 0; i < srcCount; i++) {
         unsigned newKeyLength = slot[srcSlot + i].keyLen - diff;
         unsigned space = newKeyLength + slot[srcSlot + i].payloadLen;
         dst->dataOffset -= space;
         dst->spaceUsed += space;
         dst->slot[dstSlot + i].offset = dst->dataOffset;
         uint8_t* key = getKey(srcSlot + i) + diff;
         memcpy(dst->getKey(dstSlot + i), key, space);
         dst->slot[dstSlot + i].keyLen = newKeyLength;
         dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
         dst->updateHash(dstSlot + i);
      }
   } else {
      for (unsigned i = 0; i < srcCount; i++)
         copyKeyValue(srcSlot + i, dst, dstSlot + i);
   }
   dst->count += srcCount;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

void HashNode::copyKeyValue(unsigned srcSlot, HashNode* dst, unsigned dstSlot)
{
   unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
   uint8_t key[fullLength];
   memcpy(key, getLowerFence(), prefixLength);
   memcpy(key + prefixLength, getKey(srcSlot), slot[srcSlot].keyLen);
   dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), slot[srcSlot].payloadLen);
}

void HashNode::storeKeyValue(unsigned slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   // slot
   key += prefixLength;
   keyLength -= prefixLength;
   slot[slotId].keyLen = keyLength;
   slot[slotId].payloadLen = payloadLength;
   // key
   unsigned space = keyLength + payloadLength;
   dataOffset -= space;
   spaceUsed += space;
   slot[slotId].offset = dataOffset;
   assert(getKey(slotId) >= reinterpret_cast<uint8_t*>(&slot[slotId]));
   memcpy(getKey(slotId), key, keyLength);
   memcpy(getPayload(slotId), payload, payloadLength);
   updateHash(slotId);
}

bool HashNode::remove(uint8_t* key, unsigned keyLength)
{
   int index = findIndex(key, keyLength);
   if (index < 0)
      return false;
   return removeSlot(index);
}

bool HashNode::removeSlot(unsigned slotId)
{
   spaceUsed -= slot[slotId].keyLen;
   spaceUsed -= slot[slotId].payloadLen;
   memmove(slot + slotId, slot + slotId + 1, sizeof(HashSlot) * (count - slotId - 1));
   memmove(hashes() + slotId, hashes() + slotId + 1, count - slotId - 1);
   count--;
   sortedCount -= (slotId < sortedCount);
   return true;
}

// merge "this" into "right" via "tmp"
bool HashNode::mergeNodes(unsigned slotId, AnyNode* parent, HashNode* right)
{
   HashNode tmp;
   unsigned newHashCapacity = count + right->count;
   tmp.init(getLowerFence(), lowerFenceLen, right->getUpperFence(), right->upperFenceLen, newHashCapacity);
   unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
   unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
   unsigned spaceUpperBound = spaceUsed - hashCapacity + right->spaceUsed - right->hashCapacity + newHashCapacity +
                              (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
   if (spaceUpperBound > pageSize)
      return false;
   copyKeyValueRange(&tmp, 0, 0, count);
   right->copyKeyValueRange(&tmp, count, 0, right->count);
   parent->innerRemoveSlot(slotId);
   *right = tmp;
   return true;
}

bool HashNode::range_lookup(uint8_t* key,
                            unsigned keyLen,
                            uint8_t* keyOut,
                            // called with keylen and value
                            // scan continues if callback returns true
                            const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   sort();
   memcpy(keyOut, key, prefixLength);
   for (unsigned i = lowerBound(key, keyLen); i < count; ++i) {
      memcpy(keyOut + prefixLength, getKey(i), slot[i].keyLen);
      if (!found_record_cb(slot[i].keyLen + prefixLength, getPayload(i), slot[i].payloadLen)) {
         return false;
      }
   }
   return true;
}

// lower bound search, foundOut indicates if there is an exact match, returns slotId
unsigned HashNode::lowerBound(uint8_t* key, unsigned keyLength)
{
   key += prefixLength;
   keyLength -= prefixLength;
   unsigned lower = 0;
   unsigned upper = count;
   while (lower < upper) {
      unsigned mid = ((upper - lower) / 2) + lower;
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
            return mid;
         }
      }
   }
   return lower;
}
