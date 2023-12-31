#include <algorithm>
#include <string_view>
#include "btree2020.hpp"
#include "crc32.hpp"
#include "functional"

uint8_t HashNode::compute_hash(uint8_t* key, unsigned keyLength)
{
   uint8_t hash;
   if (hashUseCrc32) {
      hash = crc32_hw(key, keyLength) % 256;
   } else {
      std::hash<std::string_view> hasher;
      hash = hasher(std::string_view{reinterpret_cast<const char*>(key), keyLength});
   }
   return hash;
}

unsigned HashNode::estimateCapacity()
{
   unsigned available = pageSizeLeaf - sizeof(HashNodeHeader) - upperFenceLen - lowerFenceLen;
   unsigned entrySpaceUse = spaceUsed - upperFenceLen - lowerFenceLen + count * sizeof(HashSlot);
   // equivalent to `available / (entrySpaceUse/count +1)`
   unsigned capacity = count == 0 ? pageSizeLeaf / 64 : available * count / (entrySpaceUse + count);
   ASSUME(capacity >= count);
   return capacity;
}

uint8_t* HashNode::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   rangeOpCounter.point_op();
   int index = findIndex(key, keyLength, compute_hash(key + prefixLength, keyLength - prefixLength));
   if (index >= 0) {
      payloadSizeOut = slot[index].payloadLen;
      return getPayload(index);
   }
   return nullptr;
}

void HashNode::print()
{
   printf("# HashNode\n");
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
      printf("%4d: [%3d] ", i, hashes()[i]);
      printKey(getKey(i),slot[i].keyLen);
      printf("\n");
   }
}

int HashNode::findIndex(uint8_t* key, unsigned keyLength, uint8_t hash)
{
   if (hashUseSimd) {
      return findIndexSimd(key, keyLength, hash);
   } else {
      return findIndexNoSimd(key, keyLength, hash);
   }
}

int HashNode::findIndexNoSimd(uint8_t* key, unsigned keyLength, uint8_t needle)
{
   key += prefixLength;
   keyLength -= prefixLength;
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

int HashNode::findIndexSimd(uint8_t* key, unsigned keyLength, uint8_t hash)
{
   ASSUME(__builtin_is_aligned(this, alignof(HashSimdVecByte)));
   key += prefixLength;
   keyLength -= prefixLength;
   int hashMisalign = hashOffset % alignof(HashSimdVecByte);
   HashSimdVecByte* haystack_ptr = reinterpret_cast<HashSimdVecByte*>(hashes() - hashMisalign);
   HashSimdVecByte needle = hash - HashSimdVecByte{};
   unsigned shift = hashMisalign;
   HashSimdBitMask matches = hashSimdEq(haystack_ptr, &needle) >> shift;
   unsigned shift_limit = shift + count;
   while (shift < shift_limit) {
      unsigned trailing_zeros = std::__countr_zero(matches);
      if (trailing_zeros == hashSimdWidth) {
         shift = shift - shift % hashSimdWidth + hashSimdWidth;
         if (shift >= shift_limit) {
            return -1;
         }
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
   AnyNode* ptr = AnyNode::allocLeaf();
   ptr->_hash.init(nullptr, 0, nullptr, 0, pageSizeLeaf / 64,RangeOpCounter{});
   return ptr;
}

uint8_t* HashNode::getLowerFence()
{
   return ptr() + pageSizeLeaf - lowerFenceLen;
}

uint8_t* HashNode::getUpperFence()
{
   return ptr() + pageSizeLeaf - lowerFenceLen - upperFenceLen;
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

void HashNode::init(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen, unsigned hashCapacity,RangeOpCounter roc)
{
   assert(sizeof(HashNode) == pageSizeLeaf);
   _tag = Tag::Hash;
   rangeOpCounter=roc;
   count = 0;
   sortedCount = 0;
   spaceUsed = upperFenceLen + lowerFenceLen;
   dataOffset = pageSizeLeaf - spaceUsed - hashCapacity;
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
   return pageSizeLeaf - (reinterpret_cast<uint8_t*>(slot + count) - ptr()) - spaceUsed;
}

bool HashNode::requestSlotAndSpace(unsigned kvSize)
{
   if (count < hashCapacity && kvSize + sizeof(HashSlot) <= freeSpace())
      return true;  // avoid capacity estimate calculation
   unsigned onCompactifyCapacity = max(estimateCapacity(), count + 1);
   if (count < hashCapacity) {
      if (onCompactifyCapacity + kvSize + sizeof(HashSlot) > freeSpaceAfterCompaction()) {
         return false;
      }
   } else {
      unsigned hashGrowCapacity = onCompactifyCapacity;
      if (hashGrowCapacity + kvSize + sizeof(HashSlot) <= freeSpace()) {
         dataOffset -= hashGrowCapacity;
         memcpy(ptr() + dataOffset, hashes(), count);
         hashOffset = dataOffset;
         hashCapacity = hashGrowCapacity;
         return true;
      } else if (onCompactifyCapacity + kvSize + sizeof(HashSlot) > freeSpaceAfterCompaction()) {
         return false;
      }
   }
   // not worth compacting for a few more keys
   if (onCompactifyCapacity <= count + (unsigned)(count)*3 / 128)
      return false;
   compactify(onCompactifyCapacity);
   return true;
}

void HashNode::compactify(unsigned newHashCapacity)
{
   unsigned should = freeSpaceAfterCompaction() - newHashCapacity;
   HashNode tmp;
   tmp.init(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen, newHashCapacity,rangeOpCounter);
   memcpy(tmp.hashes(), hashes(), count);
   memcpy(tmp.slot, slot, sizeof(HashSlot) * count);
   copyKeyValueRange(&tmp, 0, 0, count);
   tmp.sortedCount = sortedCount;
   assert(tmp.freeSpace() == should);
   *this = tmp;
}

bool HashNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   rangeOpCounter.point_op();
   assert(freeSpace() < pageSizeLeaf);
   ASSUME(keyLength >= prefixLength);
   validate();
   if (!requestSlotAndSpace(keyLength - prefixLength + payloadLength)) {
      assert(freeSpace() < pageSizeLeaf);
      return false;
   }
   uint8_t hash = compute_hash(key + prefixLength, keyLength - prefixLength);
   int index = findIndex(key, keyLength, hash);
   if (index < 0) {
      storeKeyValue(count, key, keyLength, payload, payloadLength, hash);
      count += 1;
   } else {
      storeKeyValue(index, key, keyLength, payload, payloadLength, hash);
      spaceUsed -= (keyLength - prefixLength + payloadLength);
   }
   assert(freeSpace() < pageSizeLeaf);
   validate();
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
   validate();
   if (sortedCount == count)
      return;
   // TODO could preserve hashes with some effort
   SlotProxy* slotProxy = reinterpret_cast<SlotProxy*>(slot);
   sortNode = this;
   std::sort(slotProxy + sortedCount, slotProxy + count);
   if (hashSortUseStdMerge) {
      std::inplace_merge(slotProxy, slotProxy + sortedCount, slotProxy + count);
      for (unsigned i = 0; i < count; ++i) {
         updateHash(i);
      }
   } else {
      printf("this crashes on remove with INT=1e6 SORT=1");
      abort();

      unsigned slotLen1 = sortedCount;
      unsigned slotLen2 = count - sortedCount;
      HashSlot slot2[slotLen2];
      memcpy(slot2, slot + slotLen1, slotLen2 * sizeof(HashSlot));
      unsigned writeSlot = count;
      while (writeSlot > 0) {
         ASSUME(slotLen1 < writeSlot);
         bool slot2greater = *reinterpret_cast<SlotProxy*>(slot + slotLen1 - 1) < *reinterpret_cast<SlotProxy*>(slot2 + slotLen2 - 1);
         if (slot2greater) {
            slot[--writeSlot] = slot2[--slotLen2];
            if (slotLen2 == 0)
               break;

         } else {
            slot[--writeSlot] = slot[--slotLen1];
            if (slotLen1 == 0) {
               memcpy(slot, slot2, slotLen2 * sizeof(HashSlot));
               break;
            }
         }
      }
      for (unsigned i = slotLen1; i < count; ++i) {
         updateHash(i);
      }
   }

   sortedCount = count;
   validate();
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

void HashNode::splitToBasic(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   // split this node into nodeLeft and nodeRight
   assert(sepSlot > 0);
   BTreeNode* nodeLeft = &(AnyNode::allocLeaf())->_basic_node;
   nodeLeft->init(true,rangeOpCounter);
   nodeLeft->setFences(getLowerFence(), lowerFenceLen, sepKey, sepLength);
   TmpBTreeNode right_tmp;
   BTreeNode& right = right_tmp.node;
   right.init(true,rangeOpCounter);
   right.setFences(sepKey, sepLength, getUpperFence(), upperFenceLen);
   bool succ = parent->insertChild(sepKey, sepLength, nodeLeft->any());
   assert(succ);
   copyKeyValueRangeToBasic(nodeLeft, 0, 0, sepSlot + 1);
   copyKeyValueRangeToBasic(&right, 0, nodeLeft->count, count - nodeLeft->count);
   memcpy(this, &right, pageSizeLeaf);
}

bool HashNode::tryConvertToBasic(){
   if(spaceUsed + count*sizeof(BTreeNode::Slot)+sizeof(BTreeNodeHeader) > pageSizeLeaf){
      return false;
   }
   sort();
   TmpBTreeNode tmp_space;
   BTreeNode& tmp = tmp_space.node;
   tmp.init(true,rangeOpCounter);
   tmp.setFences(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
   copyKeyValueRangeToBasic(&tmp, 0, 0, count);
//   printf("### toBasic");
//   print();
//   tmp.print();
   memcpy(this, &tmp, pageSizeLeaf);
   return true;
}

bool HashNode::hasGoodHeads()
{
   unsigned threshold = count / 16;
   unsigned collisionCount = 0;
   for (unsigned i = 1; i < count; ++i) {
      if (slot[i - 1].keyLen > 4 && slot[i].keyLen > 4 && memcmp(getKey(i - 1), getKey(i), 4) == 0) {
         collisionCount += 1;
         if (collisionCount > threshold)
            return false;
      }
   }
   return true;
}

void HashNode::splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   if (enableHashAdapt ) {
      bool goodHeads=hasGoodHeads();
      if(goodHeads){
         rangeOpCounter.setGoodHeads();
         return splitToBasic(parent, sepSlot, sepKey, sepLength);
      }else if (!rangeOpCounter.isLowRange()){
         return splitToBasic(parent, sepSlot, sepKey, sepLength);
      }
   }
   // split this node into nodeLeft and nodeRight
   assert(sepSlot > 0);
   HashNode* nodeLeft = &(AnyNode::allocLeaf())->_hash;
   unsigned capacity = estimateCapacity();
   nodeLeft->init(getLowerFence(), lowerFenceLen, sepKey, sepLength, capacity,rangeOpCounter);
   HashNode right;
   right.init(sepKey, sepLength, getUpperFence(), upperFenceLen, capacity,rangeOpCounter);
   bool succ = parent->insertChild(sepKey, sepLength, nodeLeft->any());
   assert(succ);
   copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
   copyKeyValueRange(&right, 0, nodeLeft->count, count - nodeLeft->count);
   nodeLeft->sortedCount = nodeLeft->count;
   right.sortedCount = right.count;
   nodeLeft->validate();
   right.validate();
   memcpy(this, &right, pageSizeLeaf);
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

void HashNode::copyKeyValueRangeToBasic(BTreeNode* dst, unsigned dstSlot, unsigned srcSlot, unsigned srcCount)
{
   for (unsigned i = 0; i < srcCount; i++)
      copyKeyValueToBasic(srcSlot + i, dst, dstSlot + i);
   dst->count += srcCount;
   dst->makeHint();
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

void HashNode::copyKeyValue(unsigned srcSlot, HashNode* dst, unsigned dstSlot)
{
   unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
   uint8_t key[fullLength];
   memcpy(key, getLowerFence(), prefixLength);
   memcpy(key + prefixLength, getKey(srcSlot), slot[srcSlot].keyLen);
   dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), slot[srcSlot].payloadLen,
                      compute_hash(key + dst->prefixLength, fullLength - dst->prefixLength));
}

void HashNode::copyKeyValueToBasic(unsigned srcSlot, BTreeNode* dst, unsigned dstSlot)
{
   unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
   uint8_t key[fullLength];
   memcpy(key, getLowerFence(), prefixLength);
   memcpy(key + prefixLength, getKey(srcSlot), slot[srcSlot].keyLen);
   dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), slot[srcSlot].payloadLen);
}

void HashNode::storeKeyValue(unsigned slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength, uint8_t hash)
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
   hashes()[slotId] = hash;
}

bool HashNode::remove(uint8_t* key, unsigned keyLength)
{
   validate();
   int index = findIndex(key, keyLength, compute_hash(key + prefixLength, keyLength - prefixLength));
   if (index < 0)
      return false;
   auto x = removeSlot(index);
   validate();
   return x;
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
   unsigned newHashCapacity = max(min(estimateCapacity(), right->estimateCapacity()), count + right->count);
   tmp.init(getLowerFence(), lowerFenceLen, right->getUpperFence(), right->upperFenceLen, newHashCapacity,rangeOpCounter);
   unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
   unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
   unsigned spaceUpperBound =
       spaceUsed + right->spaceUsed + newHashCapacity + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
   if (spaceUpperBound > pageSizeLeaf)
      return false;
   copyKeyValueRange(&tmp, 0, 0, count);
   right->copyKeyValueRange(&tmp, count, 0, right->count);
   parent->innerRemoveSlot(slotId);
   tmp.validate();
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
   if(rangeOpCounter.range_op() && tryConvertToBasic()){
      return reinterpret_cast<BTreeNode*>(this)->range_lookup(key,keyLen,keyOut,found_record_cb);
   }
   sort();
   bool found;
   for (unsigned i = (key == nullptr) ? 0 : lowerBound(key, keyLen, found); i < count; ++i) {
      memcpy(keyOut + prefixLength, getKey(i), slot[i].keyLen);
      if (!found_record_cb(slot[i].keyLen + prefixLength, getPayload(i), slot[i].payloadLen)) {
         return false;
      }
   }
   return true;
}

bool HashNode::range_lookup_desc(uint8_t* key,
                                 unsigned keyLen,
                                 uint8_t* keyOut,
                                 // called with keylen and value
                                 // scan continues if callback returns true
                                 const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   sort();
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

// lower bound search, foundOut indicates if there is an exact match, returns slotId
unsigned HashNode::lowerBound(uint8_t* key, unsigned keyLength, bool& found)
{
   found = false;
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
            found = true;
            return mid;
         }
      }
   }
   return lower;
}

void HashNode::validate()
{
#ifdef NDEBUG
   return;
#endif
   // space used
   unsigned used = upperFenceLen + lowerFenceLen;
   for (unsigned i = 0; i < count; ++i)
      used += slot[i].keyLen + slot[i].payloadLen;
   assert(used == spaceUsed);
   assert(lowerFenceLen == prefixLength && upperFenceLen > prefixLength || upperFenceLen == 0 ||
          lowerFenceLen > prefixLength && upperFenceLen > prefixLength && getLowerFence()[prefixLength] < getUpperFence()[prefixLength]);
   sortNode = this;
   for (unsigned i = 1; i < sortedCount; ++i)
      assert(*(reinterpret_cast<SlotProxy*>(slot + (i - 1))) < *(reinterpret_cast<SlotProxy*>(slot + i)));

   {  // check hashes
      for (unsigned i = 0; i < count; ++i) {
         uint8_t h = compute_hash(getKey(i), slot[i].keyLen);
         assert(h == hashes()[i]);
      }
   }
}
