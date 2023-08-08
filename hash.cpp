#include <algorithm>
#include "btree2020.hpp"

uint8_t HashNode::compute_hash(uint8_t* key, unsigned keyLength)
{
   return 4;  // TODO
}

uint8_t* HashNode::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   int index = findIndex(key, 0);
   if (index >= 0) {
      payloadSizeOut = slot[index].payloadLen;
      return getPayload(index);
   }
   return nullptr;
}

int HashNode::findIndex(uint8_t* key, unsigned keyLength)
{
   key += prefixLength;
   keyLength -= prefixLength;
   // TODO use simd
   uint8_t needle = compute_hash(key, keyLength);
   for (unsigned i = 0; i < count; ++i) {
      if (hashes()[i] == needle && keyLength == slot[i].keyLen && memcmp(getKey(i), key, keyLength) == 0) {
         return i;
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
   return pageSize - (reinterpret_cast<uint8_t*>(slot + count) - ptr()) - spaceUsed;
}

bool HashNode::requestSlotAndSpace(unsigned kvSize)
{
   if (count < hashCapacity) {
      return requestSpace(kvSize + sizeof(HashSlot));
   } else {
      if (!requestSpace(hashCapacity * 2 + kvSize + sizeof(HashSlot)))
         return false;
      dataOffset -= hashCapacity * 2;
      memcpy(ptr() + dataOffset, hashes(), count);
      hashOffset = dataOffset;
      hashCapacity *= 2;
      spaceUsed += hashCapacity;
      return true;
   }
}

void HashNode::compactify()
{
   unsigned newHashCapacity = count + 1;
   unsigned should = freeSpaceAfterCompaction() + hashCapacity - newHashCapacity;
   HashNode tmp;
   tmp.init(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen, newHashCapacity);
   memcpy(tmp.hashes(), hashes(), count);
   memcpy(tmp.slot, slot, sizeof(HashSlot) * count);
   for (unsigned i = 0; i < count; ++i) {
      uint16_t entryLen = slot[i].keyLen + slot[i].payloadLen;
      tmp.dataOffset -= entryLen;
      tmp.spaceUsed += entryLen;
      tmp.slot[i].keyLen = slot[i].keyLen;
      tmp.slot[i].payloadLen = slot[i].payloadLen;
      tmp.slot[i].offset = tmp.dataOffset;
      memcpy(getKey(i), tmp.getKey(i), entryLen);
   }
   tmp.count = count;
   assert(tmp.freeSpace() == should);
   *this = tmp;
}

bool HashNode::requestSpace(unsigned spaceNeeded)
{
   if (spaceNeeded <= freeSpace())
      return true;
   if (spaceNeeded <= freeSpaceAfterCompaction()) {
      compactify();
      return true;
   }
   return false;
}

struct HashSlotReferenceDeref {
   HashNode* node;
   unsigned slot;
};

bool HashNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert(findIndex(key, keyLength) < 0);
   assert(keyLength >= prefixLength);
   if (!requestSlotAndSpace(keyLength - prefixLength + payloadLength))
      return false;
   storeKeyValue(count, key, keyLength, payload, payloadLength);
   count += 1;
   return true;
}

static HashNode* sortNode;
struct SlotProxy {
   HashSlot slot;

   friend bool operator<(const SlotProxy& l, const SlotProxy& r)
   {
      uint8_t* lptr = sortNode->ptr() + l.slot.offset;
      uint8_t* rptr = sortNode->ptr() + r.slot.offset;
      return std::lexicographical_compare(lptr, lptr + l.slot.offset, rptr, rptr + r.slot.offset);
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

void HashNode::splitNode(BTreeNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
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
