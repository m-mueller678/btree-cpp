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
   ptr->_hash.init(nullptr, 0, nullptr, 0);
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

void HashNode::init(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen)
{
   assert(sizeof(HashNode) == pageSize);
   _tag = Tag::Hash;
   count = 0;
   sortedCount = 0;
   spaceUsed = upperFenceLen + lowerFenceLen;
   dataOffset = pageSize - spaceUsed;
   hashCapacity = 16;
   hashOffset = dataOffset - hashCapacity;
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
      return true;
   }
}

void HashNode::compactify()
{
   unsigned should = freeSpaceAfterCompaction();
   HashNode tmp;
   tmp.init(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
   abort();  // TODO
   //   copyKeyValueRange(&tmp, 0, 0, count);
   //   tmp.upper = upper;
   //   memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
   //   makeHint();
   //   assert(freeSpace() == should);
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

bool HashNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert(findIndex(key, keyLength) < 0);
   assert(keyLength >= prefixLength);
   keyLength -= prefixLength;
   key += prefixLength;
   if (!requestSlotAndSpace(keyLength + payloadLength))
      return false;
   dataOffset -= payloadLength + keyLength;
   slot[count].keyLen = keyLength;
   slot[count].payloadLen = payloadLength;
   slot[count].offset = dataOffset;
   memcpy(getKey(count), key, keyLength);
   memcpy(getPayload(count), payload, payloadLength);
   count += 1;
   return true;
}