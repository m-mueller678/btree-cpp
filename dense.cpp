#include <cstdint>
#include "btree2020.hpp"

unsigned DenseNode::lowerFenceOffset()
{
   return pageSize - lowerFenceLen - max(upperFenceLen, fullKeyLen);
}
uint8_t* DenseNode::getLowerFence()
{
   return ptr() + pageSize - lowerFenceLen;
}
uint8_t* DenseNode::getUpperFence()
{
   return ptr() + lowerFenceOffset();
}

AnyNode* DenseNode::any()
{
   return reinterpret_cast<AnyNode*>(this);
}

uint8_t* DenseNode::getPrefix()
{
   return getLowerFence();
}

void DenseNode::restoreKey(uint8_t* prefix, uint8_t* dst, unsigned index)
{
   unsigned numericPartLen= computeNumericPartLen(prefixLength,fullKeyLen);
   memcpy(dst,prefix,fullKeyLen-numericPartLen);
   NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(index));
   memcpy(dst+fullKeyLen-numericPartLen,reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen,numericPartLen);
}

void DenseNode::changeUpperFence(uint8_t* fence, unsigned len)
{
   assert(upperFenceLen <= fullKeyLen);
   upperFenceLen = len;
   uint8_t* uf = getUpperFence();
   memcpy(uf, fence, len);
   uint8_t* lf = getLowerFence();
   prefixLength = 0;
   while (prefixLength < upperFenceLen && prefixLength < lowerFenceLen && lf[prefixLength] == uf[prefixLength]) {
      prefixLength += 1;
   }
   // TODO could make numeric part extraction independent of prefix len
   updateArrayStart();
}

void DenseNode::copyKeyValueRangeToBasic(BTreeNode* dst, unsigned srcStart, unsigned srcEnd)
{
   assert(dst->prefixLength >= prefixLength);
   assert(dst->count == 0);
   unsigned npLen = computeNumericPartLen(prefixLength, fullKeyLen);
   unsigned outSlot = 0;
   for (unsigned i = srcStart; i < srcEnd; i++) {
      if (!isSlotPresent(i)) {
         continue;
      }
      NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(i));
      unsigned newKeyLength = fullKeyLen - dst->prefixLength;
      unsigned space = newKeyLength + valLen;
      dst->dataOffset -= space;
      dst->spaceUsed += space;
      dst->slot[outSlot].offset = dst->dataOffset;
      dst->slot[outSlot].keyLen = fullKeyLen - dst->prefixLength;
      dst->slot[outSlot].payloadLen = valLen;
      memcpy(dst->getPayload(outSlot), getVal(i), valLen);
      memcpy(dst->getPayload(outSlot) - npLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - npLen, valLen);
      memcpy(dst->getKey(outSlot), getLowerFence() + dst->prefixLength, fullKeyLen - dst->prefixLength - npLen);
      outSlot += 1;
   }
   dst->count = outSlot;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

bool DenseNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert(keyLength >= prefixLength);
   if (payloadLength != valLen || keyLength != fullKeyLen) {
      // TODO chek capacity
      BTreeNode tmp{true};
      tmp.setFences(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
      copyKeyValueRangeToBasic(&tmp, 0, slotCount);
      tmp.makeHint();
      BTreeNode* basicNode = reinterpret_cast<BTreeNode*>(this);
      *basicNode = tmp;
      return basicNode->insert(key, keyLength, payload, payloadLength);
   }
   KeyError keyIndex = keyToIndex(key + prefixLength, keyLength - prefixLength);
   switch (keyIndex) {
      case KeyError::SlightlyTooLarge:
      case KeyError::FarTooLarge:
      case KeyError::NotNumericRange:
         return false;
      case KeyError::WrongLen:
         abort();
   }
   assert(keyIndex >= 0);
   setSlotPresent(keyIndex);
   memcpy(getVal(keyIndex), payload, payloadLength);
   return true;
}

void DenseNode::splitNode(BTreeNode* parent, uint8_t* key, unsigned keyLen)
{
   assert(keyLen >= prefixLength);
   int key_index = keyToIndex(key + prefixLength, keyLen - prefixLength);
   bool split_to_self;
   switch (key_index) {
      case KeyError::FarTooLarge:
      case KeyError::NotNumericRange:
         split_to_self = false;
         break;
      case KeyError::WrongLen:
         // splitting into two new basic nodes might be impossible if prefix length is short
         if (upperFenceLen < fullKeyLen) {
            split_to_self = false;
         } else {
            // TODO split to two basic nodes instead
            split_to_self = false;
         }
         break;
      case KeyError::SlightlyTooLarge:
         split_to_self = true;
         break;
   }
   uint8_t full_boundary[fullKeyLen];
   restoreKey(key, full_boundary, slotCount - 1);

   DenseNode* denseLeft = reinterpret_cast<DenseNode*>(new AnyNode());
   memcpy(denseLeft, this, sizeof(DenseNode));
   bool succ = parent->insert(full_boundary, fullKeyLen, reinterpret_cast<uint8_t*>(&denseLeft), sizeof(BTreeNode*));
   assert(succ);
   if (split_to_self) {
      this->init(full_boundary, denseLeft->fullKeyLen, denseLeft->getUpperFence(), denseLeft->upperFenceLen, denseLeft->prefixLength,
                 denseLeft->fullKeyLen, denseLeft->valLen);
   } else {
      BTreeNode* right = &this->any()->_basic_node;
      // TODO check move constructor semantics
      *right = BTreeNode{true};
      right->setFences(full_boundary, denseLeft->fullKeyLen, denseLeft->getUpperFence(), denseLeft->upperFenceLen);
   }
   denseLeft->changeUpperFence(full_boundary, fullKeyLen);
}

unsigned DenseNode::prefixDiffLen()
{
   // TODO this gets called a lot?
   return computeNumericPrefixLength(prefixLength, fullKeyLen) - prefixLength;
}

KeyError DenseNode::keyToIndex(uint8_t* truncatedKey, unsigned truncatedLen)
{
   if (truncatedLen + prefixLength != fullKeyLen) {
      return KeyError::WrongLen;
   }
   if (memcmp(getLowerFence() + prefixLength, truncatedKey, prefixDiffLen()) != 0) {
      assert(memcmp(getLowerFence() + prefixLength, truncatedKey, prefixDiffLen()) > 0);
      return KeyError::NotNumericRange;
   }
   NumericPart numericPart = getNumericPart(truncatedKey + prefixDiffLen(), truncatedLen - prefixDiffLen());
   assert(numericPart >= arrayStart);
   NumericPart index = numericPart - arrayStart;
   if (index < slotCount) {
      return static_cast<KeyError>(index);
   } else if (index < slotCount + slotCount / 2) {
      // TODO might scrap this distinction
      return KeyError::SlightlyTooLarge;
   } else {
      return KeyError::FarTooLarge;
   }
}

unsigned DenseNode::computeNumericPartLen(unsigned prefixLength, unsigned fullKeyLen)
{
   return min(maxNumericPartLen, fullKeyLen - prefixLength);
}

unsigned DenseNode::computeNumericPrefixLength(unsigned prefixLength, unsigned fullKeyLen)
{
   return fullKeyLen - computeNumericPartLen(prefixLength, fullKeyLen);
}

void DenseNode::init(uint8_t* lowerFence,
                     unsigned lowerFenceLen,
                     uint8_t* upperFence,
                     unsigned upperFenceLen,
                     unsigned prefixLength,
                     unsigned fullKeyLen,
                     unsigned valLen)
{
   assert(sizeof (DenseNode)==pageSize);
   tag = Tag::Dense;
   this->fullKeyLen = fullKeyLen;
   this->valLen = valLen;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   this->prefixLength = prefixLength;
   assert(lowerFenceLen <= fullKeyLen);
   assert(computeNumericPrefixLength(prefixLength, fullKeyLen) <= lowerFenceLen);
   slotCount = computeSlotCount(valLen, lowerFenceOffset());
   zeroMask();
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   updateArrayStart();
}

unsigned DenseNode::mask_word_count()
{
   return (slotCount + maskBitsPerWord - 1) / maskBitsPerWord;
}

void DenseNode::zeroMask()
{
   unsigned mwc = mask_word_count();
   for (unsigned i = 0; i < mwc; ++i) {
      mask[i] = 0;
   }
}

// key is expected to be prefix truncated
NumericPart DenseNode::getNumericPart(uint8_t* key, unsigned len)
{
   switch (len) {
      case 0:
         return 0;
      case 1:
         return static_cast<uint32_t>(key[0]);
      case 2:
         return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key)));
      case 3:
         return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 8) | (static_cast<uint32_t>(key[2]));
      default:
         return __builtin_bswap32(loadUnaligned<uint32_t>(key + len - 4));
   }
}

void DenseNode::updateArrayStart()
{
   if (lowerFenceLen < fullKeyLen) {
      // TODO this might be simplified
      uint8_t zeroPaddedLowerFenceTail[maxNumericPartLen];
      unsigned lowerFenceTailLen = min(4, lowerFenceLen - prefixLength);
      memcpy(zeroPaddedLowerFenceTail, getLowerFence() + lowerFenceLen - lowerFenceTailLen, lowerFenceTailLen);
      for (unsigned i = lowerFenceTailLen; i < maxNumericPartLen; ++i) {
         zeroPaddedLowerFenceTail[i] = 0;
      }
      arrayStart = getNumericPart(zeroPaddedLowerFenceTail, lowerFenceTailLen);
   } else {
      arrayStart = getNumericPart(getLowerFence() + prefixLength, fullKeyLen - prefixLength) + 1;
   }
}

uint8_t* DenseNode::ptr()
{
   return reinterpret_cast<uint8_t*>(this);
}

unsigned DenseNode::computeSlotCount(unsigned valLen, unsigned fencesStart)
{
   unsigned count = fencesStart * 8 / (valLen * 8 + 1);
   while (true) {
      unsigned maskSize = (count + maskBitsPerWord - 1) / maskBitsPerWord * maskBytesPerWord;
      if (maskSize + count * valLen > fencesStart) {
         count -= 1;
      } else {
         return count;
      }
   }
}

bool DenseNode::try_densify(BTreeNode* basicNode)
{
   assert(basicNode->count > 0);
   unsigned pre_key_len_1 = basicNode->slot[0].keyLen;
   unsigned fullKeyLen = pre_key_len_1 + basicNode->prefixLength;
   unsigned numericPrefixLen = computeNumericPrefixLength(basicNode->prefixLength, fullKeyLen);
   if (numericPrefixLen > basicNode->lowerFence.length) {
      // this might be possible to handle, but requires more thought and should be rare.
      return false;
   }
   unsigned valLen1 = basicNode->slot[0].payloadLen;
   for (unsigned i = 1; i < basicNode->count; ++i) {
      if (basicNode->slot[i].keyLen != pre_key_len_1 || basicNode->slot[i].payloadLen != valLen1) {
         return false;
      }
   }

   // preconditios confirmed, create.
   init(basicNode->getLowerFence(), basicNode->lowerFence.length, basicNode->getUpperFence(), basicNode->upperFence.length, basicNode->prefixLength,
        fullKeyLen, valLen1);
   KeyError lastKey = keyToIndex(basicNode->getKey(basicNode->count - 1), fullKeyLen - prefixLength);
   if(lastKey<0){
      return false;
   }
   for (unsigned i = 0; i < basicNode->count; ++i) {
      int index = keyToIndex(basicNode->getKey(i), fullKeyLen - prefixLength);
      assert(index >= 0);
      setSlotPresent(index);
      memcpy(getVal(index), basicNode->getPayload(i), valLen1);
   }
   return true;
}

bool DenseNode::isSlotPresent(unsigned i)
{
   assert(i < slotCount);
   return (mask[i / maskBitsPerWord] >> (i % maskBitsPerWord) & 1) != 0;
}

void DenseNode::setSlotPresent(unsigned i)
{
   assert(i < slotCount);
   mask[i / maskBitsPerWord] |= Mask(1) << (i % maskBitsPerWord);
}

void DenseNode::unsetSlotPresent(unsigned i)
{
   assert(i < slotCount);
   mask[i / maskBitsPerWord] &= ~(Mask(1) << (i % maskBitsPerWord));
}

uint8_t* DenseNode::getVal(unsigned i)
{
   assert(i < slotCount);
   return ptr() + mask_word_count() * maskBytesPerWord + i * valLen;
}
