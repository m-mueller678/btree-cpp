#include <cstddef>
#include <cstdint>
#include "btree2020.hpp"

unsigned DenseNode::fencesOffset()
{
   if (tag == Tag::Dense) {
      return pageSize - lowerFenceLen - max(upperFenceLen, fullKeyLen);
   } else {
      return pageSize - lowerFenceLen - upperFenceLen;
   }
}
uint8_t* DenseNode::getLowerFence()
{
   return ptr() + pageSize - lowerFenceLen;
}
uint8_t* DenseNode::getUpperFence()
{
   return ptr() + fencesOffset();
}

uint8_t* DenseNode::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   KeyError index = keyToIndex(key + prefixLength, keyLength - prefixLength);
   if (index < 0 || !isSlotPresent(index)) {
      return nullptr;
   }
   payloadSizeOut = valLen;
   return getVal(index);
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
   unsigned numericPartLen = computeNumericPartLen(prefixLength, fullKeyLen);
   memcpy(dst, prefix, fullKeyLen - numericPartLen);
   NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(index));
   memcpy(dst + fullKeyLen - numericPartLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
}

void DenseNode::changeUpperFence(uint8_t* fence, unsigned len)
{
   ASSUME(tag == Tag::Dense);
   ASSUME(len <= fullKeyLen);
   upperFenceLen = len;
   memcpy(getUpperFence(), fence, len);
   updatePrefixLength();
   // TODO could make numeric part extraction independent of prefix len
   updateArrayStart();
}

void DenseNode::updatePrefixLength()
{
   uint8_t* uf = getUpperFence();
   uint8_t* lf = getLowerFence();
   prefixLength = 0;
   while (prefixLength < upperFenceLen && prefixLength < lowerFenceLen && lf[prefixLength] == uf[prefixLength]) {
      prefixLength += 1;
   }
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
      memcpy(dst->getPayload(outSlot) - npLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - npLen, npLen);
      memcpy(dst->getKey(outSlot), getLowerFence() + dst->prefixLength, fullKeyLen - dst->prefixLength - npLen);
      if (enableBasicHead)
         dst->slot[outSlot].head[0] = head(dst->getKey(outSlot), fullKeyLen - prefixLength);
      outSlot += 1;
   }
   dst->count = outSlot;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
}

bool DenseNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert(keyLength >= prefixLength);
   if (tag == Tag::Dense) {
      if (payloadLength != valLen || keyLength != fullKeyLen) {
         unsigned entrySize = occupiedCount * (fullKeyLen - prefixLength + payloadLength + sizeof(BTreeNode::Slot));
         if (entrySize + lowerFenceLen + upperFenceLen + sizeof(BTreeNodeHeader) <= pageSize) {
            BTreeNode* basicNode = convertToBasic();
            return basicNode->insert(key, keyLength, payload, payloadLength);
         } else {
            return false;
         }
      }
      KeyError keyIndex = keyToIndex(key + prefixLength, keyLength - prefixLength);
      switch (keyIndex) {
         case KeyError::SlightlyTooLarge:
         case KeyError::FarTooLarge:
         case KeyError::NotNumericRange:
            return false;
         case KeyError::WrongLen:
            ASSUME(false);
      }
      assert(keyIndex >= 0);
      occupiedCount += !isSlotPresent(keyIndex);
      setSlotPresent(keyIndex);
      memcpy(getVal(keyIndex), payload, payloadLength);
      return true;
   } else {
      if (keyLength != fullKeyLen) {
         unsigned entrySize = occupiedCount * (fullKeyLen - prefixLength + sizeof(BTreeNode::Slot) - 2) + spaceUsed;
         if (entrySize + lowerFenceLen + upperFenceLen + sizeof(BTreeNodeHeader) <= pageSize) {
            BTreeNode* basicNode = convertToBasic();
            return basicNode->insert(key, keyLength, payload, payloadLength);
         } else {
            return false;
         }
      }
      KeyError keyIndex = keyToIndex(key + prefixLength, keyLength - prefixLength);
      switch (keyIndex) {
         case KeyError::SlightlyTooLarge:
         case KeyError::FarTooLarge:
         case KeyError::NotNumericRange:
            return false;
         case KeyError::WrongLen:
            ASSUME(false);
      }
      assert(keyIndex >= 0);
      if (slots[keyIndex]) {
         spaceUsed -= loadUnaligned<uint16_t>(ptr() + slots[keyIndex]) + 2;
         slots[keyIndex] = 0;
         occupiedCount -= 1;
      }
      if (!requestSpaceFor(payloadLength))
         return false;
      insertSlotWithSpace(keyIndex, payload, payloadLength);
      occupiedCount += 1;
      return true;
   }
}

BTreeNode* DenseNode::convertToBasic()
{
   BTreeNode tmp{true};
   tmp.setFences(getLowerFence(), lowerFenceLen, getUpperFence(), upperFenceLen);
   copyKeyValueRangeToBasic(&tmp, 0, slotCount);
   tmp.makeHint();
   BTreeNode* basicNode = reinterpret_cast<BTreeNode*>(this);
   *basicNode = tmp;
   return basicNode;
}

void DenseNode::splitNode1(AnyNode* parent, uint8_t* key, unsigned keyLen)
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
   restoreKey(getLowerFence(), full_boundary, slotCount - 1);

   DenseNode* denseLeft = reinterpret_cast<DenseNode*>(new AnyNode());
   memcpy(denseLeft, this, sizeof(DenseNode));
   bool succ = parent->insertChild(full_boundary, fullKeyLen, denseLeft->any());
   assert(succ);
   if (split_to_self) {
      this->init(full_boundary, denseLeft->fullKeyLen, denseLeft->getUpperFence(), denseLeft->upperFenceLen, denseLeft->fullKeyLen,
                 denseLeft->valLen);
   } else {
      BTreeNode* right = &this->any()->_basic_node;
      *right = BTreeNode{true};
      right->setFences(full_boundary, denseLeft->fullKeyLen, denseLeft->getUpperFence(), denseLeft->upperFenceLen);
   }
   denseLeft->changeUpperFence(full_boundary, denseLeft->fullKeyLen);
}

void DenseNode::splitNode2(AnyNode* parent, uint8_t* key, unsigned keyLen)
{
   unsigned leftSlots = 0;
   unsigned leftSize = 0;
   unsigned splitSlot = 0;
   while (leftSlots < occupiedCount / 2) {
      ASSUME(splitSlot < slotCount);
      if (slots[splitSlot]) {
         leftSlots += 1;
         leftSize += 2 + slotValLen(splitSlot);
      }
      splitSlot += 1;
   }
   DenseNode* left = reinterpret_cast<DenseNode*>(new AnyNode());
   uint8_t splitKeyBuffer[fullKeyLen];
   restoreKey(getLowerFence(), splitKeyBuffer, splitSlot);
   left->init2b(getLowerFence(), lowerFenceLen, splitKeyBuffer, fullKeyLen, fullKeyLen, splitSlot + 1);
   for (unsigned i = 0; i <= splitSlot; ++i) {
      if (slots[i])
         left->insertSlotWithSpace(i, ptr() + slots[i] + 2, slotValLen(i));
   }
   DenseNode right;
   right.init2b(splitKeyBuffer, fullKeyLen, getUpperFence(), upperFenceLen, fullKeyLen, slotCount - splitSlot - 1);
   for (unsigned i = splitSlot + 1; i < slotCount; ++i) {
      if (slots[i])
         right.insertSlotWithSpace(i - splitSlot - 1, ptr() + slots[i] + 2, slotValLen(i));
   }
   bool succ = parent->insertChild(splitKeyBuffer, fullKeyLen, left->any());
   ASSUME(succ);
   memcpy(this, &right, pageSize);
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
      assert(memcmp(getLowerFence() + prefixLength, truncatedKey, prefixDiffLen()) < 0);
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

void DenseNode::init(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen, unsigned fullKeyLen, unsigned valLen)
{
   assert(enablePrefix);
   assert(sizeof(DenseNode) == pageSize);
   tag = Tag::Dense;
   this->fullKeyLen = fullKeyLen;
   this->valLen = valLen;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   occupiedCount = 0;
   // assert(lowerFenceLen <= fullKeyLen);
   slotCount = computeSlotCount(valLen, fencesOffset());
   zeroMask();
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   this->updatePrefixLength();
   assert(computeNumericPrefixLength(prefixLength, fullKeyLen) <= lowerFenceLen);
   updateArrayStart();
}

void DenseNode::init2(uint8_t* lowerFence,
                      unsigned lowerFenceLen,
                      uint8_t* upperFence,
                      unsigned upperFenceLen,
                      unsigned fullKeyLen,
                      NumericPart lastElement,
                      unsigned count,
                      unsigned totalPayloadSize)
{
   assert(enablePrefix);
   assert(sizeof(DenseNode) == pageSize);
   tag = Tag::Dense2;
   this->fullKeyLen = fullKeyLen;
   this->spaceUsed = 0;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   occupiedCount = 0;
   this->dataOffset = fencesOffset();
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   this->updatePrefixLength();
   this->updateArrayStart();
   COUNTER(reject_1, arrayStart >= lastElement, 1 << 5);
   if (arrayStart >= lastElement) {
      slotCount = 0;
      return;
   }
   {
      unsigned availableSpace = fencesOffset() - offsetof(DenseNode, slots);
      float density = float(count) / (lastElement - arrayStart);
      float spacePerSlot = 2 + density * (2 + float(totalPayloadSize) / count);
      slotCount = availableSpace / spacePerSlot;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wassume"
      ASSUME(slotEndOffset() <= dataOffset);
#pragma clang diagnostic pop
   }
   COUNTER(reject_2, slotEndOffset() + totalPayloadSize > fencesOffset() || arrayStart + slotCount < lastElement, 1 << 5);
   if (slotEndOffset() + totalPayloadSize > fencesOffset() || arrayStart + slotCount < lastElement) {
      slotCount = 0;
      return;
   }
   zeroSlots();
   assert(computeNumericPrefixLength(prefixLength, fullKeyLen) <= lowerFenceLen);
}

void DenseNode::init2b(uint8_t* lowerFence,
                       unsigned lowerFenceLen,
                       uint8_t* upperFence,
                       unsigned upperFenceLen,
                       unsigned fullKeyLen,
                       unsigned slotCount)
{
   assert(enablePrefix);
   assert(sizeof(DenseNode) == pageSize);
   tag = Tag::Dense2;
   this->fullKeyLen = fullKeyLen;
   this->spaceUsed = 0;
   this->lowerFenceLen = lowerFenceLen;
   this->upperFenceLen = upperFenceLen;
   occupiedCount = 0;
   this->dataOffset = fencesOffset();
   memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
   memcpy(this->getUpperFence(), upperFence, upperFenceLen);
   this->updatePrefixLength();
   this->updateArrayStart();
   this->slotCount = slotCount;
   zeroSlots();
   assert(computeNumericPrefixLength(prefixLength, fullKeyLen) <= lowerFenceLen);
}

bool DenseNode::is_underfull()
{
   unsigned totalEntrySize = (fullKeyLen - prefixLength + valLen + sizeof(BTreeNode::Slot)) * occupiedCount;
   return sizeof(BTreeNodeHeader) + totalEntrySize + lowerFenceLen + upperFenceLen < pageSize - BTreeNode::underFullSize;
}

unsigned DenseNode::maskWordCount()
{
   return (slotCount + maskBitsPerWord - 1) / maskBitsPerWord;
}

void DenseNode::zeroMask()
{
   ASSUME(tag == Tag::Dense);
   unsigned mwc = maskWordCount();
   for (unsigned i = 0; i < mwc; ++i) {
      mask[i] = 0;
   }
}

void DenseNode::zeroSlots()
{
   ASSUME(tag == Tag::Dense2);
   for (unsigned i = 0; i < slotCount; ++i) {
      slots[i] = 0;
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
      unsigned numericPrefixLength = computeNumericPrefixLength(prefixLength, fullKeyLen);
      uint8_t zeroPaddedLowerFenceTail[maxNumericPartLen];
      ASSUME(lowerFenceLen >= numericPrefixLength);
      unsigned lowerFenceTailLen = lowerFenceLen - numericPrefixLength;
      memcpy(zeroPaddedLowerFenceTail, getLowerFence() + numericPrefixLength, lowerFenceTailLen);
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
      if (offsetof(DenseNode, mask) + maskSize + count * valLen > fencesStart) {
         count -= 1;
      } else {
         return count;
      }
   }
}

bool DenseNode::remove(uint8_t* key, unsigned keyLength)
{
   KeyError index = keyToIndex(key + prefixLength, keyLength - prefixLength);
   if (index < 0 || !isSlotPresent(index)) {
      return false;
   }
   unsetSlotPresent(index);
   occupiedCount -= 1;
   return true;
}

bool DenseNode::try_densify(BTreeNode* basicNode)
{
   assert(!enableDense || !enableDense2);
   assert(basicNode->count > 0);
   unsigned pre_key_len_1 = basicNode->slot[0].keyLen;
   unsigned fullKeyLen = pre_key_len_1 + basicNode->prefixLength;
   unsigned numericPrefixLen = computeNumericPrefixLength(basicNode->prefixLength, fullKeyLen);
   COUNTER(reject_0, numericPrefixLen > basicNode->lowerFence.length, 1 << 5);
   if (numericPrefixLen > basicNode->lowerFence.length) {
      // this might be possible to handle, but requires more thought and should be rare.
      return false;
   }
   unsigned valLen1;
   if (enableDense) {
      valLen1 = basicNode->slot[0].payloadLen;
      for (unsigned i = 1; i < basicNode->count; ++i) {
         if (basicNode->slot[i].keyLen != pre_key_len_1 || basicNode->slot[i].payloadLen != valLen1) {
            return false;
         }
      }
      // preconditios confirmed, create.
      init(basicNode->getLowerFence(), basicNode->lowerFence.length, basicNode->getUpperFence(), basicNode->upperFence.length, fullKeyLen, valLen1);
   } else {
      for (unsigned i = 1; i < basicNode->count; ++i) {
         if (basicNode->slot[i].keyLen != pre_key_len_1) {
            return false;
         }
      }
      unsigned totalValLen = 0;
      for (unsigned i = 0; i < basicNode->count; ++i) {
         totalValLen += basicNode->slot[i].payloadLen;
      }
      NumericPart lastNumeric = getNumericPart(basicNode->getKey(basicNode->count - 1), basicNode->slot[basicNode->count - 1].keyLen);
      init2(basicNode->getLowerFence(), basicNode->lowerFence.length, basicNode->getUpperFence(), basicNode->upperFence.length, fullKeyLen,
            lastNumeric, basicNode->count, totalValLen);
   }
   KeyError lastKey = keyToIndex(basicNode->getKey(basicNode->count - 1), fullKeyLen - prefixLength);
   COUNTER(reject_last_key, lastKey < 0, 1 << 5);
   if (lastKey < 0) {
      return false;
   }
   for (unsigned i = 0; i < basicNode->count; ++i) {
      int index = keyToIndex(basicNode->getKey(i), fullKeyLen - prefixLength);
      assert(index >= 0);
      if (enableDense) {
         setSlotPresent(index);
         memcpy(getVal(index), basicNode->getPayload(i), valLen1);
      } else {
         insertSlotWithSpace(index, basicNode->getPayload(i), basicNode->slot[i].payloadLen);
      }
   }
   occupiedCount = basicNode->count;
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
   return ptr() + offsetof(DenseNode, mask) + maskWordCount() * maskBytesPerWord + i * valLen;
}

AnyKeyIndex DenseNode::anyKeyIndex(uint8_t* key, unsigned keyLen)
{
   unsigned numericPrefixLen = computeNumericPrefixLength(prefixLength, fullKeyLen);
   ASSUME(prefixLength <= keyLen);
   ASSUME(prefixLength <= numericPrefixLen);
   ASSUME(numericPrefixLen <= lowerFenceLen);
   ASSUME(lowerFenceLen <= fullKeyLen);
   if (keyLen < fullKeyLen) {
      uint8_t buffer[fullKeyLen];
      memcpy(buffer, key, keyLen);
      memset(buffer + keyLen, 0, fullKeyLen - keyLen);
      KeyError index = keyToIndex(key + prefixLength, fullKeyLen - prefixLength);
      switch (index) {
         case KeyError::WrongLen:
            ASSUME(false);
         case KeyError::NotNumericRange:
            abort();  // TODO
         case KeyError::SlightlyTooLarge:
         case KeyError::FarTooLarge:
            return AnyKeyIndex{static_cast<unsigned>(slotCount - 1), AnyKeyRel::After};
      };
      ASSUME(index >= 0);
      return AnyKeyIndex{static_cast<unsigned>(index), AnyKeyRel::Before};
   } else if (keyLen == fullKeyLen) {
      KeyError index = keyToIndex(key + prefixLength, fullKeyLen - prefixLength);
      switch (index) {
         case KeyError::WrongLen:
            ASSUME(false);
         case KeyError::NotNumericRange:
         case KeyError::SlightlyTooLarge:
         case KeyError::FarTooLarge:
            return AnyKeyIndex{static_cast<unsigned>(slotCount - 1), AnyKeyRel::After};
      }
      ASSUME(index >= 0);
      return AnyKeyIndex{static_cast<unsigned>(index), AnyKeyRel::At};
   } else {
      if (memcmp(getLowerFence(), key, fullKeyLen) == 0) {
         return AnyKeyIndex{0, AnyKeyRel::Before};
      }
      KeyError index = keyToIndex(key + prefixLength, fullKeyLen - prefixLength);
      switch (index) {
         case KeyError::WrongLen:
            ASSUME(false);
         case KeyError::NotNumericRange:
         case KeyError::SlightlyTooLarge:
         case KeyError::FarTooLarge:
            return AnyKeyIndex{static_cast<unsigned>(slotCount - 1), AnyKeyRel::After};
      }
      ASSUME(index >= 0);
      return AnyKeyIndex{static_cast<unsigned>(index), AnyKeyRel::Before};
   }
}

bool DenseNode::range_lookup(uint8_t* key,
                             unsigned keyLen,
                             uint8_t* keyOut,
                             // called with keylen and value
                             // scan continues if callback returns true
                             const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   AnyKeyIndex keyPosition = anyKeyIndex(key, keyLen);
   unsigned firstIndex = keyPosition.index + (keyPosition.rel == AnyKeyRel::After);
   /*        if x == self.slot_count as usize - 1 {
               return true;
           } else {
               x + 1
           }
                             }*/
   unsigned nprefLen = computeNumericPrefixLength(prefixLength, fullKeyLen);
   memcpy(keyOut, getLowerFence(), nprefLen);

   unsigned wordIndex = firstIndex / maskBitsPerWord;
   Mask word = mask[wordIndex];
   unsigned shift = firstIndex % maskBitsPerWord;
   word >>= shift;
   while (true) {
      unsigned trailingZeros = std::__countr_zero(word);
      if (trailingZeros == maskBitsPerWord) {
         wordIndex += 1;
         if (wordIndex >= maskWordCount()) {
            return true;
         }
         shift = 0;
         word = mask[wordIndex];
      } else {
         shift += trailingZeros;
         word >>= trailingZeros;
         unsigned entryIndex = wordIndex * maskBitsPerWord + shift;
         if (entryIndex > slotCount) {
            return true;
         }
         NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(entryIndex));
         unsigned numericPartLen = fullKeyLen - nprefLen;
         memcpy(keyOut + nprefLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
         if (!found_record_cb(fullKeyLen, getVal(entryIndex), valLen)) {
            return false;
         }
         shift += 1;
         word >>= 1;
      }
   }
}

bool DenseNode::range_lookup_desc(uint8_t* key,
                                  unsigned keyLen,
                                  uint8_t* keyOut,
                                  // called with keylen and value
                                  // scan continues if callback returns true
                                  const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   AnyKeyIndex keyPosition = anyKeyIndex(key, keyLen);
   int anyKeyIndex = keyPosition.index;
   int firstIndex = anyKeyIndex - (keyPosition.rel == AnyKeyRel::Before);
   if (firstIndex < 0)
      return true;
   unsigned nprefLen = computeNumericPrefixLength(prefixLength, fullKeyLen);
   memcpy(keyOut, getLowerFence(), nprefLen);

   int wordIndex = firstIndex / maskBitsPerWord;
   Mask word = mask[wordIndex];
   unsigned shift = (maskBitsPerWord - 1 - firstIndex % maskBitsPerWord);
   word <<= shift;
   while (true) {
      unsigned leadingZeros = std::__countl_zero(word);
      if (leadingZeros == maskBitsPerWord) {
         wordIndex -= 1;
         if (wordIndex < 0) {
            return true;
         }
         shift = 0;
         word = mask[wordIndex];
      } else {
         shift += leadingZeros;
         ASSUME(shift < maskBitsPerWord);
         word <<= leadingZeros;
         unsigned entryIndex = wordIndex * maskBitsPerWord + (maskBitsPerWord - 1 - shift);
         NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(entryIndex));
         unsigned numericPartLen = fullKeyLen - nprefLen;
         memcpy(keyOut + nprefLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
         if (!found_record_cb(fullKeyLen, getVal(entryIndex), valLen)) {
            return false;
         }
         shift += 1;
         word <<= 1;
      }
   }
}

void DenseNode::print()
{
   printf("# DenseNode\n");
   printf("lower fence: ");
   printKey(getLowerFence(), lowerFenceLen);
   printf("\nupper fence: ");
   printKey(getUpperFence(), upperFenceLen);
   uint8_t keyBuffer[fullKeyLen];
   printf("\n");
   for (unsigned i = 0; i < slotCount; ++i) {
      if (isSlotPresent(i)) {
         printf("%d: ", i);
         restoreKey(getLowerFence(), keyBuffer, i);
         printKey(keyBuffer + prefixLength, fullKeyLen - prefixLength);
         printf("\n");
      }
   }
}

void DenseNode::insertSlotWithSpace(unsigned int i, uint8_t* payload, unsigned int payloadLen)
{
   spaceUsed += payloadLen + 2;
   dataOffset -= payloadLen + 2;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wassume"
   ASSUME(slotEndOffset() <= dataOffset);
#pragma clang diagnostic pop
   ASSUME(i < slotCount);
   storeUnaligned<uint16_t>(ptr() + dataOffset, payloadLen);
   memcpy(ptr() + dataOffset + 2, payload, payloadLen);
   slots[i] = dataOffset;
}

bool DenseNode::requestSpaceFor(unsigned int payloadLen)
{
   if (dataOffset - slotEndOffset() >= payloadLen + 2) {
      return true;
   }
   if (slotEndOffset() + spaceUsed + payloadLen + 2 <= fencesOffset()) {
      uint8_t buffer[pageSize];
      unsigned bufferOffset = fencesOffset();
      for (unsigned i = 0; i < slotCount; ++i) {
         if (slots[i]) {
            bufferOffset -= 2 + slotValLen(i);
            memcpy(buffer + bufferOffset, ptr() + slots[i], 2 + slotValLen(i));
            slots[i] = bufferOffset;
         }
      }
      memcpy(ptr() + bufferOffset, buffer + bufferOffset, fencesOffset() - bufferOffset);
      return true;
   }
   return false;
}

unsigned DenseNode::slotEndOffset()
{
   return offsetof(DenseNode, slots) + 2 * slotCount;
}

unsigned DenseNode::slotValLen(unsigned int index)
{
   return loadUnaligned<uint16_t>(ptr() + slots[index]);
}
