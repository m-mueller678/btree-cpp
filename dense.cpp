#include <cstdint>
#include "btree2020.hpp"

typedef uint32_t NumericPart;
constexpr unsigned maxNumericPartLen = sizeof(NumericPart);

typedef uint64_t Mask;
constexpr unsigned maskBytesPerWord = sizeof(Mask);
constexpr unsigned maskBitsPerWord = 8 * maskBytesPerWord;

struct DenseNodeHeader {
   Tag tag;
   uint16_t fullKeyLen;
   NumericPart arrayStart;
   uint16_t valLen;
   uint16_t slotCount;
   uint16_t lowerFenceLen;
   uint16_t upperFenceLen;
   uint16_t prefixLength;
};

enum KeyError : int {
   WrongLen = -1,
   NotNumericRange = -2,
   SlightlyTooLarge = -3,
   FarTooLarge = -4,
};

struct DenseNode : public DenseNodeHeader {
   union {
      Mask mask[(pageSize - sizeof(DenseNodeHeader)) / sizeof(Mask)];
      uint8_t heap[pageSize - sizeof(DenseNodeHeader)];
   };
   unsigned lowerFenceOffset() { return pageSize - lowerFenceLen - max(upperFenceLen, fullKeyLen); }
   uint8_t* getLowerFence() { return ptr() + pageSize - lowerFenceLen; }
   uint8_t* getUpperFence() { return ptr() + lowerFenceOffset(); }

   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }

   uint8_t* getPrefix() { return getLowerFence(); }

   void restoreKey(uint8_t* prefix, uint8_t* dst, unsigned index) { abort(); }

   void changeUpperFence(uint8_t* fence, unsigned len)
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

   void splitNode(BTreeNode* parent, uint8_t* key, unsigned keyLen)
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
               // TODO split to two basic nodes
               abort();
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
      AnyNode* right = this->any();
      if (split_to_self) {
         // TODO create new empty node at right
         //  let new_node = Self::new(
         //                        FenceData {
         //                            lower_fence: FenceRef::from_full(full_boundary, left.prefixLength as usize),
         //                                             upper_fence: left.fences().upper_fence,
         //                                             prefixLength: left.prefixLength as usize,
         //                        }
         //                        .restrip(),
         //                                             left.fullKeyLen as usize,
         //                                             left.valLen as usize,
         //                    );
         //                    right.write_inner(new_node);
         abort();
      } else {
         // TODO check move constructor semantics
         right->_basic_node = BTreeNode{true};
         right->_basic_node.setFences(full_boundary, denseLeft->fullKeyLen, denseLeft->getUpperFence(), denseLeft->upperFenceLen);
      }
      denseLeft->changeUpperFence(full_boundary, fullKeyLen);
   }

   unsigned prefixDiffLen()
   {
      // TODO this gets called a lot?
      return compute_numeric_prefixLength(prefixLength, fullKeyLen) - prefixLength;
   }

   KeyError keyToIndex(uint8_t* truncatedKey, unsigned truncatedLen)
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

   static unsigned compute_numeric_prefixLength(unsigned prefixLength, unsigned fullKeyLen)
   {
      unsigned numericPartLen = min(maxNumericPartLen, fullKeyLen - prefixLength);
      return fullKeyLen - numericPartLen;
   }

   void init(uint8_t* lowerFence,
             unsigned lowerFenceLen,
             uint8_t* upperFence,
             unsigned upperFenceLen,
             unsigned prefixLength,
             unsigned fullKeyLen,
             unsigned valLen)
   {
      tag = Tag::Dense;
      this->fullKeyLen = fullKeyLen;
      this->valLen = valLen;
      this->lowerFenceLen = lowerFenceLen;
      this->upperFenceLen = upperFenceLen;
      this->prefixLength = prefixLength;
      assert(lowerFenceLen <= fullKeyLen);
      assert(compute_numeric_prefixLength(prefixLength, fullKeyLen) <= lowerFenceLen);
      slotCount = computeSlotCount(valLen, lowerFenceOffset());
      zeroMask();
      memcpy(this->getLowerFence(), lowerFence, lowerFenceLen);
      memcpy(this->getUpperFence(), upperFence, upperFenceLen);
      updateArrayStart();
   }

   unsigned mask_word_count() { return (slotCount + maskBitsPerWord - 1) / maskBitsPerWord; }

   void zeroMask()
   {
      unsigned mwc = mask_word_count();
      for (unsigned i = 0; i < mwc; ++i) {
         mask[i] = 0;
      }
   }

   // key is expected to be prefix truncated
   static NumericPart getNumericPart(uint8_t* key, unsigned len)
   {
      switch (len) {
         case 0:
            abort();  // shouldn't happen, likely a bug
         case 1:
            return static_cast<uint32_t>(key[0]);
         case 2:
            return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key)));
         case 3:
            return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 8) | (static_cast<uint32_t>(key[2]) << 8);
         default:
            return __builtin_bswap32(loadUnaligned<uint32_t>(key + len - 4));
      }
   }

   void updateArrayStart()
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

   uint8_t* ptr() { return reinterpret_cast<uint8_t*>(this); }

   static unsigned computeSlotCount(unsigned valLen, unsigned fencesStart)
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

   bool try_densify(BTreeNode* basicNode)
   {
      assert(basicNode->count > 0);
      unsigned pre_key_len_1 = basicNode->slot[0].keyLen;
      unsigned fullKeyLen = pre_key_len_1 + basicNode->prefixLength;
      unsigned numericPrefixLen = compute_numeric_prefixLength(basicNode->prefixLength, fullKeyLen);
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
      init(basicNode->getLowerFence(), basicNode->lowerFence.length, basicNode->getUpperFence(), basicNode->upperFence.length,
           basicNode->prefixLength, fullKeyLen, valLen1);
      int lastKey = keyToIndex(basicNode->getKey(basicNode->count - 1), fullKeyLen - prefixLength);
      assert(lastKey >= 0);
      for (unsigned i = 0; i < basicNode->count; ++i) {
         int index = keyToIndex(basicNode->getKey(i), fullKeyLen - prefixLength);
         assert(index >= 0);
         setSlotPresent(index);
         memcpy(getVal(index), basicNode->getPayload(i), valLen1);
      }
      return true;
   }

   bool isSlotPresent(unsigned i)
   {
      assert(i < slotCount);
      return (mask[i / maskBitsPerWord] >> (i % maskBitsPerWord) & 1) != 0;
   }

   void setSlotPresent(unsigned i)
   {
      assert(i < slotCount);
      mask[i / maskBitsPerWord] |= Mask(1) << (i % maskBitsPerWord);
   }

   void unsetSlotPresent(unsigned i)
   {
      assert(i < slotCount);
      mask[i / maskBitsPerWord] &= ~(Mask(1) << (i % maskBitsPerWord));
   }

   uint8_t* getVal(unsigned i)
   {
      assert(i < slotCount);
      return ptr() + mask_word_count() * maskBytesPerWord + i * valLen;
   }
};
