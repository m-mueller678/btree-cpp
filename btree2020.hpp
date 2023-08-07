#pragma once
#pragma clang diagnostic ignored "-Wvla-extension"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

// maximum page size (in bytes) is 65536
constexpr unsigned pageSize = 4096;
constexpr bool enableDense = true;

inline unsigned min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

inline unsigned max(unsigned a, unsigned b)
{
   return a < b ? b : a;
}

#define COUNTER(name, val, freq)                                                                                         \
   {                                                                                                                     \
      static uint64_t name##_count = 0;                                                                                  \
      static uint64_t name##_sum = 0;                                                                                    \
      name##_count += 1;                                                                                                 \
      name##_sum += (val);                                                                                               \
      if (name##_count % (freq) == 0) {                                                                                  \
         printf("counter %20s| avg %.3e | sum %.3e | cnt %.3e\n", #name, static_cast<double>(name##_sum) / name##_count, \
                static_cast<double>(name##_sum), static_cast<double>(name##_count));                                     \
      }                                                                                                                  \
   }

template <class T>
static T loadUnaligned(void* p)
{
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

struct BTreeNode;
union AnyNode;

enum class Tag : uint8_t {
   Leaf = 0,
   Inner = 1,
   Dense = 2,
};

struct BTreeNodeHeader {
   Tag tag;

   static const unsigned underFullSize = pageSize / 4;  // merge nodes below this size

   struct FenceKeySlot {
      uint16_t offset;
      uint16_t length;
   };

   AnyNode* upper = nullptr;  // only used in inner nodes

   FenceKeySlot lowerFence = {0, 0};  // exclusive
   FenceKeySlot upperFence = {0, 0};  // inclusive

   uint16_t count = 0;
   uint16_t spaceUsed = 0;
   uint16_t dataOffset = static_cast<uint16_t>(pageSize);
   uint16_t prefixLength = 0;

   static const unsigned hintCount = 16;
   uint32_t hint[hintCount];
   uint32_t padding;

   BTreeNodeHeader(bool isLeaf) : tag(isLeaf ? Tag::Leaf : Tag::Inner) {}
};

struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      uint16_t offset;
      uint16_t keyLen;
      uint16_t payloadLen;
      union {
         uint32_t head;
         uint8_t headBytes[4];
      };
   } __attribute__((packed));
   union {
      Slot slot[(pageSize - sizeof(BTreeNodeHeader)) / sizeof(Slot)];  // grows from front
      uint8_t heap[pageSize - sizeof(BTreeNodeHeader)];                // grows from back
   };

   static constexpr unsigned maxKVSize = ((pageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

   BTreeNode(bool isLeaf);

   uint8_t* ptr();
   bool isInner();
   bool isLeaf();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   uint8_t* getPrefix();

   unsigned freeSpace();
   unsigned freeSpaceAfterCompaction();

   bool requestSpaceFor(unsigned spaceNeeded);

   static AnyNode* makeLeaf();
   static AnyNode* makeInner();

   uint8_t* getKey(unsigned slotId);
   uint8_t* getPayload(unsigned slotId);

   AnyNode* getChild(unsigned slotId);

   // How much space would inserting a new key of length "keyLength" require?
   unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength);

   void makeHint();

   void updateHint(unsigned slotId);

   void searchHint(uint32_t keyHead, unsigned& lowerOut, unsigned& upperOut);

   // lower bound search, foundOut indicates if there is an exact match, returns slotId
   unsigned lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut);

   // lowerBound wrapper ignoring exact match argument (for convenience)
   unsigned lowerBound(uint8_t* key, unsigned keyLength);

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   bool removeSlot(unsigned slotId);

   bool remove(uint8_t* key, unsigned keyLength);

   void compactify();

   // merge "this" into "right" via "tmp"
   bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right);

   // store key/value pair at slotId
   void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount);

   void copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot);

   void insertFence(FenceKeySlot& fk, uint8_t* key, unsigned keyLength);

   void setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen);

   void splitNode(BTreeNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);

   struct SeparatorInfo {
      unsigned length;   // length of new separator
      unsigned slot;     // slot at which we split
      bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
   };

   unsigned commonPrefix(unsigned slotA, unsigned slotB);

   SeparatorInfo findSeparator();

   void getSep(uint8_t* sepKeyOut, SeparatorInfo info);

   AnyNode* lookupInner(uint8_t* key, unsigned keyLength);

   void destroy();

   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }
};

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
   uint16_t _pad[3];
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
   unsigned lowerFenceOffset();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();

   AnyNode* any();

   uint8_t* getPrefix();

   void restoreKey(uint8_t* prefix, uint8_t* dst, unsigned index);

   void changeUpperFence(uint8_t* fence, unsigned len);

   void copyKeyValueRangeToBasic(BTreeNode* dst, unsigned srcStart, unsigned srcEnd);

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void splitNode(BTreeNode* parent, uint8_t* key, unsigned keyLen);

   unsigned prefixDiffLen();
   KeyError keyToIndex(uint8_t* truncatedKey, unsigned truncatedLen);

   static unsigned computeNumericPartLen(unsigned prefixLength, unsigned fullKeyLen);
   static unsigned computeNumericPrefixLength(unsigned prefixLength, unsigned fullKeyLen);

   void init(uint8_t* lowerFence,
             unsigned lowerFenceLen,
             uint8_t* upperFence,
             unsigned upperFenceLen,
             unsigned prefixLength,
             unsigned fullKeyLen,
             unsigned valLen);

   unsigned mask_word_count();

   void zeroMask();

   // key is expected to be prefix truncated
   static NumericPart getNumericPart(uint8_t* key, unsigned len);

   void updateArrayStart();

   uint8_t* ptr();

   static unsigned computeSlotCount(unsigned valLen, unsigned fencesStart);

   bool try_densify(BTreeNode* basicNode);

   bool isSlotPresent(unsigned i);

   void setSlotPresent(unsigned i);

   void unsetSlotPresent(unsigned i);

   uint8_t* getVal(unsigned i);
};

union AnyNode {
   Tag tag;
   BTreeNode _basic_node;

   AnyNode(BTreeNode basic) : _basic_node(basic) {}
   AnyNode() {}

   void destroy()
   {
      switch (tag) {
         case Tag::Leaf:
         case Tag::Inner:
            return basic()->destroy();
         default:
            abort();
      }
   }

   void dealloc() { delete this; }

   bool isAnyInner()
   {
      switch (tag) {
         case Tag::Leaf:
            return false;
         case Tag::Inner:
            return true;
         default:
            abort();
      }
   }

   BTreeNode* basic()
   {
      assert(tag == Tag::Leaf || tag == Tag::Inner);
      return reinterpret_cast<BTreeNode*>(this);
   }

   DenseNode* dense()
   {
      assert(tag == Tag::Dense);
      return reinterpret_cast<DenseNode*>(this);
   }
};

struct BTree {
  private:
   AnyNode* root;

   void splitNode(AnyNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength);
   void ensureSpace(AnyNode* toSplit, uint8_t* key, unsigned keyLength);

  public:
   BTree();
   ~BTree();

   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   bool lookup(uint8_t* key, unsigned keyLength);
   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);
   bool remove(uint8_t* key, unsigned keyLength);
};
