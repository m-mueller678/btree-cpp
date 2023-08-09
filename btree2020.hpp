#pragma once
#pragma clang diagnostic ignored "-Wvla-extension"

#include <bit>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>

// maximum page size (in bytes) is 65536
constexpr unsigned pageSize = 4096;
constexpr bool enableDense = false;
constexpr bool enableHash = false;
constexpr bool enableHeadNode = true;

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

#define ASSUME(x)          \
   {                       \
      assert(x);           \
      __builtin_assume(x); \
   }

template <class T>
static T loadUnaligned(void* p)
{
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

template <class T>
static void storeUnaligned(void* p, T t)
{
   memcpy(p, &t, sizeof(T));
}

// Get order-preserving head of key (assuming little endian)
inline uint32_t head(uint8_t* key, unsigned keyLength)
{
   switch (keyLength) {
      case 0:
         return 0;
      case 1:
         return static_cast<uint32_t>(key[0]) << 24;
      case 2:
         return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16;
      case 3:
         return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16) | (static_cast<uint32_t>(key[2]) << 8);
      default:
         return __builtin_bswap32(loadUnaligned<uint32_t>(key));
   }
}

template <class T>
inline T byteswap(T x);

template <>
inline uint32_t byteswap<uint32_t>(uint32_t x)
{
   return __builtin_bswap32(x);
}

template <>
inline uint64_t byteswap<uint64_t>(uint64_t x)
{
   return __builtin_bswap64(x);
}

struct BTreeNode;
union AnyNode;

enum class Tag : uint8_t {
   Leaf = 0,
   Inner = 1,
   Dense = 2,
   Hash = 3,
   Head4 = 4,
   Head8 = 5,
};

struct BTreeNodeHeader {
   Tag _tag;

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

   BTreeNodeHeader(bool isLeaf) : _tag(isLeaf ? Tag::Leaf : Tag::Inner) {}
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

   uint8_t* getKey(unsigned slotId);
   uint8_t* getPayload(unsigned slotId);

   AnyNode* getChild(unsigned slotId);

   // How much space would inserting a new key of length "getKeyLength" require?
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

   void splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);

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
   bool is_underfull();
   bool insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child);
   bool range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void validate_child_fences();
   void print();
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

enum AnyKeyRel {
   Before,
   After,
   At,
};

struct AnyKeyIndex {
   unsigned index;
   AnyKeyRel rel;
};

struct DenseNode : public DenseNodeHeader {
   union {
      Mask mask[(pageSize - sizeof(DenseNodeHeader)) / sizeof(Mask)];
      uint8_t heap[pageSize - sizeof(DenseNodeHeader)];
   };
   unsigned fencesOffset();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();

   AnyNode* any();

   uint8_t* getPrefix();

   void restoreKey(uint8_t* prefix, uint8_t* dst, unsigned index);

   void changeUpperFence(uint8_t* fence, unsigned len);

   void copyKeyValueRangeToBasic(BTreeNode* dst, unsigned srcStart, unsigned srcEnd);

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void splitNode(AnyNode* parent, uint8_t* key, unsigned keyLen);

   unsigned prefixDiffLen();
   KeyError keyToIndex(uint8_t* truncatedKey, unsigned truncatedLen);

   static unsigned computeNumericPartLen(unsigned prefixLength, unsigned fullKeyLen);
   static unsigned computeNumericPrefixLength(unsigned prefixLength, unsigned fullKeyLen);

   void init(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen, unsigned fullKeyLen, unsigned valLen);

   unsigned maskWordCount();

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
   uint8_t* lookup(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut);
   void updatePrefixLength();
   bool remove(uint8_t* key, unsigned int keyLength);
   bool is_underfull();
   unsigned int occupiedCount();
   BTreeNode* convertToBasic();
   bool range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   AnyKeyIndex anyKeyIndex(uint8_t* key, unsigned keyLen);
};

struct HashNodeHeader {
   Tag _tag;
   uint16_t count;
   uint16_t sortedCount;
   uint16_t spaceUsed;
   uint16_t dataOffset;
   uint16_t prefixLength;
   uint16_t hashCapacity;
   uint16_t hashOffset;
   uint16_t lowerFenceLen;
   uint16_t upperFenceLen;
};

struct HashSlot {
   uint16_t offset;
   uint16_t keyLen;
   uint16_t payloadLen;
};

struct HashNode : public HashNodeHeader {
   union {
      HashSlot slot[(pageSize - sizeof(HashNodeHeader)) / sizeof(HashSlot)];  // grows from front
      uint8_t heap[pageSize - sizeof(HashNodeHeader)];                        // grows from back
   };

   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   static uint8_t compute_hash(uint8_t* key, unsigned int keyLength);
   uint8_t* ptr();
   uint8_t* hashes();
   uint8_t* getPayload(unsigned int slotId);
   uint8_t* getKey(unsigned int slotId);
   void init(uint8_t* lowerFence, unsigned int lowerFenceLen, uint8_t* upperFence, unsigned int upperFenceLen, unsigned hashCapacity);
   static AnyNode* makeRootLeaf();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   void updatePrefixLength();
   bool insert(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength);
   int findIndex(uint8_t* key, unsigned keyLength);
   unsigned int freeSpace();
   unsigned int freeSpaceAfterCompaction();
   bool requestSpace(unsigned int spaceNeeded);
   bool requestSlotAndSpace(unsigned int spaceNeeded);
   void compactify(unsigned newHashCapacity);
   void sort();
   unsigned int commonPrefix(unsigned int slotA, unsigned int slotB);
   BTreeNode::SeparatorInfo findSeparator();
   void getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info);
   void splitNode(AnyNode* parent, unsigned int sepSlot, uint8_t* sepKey, unsigned int sepLength);
   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }
   void updateHash(unsigned int i);
   void copyKeyValue(unsigned srcSlot, HashNode* dst, unsigned dstSlot);
   void storeKeyValue(unsigned int slotId, uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength);
   void copyKeyValueRange(HashNode* dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);
   bool removeSlot(unsigned int slotId);
   bool remove(uint8_t* key, unsigned int keyLength);
   bool mergeNodes(unsigned int slotId, BTreeNode* parent, HashNode* right);
   void print();
   bool range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   unsigned int lowerBound(uint8_t* key, unsigned int keyLength);
};

struct HeadNodeHead {
   Tag _tag;
   uint16_t count;
   uint16_t keyCapacity;
   uint16_t lowerFenceLen;
   uint16_t upperFenceLen;
   uint16_t prefixLength;
   uint8_t* ptr();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   void updatePrefixLength();
   unsigned int fencesOffset();
   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }
   static bool requestChildConvertFromBasic(BTreeNode* node, unsigned int newKeyLength);
};

template <class T>
struct HeadNode : public HeadNodeHead {
   static constexpr unsigned paddedHeadSize = (sizeof(HeadNodeHead) + alignof(T) - 1) / alignof(T) * alignof(T);
   union {
      T keys[(pageSize - paddedHeadSize) / sizeof(T)];
      uint8_t data[pageSize - paddedHeadSize];
   };

   void destroy();
   void splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);
   bool insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child);
   bool requestSpaceFor(unsigned keyLen);
   void getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info);
   AnyNode* lookupInner(uint8_t* key, unsigned keyLength);
   static bool makeSepHead(uint8_t* key, unsigned int keyLen, T* out);
   static void makeNeedleHead(uint8_t* key, unsigned int keyLen, T* out);
   unsigned int lowerBound(T head, bool& foundOut);
   void insertAt(unsigned int index, T head, AnyNode* child);
   void init(uint8_t* lowerFence, unsigned int lowerFenceLen, uint8_t* upperFence, unsigned int upperFenceLen);
   AnyNode** children();
   void clone_from_basic(BTreeNode* src);
   unsigned getKeyLength(unsigned int i);
   void copyKeyValueRangeToBasic(BTreeNode* dst, unsigned int srcStart, unsigned int srcEnd);
   void copyKeyValueRange(HeadNode<T>* dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);
   bool convertToBasicWithSpace(unsigned int newKeyLen);
   BTreeNode::SeparatorInfo findSeparator();
};

typedef HeadNode<uint32_t> HeadNode4;
typedef HeadNode<uint64_t> HeadNode8;

union AnyNode {
   Tag _tag;
   BTreeNode _basic_node;
   DenseNode _dense;
   HashNode _hash;
   HeadNode4 _head4;
   HeadNode8 _head8;

   Tag tag()
   {
      ASSUME(_tag == Tag::Inner || _tag == Tag::Leaf || _tag == Tag::Dense || _tag == Tag::Hash || _tag == Tag::Head4 || _tag == Tag::Head8);
      ASSUME((enableDense && !enableHash) || _tag != Tag::Dense);
      ASSUME(enableHash || _tag != Tag::Hash);
      ASSUME(!enableHash || _tag != Tag::Leaf);
      ASSUME(enableHeadNode || _tag != Tag::Head4);
      ASSUME(enableHeadNode || _tag != Tag::Head8);
      return _tag;
   }

   AnyNode(BTreeNode basic) : _basic_node(basic) {}
   AnyNode() {}

   void destroy()
   {
      switch (tag()) {
         case Tag::Leaf:
         case Tag::Inner:
            return basic()->destroy();
         case Tag::Dense:
         case Tag::Hash:
            return dealloc();
         case Tag::Head4:
            return head4()->destroy();
         case Tag::Head8:
            return head8()->destroy();
      }
   }

   void dealloc() { delete this; }

   bool isAnyInner()
   {
      switch (tag()) {
         case Tag::Leaf:
         case Tag::Dense:
         case Tag::Hash:
            return false;
         case Tag::Inner:
         case Tag::Head4:
         case Tag::Head8:
            return true;
      }
   }

   BTreeNode* basic()
   {
      ASSUME(_tag == Tag::Leaf || _tag == Tag::Inner);
      return reinterpret_cast<BTreeNode*>(this);
   }

   DenseNode* dense()
   {
      ASSUME(_tag == Tag::Dense);
      return reinterpret_cast<DenseNode*>(this);
   }

   HashNode* hash()
   {
      ASSUME(_tag == Tag::Hash);
      return reinterpret_cast<HashNode*>(this);
   }

   HeadNode<uint32_t>* head4()
   {
      ASSUME(_tag == Tag::Head4);
      return reinterpret_cast<HeadNode<uint32_t>*>(this);
   }

   HeadNode<uint64_t>* head8()
   {
      ASSUME(_tag == Tag::Head8);
      return reinterpret_cast<HeadNode<uint64_t>*>(this);
   }

   bool insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child)
   {
      switch (tag()) {
         case Tag::Inner:
            return basic()->insertChild(key, keyLength, child);
         case Tag::Head4:
            return head4()->insertChild(key, keyLength, child);
         case Tag::Head8:
            return head8()->insertChild(key, keyLength, child);
         case Tag::Leaf:
         case Tag::Hash:
         case Tag::Dense:
            ASSUME(false);
      }
   }

   bool innerRequestSpaceFor(unsigned keyLen)
   {
      switch (tag()) {
         case Tag::Inner: {
            bool succ = basic()->requestSpaceFor(basic()->spaceNeeded(keyLen, sizeof(AnyNode*)));
            if (enableHeadNode && !succ) {
               return HeadNodeHead::requestChildConvertFromBasic(basic(), keyLen);
            }
            return succ;
         }
         case Tag::Head4:
            return head4()->requestSpaceFor(keyLen);
         case Tag::Head8:
            return head8()->requestSpaceFor(keyLen);
         case Tag::Leaf:
         case Tag::Hash:
         case Tag::Dense:
            ASSUME(false);
      }
   }

   AnyNode* lookupInner(uint8_t* key, unsigned keyLength)
   {
      switch (tag()) {
         case Tag::Inner:
            return basic()->lookupInner(key, keyLength);
         case Tag::Head4:
            return head4()->lookupInner(key, keyLength);
         case Tag::Head8:
            return head8()->lookupInner(key, keyLength);
         case Tag::Leaf:
         case Tag::Hash:
         case Tag::Dense:
            ASSUME(false);
      }
   }
   static AnyNode* makeRoot(AnyNode* child);
};

struct BTree {
   AnyNode* root;

   void splitNode(AnyNode* node, AnyNode* parent, uint8_t* key, unsigned keyLength);
   void ensureSpace(AnyNode* toSplit, uint8_t* key, unsigned keyLength);

  public:
   BTree();
   ~BTree();

   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   bool lookup(uint8_t* key, unsigned keyLength);
   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);
   bool remove(uint8_t* key, unsigned keyLength);
   void range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
};
