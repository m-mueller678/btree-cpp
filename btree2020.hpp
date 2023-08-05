#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include "common.hpp"

struct BTreeNode;

struct BTreeNodeHeader {
   static const unsigned underFullSize = pageSize / 4;  // merge nodes below this size

   struct FenceKeySlot {
      uint16_t offset;
      uint16_t length;
   };

   BTreeNode* upper = nullptr;  // only used in inner nodes

   FenceKeySlot lowerFence = {0, 0};  // exclusive
   FenceKeySlot upperFence = {0, 0};  // inclusive

   uint16_t count = 0;
   bool isLeaf;
   uint16_t spaceUsed = 0;
   uint16_t dataOffset = static_cast<uint16_t>(pageSize);
   uint16_t prefixLength = 0;

   static const unsigned hintCount = 16;
   uint32_t hint[hintCount];
   uint32_t padding;

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}
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
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   uint8_t* getPrefix();

   unsigned freeSpace();
   unsigned freeSpaceAfterCompaction();

   bool requestSpaceFor(unsigned spaceNeeded);

   static BTreeNode* makeLeaf();
   static BTreeNode* makeInner();

   uint8_t* getKey(unsigned slotId);
   uint8_t* getPayload(unsigned slotId);

   BTreeNode* getChild(unsigned slotId);

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

   BTreeNode* lookupInner(uint8_t* key, unsigned keyLength);

   void destroy();
};

struct BTree {
  private:
   BTreeNode* root;

   void splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength, unsigned payloadLength);
   void ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength, unsigned payloadLength);

  public:
   BTree();
   ~BTree();

   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   bool lookup(uint8_t* key, unsigned keyLength);
   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);
   bool remove(uint8_t* key, unsigned keyLength);
};
