#include <cstdint>
#include "any_node.hpp" 

struct DenseNodeHeader {
    uint8_t tag;
   uint16_t full_key_len;
   uint32_t array_start;
   uint16_t val_len;
   uint16_t slot_count;
   uint16_t lower_fence_len;
   uint16_t upper_fence_len;
   uint16_t prefix_len;
};

static unsigned
min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

static unsigned max(unsigned a, unsigned b)
{
   return a < b ? b : a;
}

constexpr int KEY_ERROR_WRONG_LEN=-1;
constexpr int KEY_ERROR_NOT_NUMERIC_RAMGE=-2;
constexpr int KEY_ERROR_SLIGHTLY_TOO_LARGE=-3;
constexpr int KEY_ERROR_FAR_TOO_LARGE=-4;

struct DenseNode : public DenseNodeHeader {
   union {
      uint64_t mask[(pageSize - sizeof(DenseNodeHeader)) / sizeof(uint64_t)];
      uint8_t heap[pageSize - sizeof(DenseNodeHeader)];
   };
   uint8_t* getLowerFence(){return ptr() + pageSize - lower_fence_len} uint8_t* getUpperFence(){
       return ptr() + pageSize - lower_fence_len - max(upper_fence_len, full_key_len)} uint8_t* getPrefix()
   {
      return getLowerFence()
   }
   
   void restoreKey(uint8_t* prefix,uint8_t * dst,unsigned index){
        abort()
    }
   
   void splitNode(BTreeNode* parent,uint8_t* key,unsigned keyLen){
       int key_index = key_to_index(key);
       bool split_to_self;
       switch (key_index){
           case KEY_ERROR_FAR_TOO_LARGE:
           case KEY_ERROR_NOT_NUMERIC_RANGE:
               split_to_self=false;
               break;
           case KEY_ERROR_WRONG_LEN:
               // splitting into two new basic nodes might be impossible if prefix length is short
               if upper_fence_len < full_key_len{
                   split_to_self=false;
                }else{
                    // TODO split to two basic nodes
                    abort();
                }
                break;
            case  KEY_ERROR_SLIGHTLY_TOO_LARGE:
                split_to_self=true;
                break;
       }
       uint8_t full_boundary[full_key_len];
       restoreKey(key,full_boundary,slot_count -1);
       
       DenseNode* denseLeft=nodeLeft;
       DenseNode* nodeLeft = new AnyNode();
       memcpy(nodeLeft,this,sizeof(DenseNode));
       bool succ = parent->insert(full_boundary, full_key_len, reinterpret_cast<uint8_t*>(&nodeLeft), sizeof(BTreeNode*));
       assert(succ);
       AnyNode* right = this;
       if (split_to_self){
           //TODO create new empty node at right
               //  let new_node = Self::new(
               //                        FenceData {
               //                            lower_fence: FenceRef::from_full(full_boundary, left.prefix_len as usize),
               //                                             upper_fence: left.fences().upper_fence,
               //                                             prefix_len: left.prefix_len as usize,
               //                        }
               //                        .restrip(),
               //                                             left.full_key_len as usize,
               //                                             left.val_len as usize,
               //                    );
               //                    right.write_inner(new_node);
           abort();
        }else{
            *right.basic = BTreeNode{true};
            nodeRight->setFences(full_boundary, denseLeft.full_key_len, denseLeft.upper_fence(),denseLeft.upper_fence_len);
        }
        nodeLeft.change_upper_fence(full_boundary);
}
}
