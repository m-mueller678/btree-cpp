#include "btree2020.hpp"

void HeadNode::destroy()
{
   abort();
}

void byteswap(uint32_t* x)
{
   *x = __builtin_bswap32(*x)
}

void byteswap(uint64_t* x)
{
   *x = __builtin_bswap64(*x)
}

template <T>
bool HeadNode::makeSepHead<T>(uint8_t* key, unsigned keyLen, T* out)
{
   uint8_t* bytes = reinterpret_cast<uint8_t*>(out);
   memset(bytes, 0, sizeof(T));
   if (keyLen < sizeof(T)) {
      memcpy(bytes, key, keyLen);
      bytes[sizeof(T) - 1] = keyLen;
      byteswap(out) return true
   } else {
      return false
   }
}

fn make_fence_head(key : PrefixTruncatedKey)->Option<Self>
{
   let mut ret = T::zeroed();
   let bytes = bytes_of_mut(&mut ret);
   debug_assert !(!key .0.is_empty());
   let(len, data_area) = bytes.split_last_mut().unwrap();
   if
      key .0.len() <= data_area.len()
      {
         data_area[..key .0.len()].copy_from_slice(key .0);
         *len = key .0.len() as u8;
         Some(ExplicitLengthHead(ret.swap_big_native_endian()))
      }
   else {
      None
   }
}

fn make_needle_head(key : PrefixTruncatedKey)->Self
{
   let mut ret = T::zeroed();
   let bytes = bytes_of_mut(&mut ret);
   let(len, data_area) = bytes.split_last_mut().unwrap();
   if
      key .0.len() <= data_area.len()
      {
         data_area[..key .0.len()].copy_from_slice(key .0);
         *len = key .0.len() as u8;
      }
   else {
      data_area.copy_from_slice(&key .0 [..data_area.len()]);
      *len = 8;
   }
   ExplicitLengthHead(ret.swap_big_native_endian())
}

void HeadNode::splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);
bool HeadNode::insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child)
{
   ASSUME(count < keyCapacity);

   //     debug_assert!(self.head.key_count < self.head.key_capacity);
   //     if let Some(key) = Head::make_fence_head(key) {
   //         let (head, keys, children, _) = self.as_parts_mut();
   //         keys[..head.key_count as usize + 1]
   //         .copy_within(index..head.key_count as usize, index + 1);
   //         children[..head.key_count as usize + 2]
   //         .copy_within(index..head.key_count as usize + 1, index + 1);
   //         keys[index] = key;
   //         children[index] = child;
   //         head.key_count += 1;
   //         self.update_hint(index);
   //         Ok(())
   //     } else {
   //         let data = NodeDataBuffer::new();
   //         let fences = data.store_fences(self.fences());
   //         self.export(&data, PrefixMode::Keep);
   //         let this = unsafe { reinterpret_mut::<Self, BTreeNode>(self) };
   //         <BasicNode as NodeCreator<true>>::create(
   //             this,
   //             fences,
   //             &data,
   //             0..data.len(),
   //                                                  PrefixMode::Keep,
   //         )?;
   //         let this = unsafe { reinterpret_mut::<BTreeNode, BasicNode>(this) };
   //         Self::try_insert_to_basic(this, index, key, child)
   //     }
}

bool HeadNode::requestSpaceFor(unsigned keyLen);
void HeadNode::getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info);
AnyNode* HeadNode::lookupInner(uint8_t* key, unsigned keyLength);
