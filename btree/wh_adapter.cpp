#include "wh_adapter.hpp"
#include "../in-memory-structures/wormhole/lib.h"
#include "../in-memory-structures/wormhole/kv.h"
#include "../in-memory-structures/wormhole/wh.h"
#include "tuple.hpp"
//
//struct kv* init_tmp_kv(){
//    struct kv* r= malloc(sizeof(struct kv)+1024);
//    r->klen=512;
//    r->vlen=512;
//    return r;
//}


static struct kv* kv_out;

// mm {{{
// copy-out
struct kv *
kvmap_mm_out_peek(struct kv * const kv, struct kv * const out)
{
   kv_out=kv;
   return kv;
}

// copy-out
struct kv *
kvmap_mm_out_dup(struct kv * const kv, struct kv * const out)
{
   return kv_dup2(kv, out);
}

void
kvmap_mm_free_free(struct kv * const kv, void * const priv)
{
   (void)priv;
   free(kv);
}

const struct kvmap_mm kvmap_mm_btree = {
    .in = kvmap_mm_in_dup,
    .out = kvmap_mm_out_peek,
    .free = kvmap_mm_free_free,
    .priv = NULL,
};

WhBTreeAdapter::WhBTreeAdapter(bool isInt){
   wh = whunsafe_create(&kvmap_mm_btree);
}

uint8_t* WhBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut){
   struct kref kref;
   kref_ref_hash32(&kref, key, keyLength);
   if (whunsafe_get(wh,&kref,nullptr) == nullptr)
      return nullptr;
   payloadSizeOut = kv_out->vlen;
   return kv_out->kv+kv_out->klen;
}

void WhBTreeAdapter::insertImpl(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength){
   struct kv * const newkv = kv_create(key, keyLength, payload, payloadLength);
   whunsafe_put(wh,newkv);
}

bool WhBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const{abort();}
void WhBTreeAdapter::range_lookupImpl(uint8_t* key,
                      unsigned int keyLen,
                      uint8_t* keyOut,
                      const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb){abort();}
void WhBTreeAdapter::range_lookup_descImpl(uint8_t* key,
                           unsigned int keyLen,
                           uint8_t* keyOut,
                           const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb){abort();}