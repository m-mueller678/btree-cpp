#include "lits_adapter.hpp"
#include "../in-memory-structures/lits/lits/lits.hpp"
#include "tuple.hpp"

LitsBTreeAdapter::LitsBTreeAdapter(bool isInt){
   lits = new lits::LITS();
}

void LitsBTreeAdapter::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   uintptr_t tuple = Tuple::makeTuple(key, keyLength+1, payload, payloadLength);
   Tuple::tupleKeyPtr(tuple)[keyLength]=0;
   lits->upsert((char*)Tuple::tupleKeyPtr(tuple),tuple);
}

uint8_t* LitsBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut)
{
   lits::kv* found = lits->lookup((char*)key);
   if(!found)
      return nullptr;
   auto tuple= found->v;
   payloadSizeOut = Tuple::tuplePayloadLen(tuple);
   return Tuple::tuplePayloadPtr(tuple);
}

bool LitsBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const
{
   abort();
}

void LitsBTreeAdapter::range_lookupImpl(uint8_t* key, unsigned int keyLen, uint8_t* keyOut, const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   auto it = lits->find((const char *)(key));
   while (it.not_finish()) {
      Tuple* tuple= reinterpret_cast<Tuple*>(it.getKV()->v);
      memcpy(keyOut, tuple->data, tuple->keyLen-1);
      if (!found_record_cb(tuple->keyLen-1, tuple->payload(), tuple->payloadLen)) {
         break;
      }
      it.next();
   }
}

void LitsBTreeAdapter::range_lookup_descImpl(uint8_t* key, unsigned int keyLen, uint8_t* keyOut, const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}

void LitsBTreeAdapter::bulkInsert(std::vector<std::string>& keys)
{
   auto insertKeyCount = keys.size()/2;
   if(insertKeyCount<1000){
      if (keys.size() < 1000)
         abort();
      else
         insertKeyCount = 1000;
   }
   std::vector<char*> insertKeys;
   std::vector<uint64_t> insertVals;
   for(int i=0;i<insertKeyCount;++i){
      insertKeys.push_back(keys[i].data());
      insertVals.push_back(1);
   }
   std::sort(insertKeys.begin(), insertKeys.end(), [](char* a, char* b) {
      return lits::ustrcmp(a, b) < 0;
   });
   bool success = lits->bulkload((const char**)insertKeys.data(),insertVals.data(),insertKeyCount);
   if(!success)
      abort();
}