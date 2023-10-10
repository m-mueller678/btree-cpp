#pragma once
#include <stdlib.h>
#include <cstring>

struct Tuple {
   uint16_t keyLen;
   uint16_t payloadLen;
   uint8_t data[];

   uint8_t* payload() { return data + keyLen; }


   static uintptr_t makeTuple(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      Tuple* tuple = reinterpret_cast<Tuple*>(malloc(sizeof(Tuple) + keyLength + payloadLength));
      tuple->keyLen = keyLength;
      tuple->payloadLen = payloadLength;
      memcpy(tuple->data, key, keyLength);
      memcpy(tuple->data + keyLength, payload, payloadLength);
      return reinterpret_cast<uintptr_t>(tuple);
   }

   static uint8_t* tuplePayloadPtr(uintptr_t tuple)
   {
      return reinterpret_cast<Tuple*>(tuple)->payload();
   }

   static uint8_t* tupleKeyPtr(uintptr_t tuple)
   {
      return reinterpret_cast<Tuple*>(tuple)->data;
   }

   static unsigned tuplePayloadLen(uintptr_t tuple)
   {
      return reinterpret_cast<Tuple*>(tuple)->payloadLen;
   }

   static unsigned tupleKeyLen(uintptr_t tuple)
   {
      return reinterpret_cast<Tuple*>(tuple)->keyLen;
   }
};