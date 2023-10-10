#pragma once
#include <stdlib.h>
#include <cstring>

constexpr bool ZERO_TERMINATE = false;

struct Tuple {
   uint16_t keyLen;
   uint16_t payloadLen;
   uint8_t data[];

   uint8_t* payload() { return data + keyLen + ZERO_TERMINATE; }

   static uintptr_t makeTuple(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      Tuple* tuple = reinterpret_cast<Tuple*>(malloc(sizeof(Tuple) + keyLength + ZERO_TERMINATE + payloadLength));
      tuple->keyLen = keyLength;
      tuple->payloadLen = payloadLength;
      memcpy(tuple->data, key, keyLength);
      if (ZERO_TERMINATE)
         tuple->data[keyLength] = 0;
      memcpy(tuple->payload(), payload, payloadLength);
      return reinterpret_cast<uintptr_t>(tuple);
   }

   static uint8_t* tuplePayloadPtr(uintptr_t tuple) { return reinterpret_cast<Tuple*>(tuple)->payload(); }

   static uint8_t* tupleKeyPtr(uintptr_t tuple) { return reinterpret_cast<Tuple*>(tuple)->data; }

   static unsigned tuplePayloadLen(uintptr_t tuple) { return reinterpret_cast<Tuple*>(tuple)->payloadLen; }

   static unsigned tupleKeyLen(uintptr_t tuple) { return reinterpret_cast<Tuple*>(tuple)->keyLen; }
};
