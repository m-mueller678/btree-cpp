#pragma once
#include <cstdint>
#include <nmmintrin.h>

typedef uint32_t CrcWordType;

inline uint32_t crc32_hw(uint8_t* data, uint32_t length, uint32_t crc = 0) {
   while (length > 0 && reinterpret_cast<uintptr_t>(data)% alignof(CrcWordType)!=0) {
      crc = _mm_crc32_u8(crc, *data);
      ++data;
      --length;
   }

   while (length >= sizeof(CrcWordType)) {
      crc = _mm_crc32_u32(crc, *reinterpret_cast<CrcWordType*>(data) );
      data += sizeof(CrcWordType);
      length -= sizeof(CrcWordType);
   }

   while (length > 0) {
      crc = _mm_crc32_u8(crc, *data);
      ++data;
      --length;
   }

   return crc;
}
