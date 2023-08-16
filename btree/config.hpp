#pragma once

#ifndef NAMED_CONFIG
constexpr const char* configName = "none";
constexpr bool enablePrefix = true;
constexpr bool enableBasicHead = true;
constexpr bool enableDense = false;
constexpr bool enableHash = false;
constexpr bool enableHeadNode = false;
constexpr unsigned basicHintCount = 16;
#endif

typedef uint32_t HashSimdBitMask;  // maximum page size (in bytes) is 65536
constexpr unsigned pageSize = 4096;

constexpr unsigned headNode4HintCount = 16;
constexpr unsigned headNode8HintCount = 16;
constexpr bool hashUseCrc32 = false;
constexpr bool hashUseSimd = true;
constexpr unsigned hashSimdWidth = sizeof(HashSimdBitMask) * 8;

#define S(x) {"const_" #x, x},
static const std::vector<std::pair<const char*, unsigned>> btree_constexpr_settings{S(pageSize) S(enablePrefix) S(enableBasicHead) S(enableDense) S(
    enableHash) S(enableHeadNode) S(basicHintCount) S(headNode4HintCount) S(headNode8HintCount) S(hashUseSimd) S(hashUseCrc32) S(hashSimdWidth)};
#undef S
