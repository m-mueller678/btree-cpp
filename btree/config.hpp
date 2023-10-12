#pragma once

#ifndef NAMED_CONFIG
#define USE_STRUCTURE_BTREE
constexpr const char* configName = "none";
constexpr bool enablePrefix = true;
constexpr bool enableBasicHead = true;
constexpr bool enableDense = true;
constexpr bool enableHash = false;
constexpr bool enableHeadNode = false;
constexpr unsigned basicHintCount = 16;
constexpr bool enableDense2 = false;
constexpr bool enableHashAdapt = false;
#else
constexpr const char* configName = NAMED_CONFIG;
#endif

typedef uint32_t HashSimdBitMask;  // maximum page size (in bytes) is 65536

#ifdef PS_L
constexpr unsigned pageSizeLeaf = PS_L;
#else
constexpr unsigned pageSizeLeaf = 4096;
#endif
#ifdef PS_I
constexpr unsigned pageSizeInner = PS_I;
#else
constexpr unsigned pageSizeInner = 4096;
#endif

constexpr unsigned headNode4HintCount = 16;
constexpr unsigned headNode8HintCount = 16;
constexpr bool hashUseCrc32 = false;
constexpr bool hashUseSimd = true;
constexpr unsigned hashSimdWidth = sizeof(HashSimdBitMask) * 8;
constexpr unsigned hashSortUseStdMerge = true;

#define S(x) {"const_" #x, x},
static const std::vector<std::pair<const char*, unsigned>> btree_constexpr_settings{
    S(pageSizeInner) S(pageSizeLeaf) S(enablePrefix) S(enableBasicHead) S(enableDense) S(enableHash) S(enableHashAdapt) S(enableDense2) S(enableHeadNode) S(basicHintCount) S(headNode4HintCount)
        S(headNode8HintCount) S(hashUseSimd) S(hashUseCrc32) S(hashSimdWidth) S(hashSortUseStdMerge)};
#undef S
