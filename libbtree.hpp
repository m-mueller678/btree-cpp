#pragma once

#include <cstdint>

struct BTree;
BTree* btree_new();
void btree_insert(BTree* b_tree, std::uint8_t* key, std::uint64_t keyLen, std::uint8_t* payload, std::uint64_t payloadLen);
std::uint8_t* btree_lookup(BTree* b_tree, std::uint8_t* key, std::uint64_t keyLen, std::uint32_t* payloadLenOut);
bool btree_remove(BTree* b_tree, std::uint8_t* key, std::uint64_t keyLen);
void btree_destroy(BTree* b_tree);
void btree_print_info(BTree* b_tree);
void print_tpcc_result(double time_sec, std::uint64_t tx_count, std::uint64_t warehouse_count);

// key_buffer and key must not be null, even if zero length
void btree_scan_asc(BTree* b_tree,
                    std::uint8_t* key,
                    std::uint64_t key_len,
                    std::uint8_t* key_buffer,
                    bool (*continue_callback)(std::uint8_t const*));
void btree_scan_desc(BTree* b_tree,
                     std::uint8_t* key,
                     std::uint64_t key_len,
                     std::uint8_t* key_buffer,
                     bool (*continue_callback)(std::uint8_t const*));
