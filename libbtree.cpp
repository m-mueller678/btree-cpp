#include "libbtree.hpp"
#include "btree2020.hpp"

BTree* btree_new()
{
   return new BTree();
}

void btree_insert(BTree* b_tree, std::uint8_t* key, std::uint64_t keyLen, std::uint8_t* payload, std::uint64_t payloadLen)
{
   b_tree->insert(key, keyLen, payload, payloadLen);
}
std::uint8_t* btree_lookup(BTree* b_tree, std::uint8_t* key, std::uint64_t keyLen, std::uint32_t* payloadLenOut)
{
   return b_tree->lookup(key, keyLen, *payloadLenOut);
}

bool btree_remove(BTree* b_tree, std::uint8_t* key, std::uint64_t keyLen)
{
   return b_tree->remove(key, keyLen);
}

void btree_destroy(BTree* b_tree)
{
   delete b_tree;
}

void print_tpcc_result(double time_sec, std::uint64_t tx_count, std::uint64_t warehouse_count)
{
   printf("count: %lu\n", tx_count);
}

// key_buffer and key must not be null, even if zero length
void btree_scan_asc(BTree* b_tree, std::uint8_t* key, std::uint64_t key_len, std::uint8_t* key_buffer, bool (*continue_callback)(std::uint8_t const*))
{
   b_tree->range_lookup(key, key_len, key_buffer, [=](unsigned keyLen, uint8_t* payload, unsigned payloadLen) { return continue_callback(payload); });
}

void btree_scan_desc(BTree* b_tree,
                     std::uint8_t* key,
                     std::uint64_t key_len,
                     std::uint8_t* key_buffer,
                     bool (*continue_callback)(std::uint8_t const*))
{
   abort();
}
