
/*
unsigned countInner(BTreeNode* node)
{
   if (node->isLeaf)
      return 0;
   unsigned sum = 1;
   for (unsigned i = 0; i < node->count; i++)
      sum += countInner(node->getChild(i));
   sum += countInner(node->upper);
   return sum;
}

unsigned countPages(BTreeNode* node)
{
   if (node->isLeaf)
      return 1;
   unsigned sum = 1;
   for (unsigned i = 0; i < node->count; i++)
      sum += countPages(node->getChild(i));
   sum += countPages(node->upper);
   return sum;
}

uint64_t bytesFree(BTreeNode* node)
{
   if (node->isLeaf)
      return node->freeSpaceAfterCompaction();
   uint64_t sum = node->freeSpaceAfterCompaction();
   for (unsigned i = 0; i < node->count; i++)
      sum += bytesFree(node->getChild(i));
   sum += bytesFree(node->upper);
   return sum;
}

unsigned height(BTreeNode* node)
{
   if (node->isLeaf)
      return 1;
   return 1 + height(node->upper);
}

void printInfos(BTreeNode* root)
{
   uint64_t cnt = countPages(root);
   uint64_t bytesFr = bytesFree(root);
   cerr << "nodes:" << cnt << " innerNodes:" << countInner(root) << " height:" << height(root) << " rootCnt:" << root->count
        << " bytesFree:" << bytesFr << " fillfactor:" << (1 - (bytesFr / ((double)cnt * pageSize))) << endl;
}

void pr(uint8_t* s, unsigned len) {
   for (unsigned i=0; i<len; i++)
      cout << s[i];
}

void printTree(BTreeNode* node) {
   cout << (node->isLeaf?"L":"I") << " " << node << endl;

   if (node->isLeaf) {
      for (unsigned i=0; i<node->count; i++) {
         cout << i << " " << node->getKey(i) << " ";
         pr(node->getPrefix(), node->prefixLength);
         cout << " ";
         pr(node->getKey(i), node->getKeyLen(i));
         cout << endl;
      }
      cout << endl;
      return;
   }

   for (unsigned i=0; i<node->count; i++) {
         pr(node->getPrefix(), node->prefixLength);
         cout << " ";
         pr(node->getKey(i), node->getKeyLen(i));
         cout << " " << node->getChild(i);
         cout << endl;
   }
   cout << " " << node->upper;

   for (unsigned i=0; i<node->count; i++)
      printTree(node->getChild(i));
   printTree(node->upper);
}
*/
