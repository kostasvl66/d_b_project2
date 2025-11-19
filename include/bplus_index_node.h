#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

typedef struct {
    // number of children indexes currently stored
    // number of keys is always current_index_count - 1
    int index_count;
    int parent_index; // index of the parent block (index node)
    int minRecordKey; // the minimum key accessible via the leftmost index; useful in insertion
} IndexNodeHeader;

typedef struct {
    // a key such that currentEntry.right_index keys >= key and prevEntry.right_index keys < key,
    // where prevEntry is any IndexNodeEntry at the left of currentEntry;
    // the leftmost index of an index node is defined separately of this struct
    int key;
    int right_index;
} IndexNodeEntry;

#endif