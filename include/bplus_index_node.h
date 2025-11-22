#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H

#define BLOCK_TYPE_INDEX 1

/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

/* The structure of an index block is the following; for each [][] pair there is no padding between
** (START)[int][IndexNodeHeader][int][IndexNodeEntry][IndexNodeEntry]...[IndexNodeEntry][possibly unused space](END)
** - int (first) is BLOCK_TYPE_DATA for data block, BLOCK_TYPE_INDEX for index block
** - IndexNodeHeader is the index block header
** - int (second) is leftmost index of the block, such that for each key accessible via that index: key < first_entry.key,
**                                                where first_entry is the leftmost (smallest) IndexNodeEntry in the block
** - IndexNodeEntry (0) ... IndexNodeEntry (k) with k < max_indexes_per_block are key-index entries,
**                                             such that for each key accessible via entry.index, key >= entry.key for any entry;
**                                             when a new one is inserted, some others are shifted to maintain ordering
** - possibly unused space is either space not yet used by future entries or a remainder < sizeof(IndexNodeEntry)
*/

typedef struct {
    // index_count is the number of children indexes currently stored
    // number of keys is always index_count - 1
    // number of entries (IndexNodeEntry) is also index_count - 1 (leftmost index is not an "entry")
    int index_count;
    int parent_index; // index of the parent block (index node)
    int min_record_key; // the minimum key accessible via the leftmost index; useful in insertion
} IndexNodeHeader;

typedef struct {
    // a key such that current_entry.right_index keys >= key and prev_entry.right_index keys < key,
    // where prev_entry is any IndexNodeEntry at the left of current_entry;
    // the leftmost index of an index node is defined separately of this struct
    int key;
    int right_index;
} IndexNodeEntry;

// returns 1 if this is an index block, 0 otherwise
int is_index_block(char *block_start);

// prints an index block (requires pointer to block data)
void index_block_print(char *block_start, BPlusMeta* metadata);

// returns the index node header of a block
// returns NULL if unsuccessful
// caller is responsible for freeing the returned memory
IndexNodeHeader *index_block_get_header(char *block_start);

// returns the leftmost index of a block
int index_block_get_leftmost_index(char *block_start);

// returns the entry at index (entries sorted by key)
// returns NULL if index >= current entry count or if unsuccessful
// caller is responsible for freeing the returned memory
IndexNodeEntry *index_block_get_entry(char *block_start, IndexNodeHeader *block_header, int index);

// returns 1 if at least one more entry can be inserted, 0 otherwise
int index_block_has_available_space(IndexNodeHeader *block_header, BPlusMeta *metadata);

#endif