#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H

#define BLOCK_TYPE_INDEX 1
#define INDEX_BLOCK_SEARCH_ERROR -2 // this refers to runtime errors (malloc etc), not logical edge cases; should be less than -1

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
    int min_record_key; // minimum key accessible via the leftmost index; useful in insertion
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

// sets the block type to index block
void set_index_block(char *block_start);

// prints an index block (requires pointer to block data)
void index_block_print(char *block_start, BPlusMeta* metadata);

// returns the index node header of a block
// returns NULL if unsuccessful
// caller is responsible for freeing the returned memory
IndexNodeHeader *index_block_read_header(char *block_start);

// returns the leftmost index of a block
int index_block_read_leftmost_index(char *block_start);

// returns the entry at index (entries sorted by key)
// returns NULL if index >= current entry count or if unsuccessful
// caller is responsible for freeing the returned memory
IndexNodeEntry *index_block_read_entry(char *block_start, IndexNodeHeader *block_header, int index);

// fills an allocated buffer entry_array with all entries of the block, **including leftmost index** as an entry,
// with the appropriate value as the minimum key
// the count of copied entries is the current count of indexes (that is current entry count + 1)
// entry_array buffer is assumed to be large enough to fit the entries; if not, this is undefined behavior
void index_block_read_entries_as_array(char *block_start, IndexNodeHeader *block_header, IndexNodeEntry *entry_array);

// returns 1 if at least one more entry can be inserted, 0 otherwise
int index_block_has_available_space(IndexNodeHeader *block_header, BPlusMeta *metadata);

// writes header in the IndexNodeHeader part of the block
void index_block_write_header(char *block_start, IndexNodeHeader *header);

// writes leftmost_index in the leftmost index part of the block
void index_block_write_leftmost_index(char *block_start, int leftmost_index);

// writes the first count entries of entry_array to the block's entries, **including leftmost index** which
// is assigned the index of entry_array[0]; also the key of entry_array[0] becomes the block's minimum key
// count is assumed to not exceed max entry count; else, this is undefined behavior
// if count < 1 it does nothing
void index_block_write_array_as_entries(char *block_start, IndexNodeHeader *block_header, IndexNodeEntry *entry_array, int count);

// returns the (0-based) position of the entry (excluding leftmost index), where a new entry with new_key as key can be inserted
// new entries can never replace the leftmost index of an index block, so the leftmost index is excluded from the search
// returns -1 if the specified key already exists in the index block
// returns INDEX_BLOCK_SEARCH_ERROR if unsuccessful
int index_block_search_insert_pos(char *block_start, IndexNodeHeader *block_header, int new_key);

// returns the (0-based) entry position, where that entry can lead to the specified key via the entry's right_index
// returns -1 if the specified key can be found via the leftmost index of the block
// returns INDEX_BLOCK_SEARCH_ERROR if unsuccessful
int index_block_key_search(char *block_start, IndexNodeHeader *block_header, int key);

#endif