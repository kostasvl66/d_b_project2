#ifndef BP_DATANODE_H
#define BP_DATANODE_H

#define BLOCK_TYPE_DATA 0
#define BLOCK_TYPE_INDEX 1

/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

/* The structure of a data block is the following; for each [][] pair there is no padding between
** (START)[int][DataNodeHeader][int[max_records_per_block]][Record][Record]...[Record][possibly unused space](END)
** - int is BLOCK_TYPE_DATA for data block, BLOCK_TYPE_INDEX for index block
** - DataNodeHeader is the data block header
** - int[max_records_per_block] is an array of indexes to records, remains sorted so that the records themselves need not be sorted;
**                              Only the first n values are valid, if n is the current number of records in the block
** - Record (0) ... Record (k) with k < max_records_per_block are record data, each new one appended at the end;
**                             because of that, this "heap" part is unsorted; their sorted order is defined using
**                             the index array, which is always updated as needed
** - possibly unused space is either space not yet used by future records or a remainder < sizeof(Record)
*/

typedef struct {
    int record_count; // number of records currently stored in the data blocks
    int parent_index; // index of the parent block (index node)
    int next_index; // index to the adjacent (to the right) data node
} DataNodeHeader;

// returns 1 if this is a data block, 0 otherwise
int is_data_block(char *block_start);

// prints a data block (requires pointer to block data)
void data_block_print(char *block_start, BPlusMeta *metadata);

// returns pointer to the first byte of DataNodeHeader
char *data_block_get_header(char *block_start);

// returns pointer to the first byte of the index array
char *data_block_get_index_array(char *block_start);

// returns pointer to the first byte of the record at index, where index refers to the unsorted "heap" of records
// returns NULL if index >= current record count
char *data_block_get_unordered_record(char *block_start, BPlusMeta *metadata, int index);

// returns pointer to the first byte of the record at index, where index i refers to the i-th smallest record (sorted)
// returns NULL if index >= current record count
char *data_block_get_record(char *block_start, BPlusMeta *metadata, int index);

// returns 1 if at least one more record can be inserted, 0 otherwise
int data_block_has_available_space(char *block_start, BPlusMeta *metadata);

#endif