#ifndef BP_DATANODE_H
#define BP_DATANODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

/* The structure of a data block is the following; for each [][] pair there is no padding between
** (START)[int][DataNodeHeader][int[max_records_per_block]][Record][Record]...[Record][possibly unused space](END)
** - int is 0 for data block, 1 for index block
** - DataNodeHeader is the data block header
** - int[max_records_per_block] is an array of indexes to records, remains sorted so that the records themselves need not be sorted;
**                              Only the first n values are valid, if n is the current number of records in the block
** - Record (0) ... Record (k) with k < max_records_per_block are record data, each new one appended at the end;
**                             their ordering is maintained by the index array, which is always updated
** - possibly unused space is either space not yet used by future records or a remainder < sizeof(Record)
*/

typedef struct {
    int record_count; // number of records currently stored in the data blocks
    int parent_index; // index of the parent block (index node)
    int next_index; // index to the adjacent (to the right) data node
} DataNodeHeader;

#endif