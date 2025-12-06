//
// Created by theofilos on 11/4/25.
//

#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H

// needed for bplus_datanode.c and bplus_index_node.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "record.h"

/* The structure of a B+ Tree file is the following:
** (START)[Block0][Block1][Block2]...[BlockN](END) where N is block_count - 1
** - Block0 always contains the BPlusMeta
** - The next blocks can be either data blocks or index blocks and are always appended at the end of the file
** The position (0-based) of each block in the file is defined as its index. Each block stores indexes that 
** act as pointers to other blocks, and these connections shape the B+ Tree. The index of the root can be found in BPlusMeta.
** - When a block has no children or parent, the related indexes are defined to be -1. This is equivalent to NULL pointers.
** - When the B+ tree gets its first record, the root is the data block itself that contains this record. Only **after** this very
** first block has not enough space, does an index block appear as its parent and new root
*/

typedef struct {
    char magic_num[4]; // identifies the file format
    int block_count; // total number of blocks in the file
    int record_count; // total number of records in the file
    int max_records_per_block; // maximum number of records in a data block
    int max_indexes_per_block; // maximum number of indexes in an index block to its children
    int root_index; // index of the B+ root (index block)
    TableSchema schema; // info for the stored schema (includes record size)
} BPlusMeta;

#endif // BPLUS_BPLUS_FILE_STRUCTS_H
