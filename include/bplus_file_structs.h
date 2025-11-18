//
// Created by theofilos on 11/4/25.
//

#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H
#include "bf.h"
#include "bplus_datanode.h"
#include "bplus_file_structs.h"
#include "bplus_index_node.h"
#include "record.h"

typedef struct {
    char magic_num[4]; // identifies the file format
    int block_count; // total number of blocks in the file
    int record_count; // total number of records in the file
    int max_records_per_block; // maximum number of records in a data block
    TableSchema schema; // info for the stored schema (includes record size)
} BPlusMeta;

#endif // BPLUS_BPLUS_FILE_STRUCTS_H
