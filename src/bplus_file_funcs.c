#include "bplus_file_funcs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return bplus_ERROR;     \
    }                         \
  }

#define bplus_ERROR -1

const char BP_MAGIC_NUM[4] = { 0x80, 0xAA, 'B', 'P' };

int bplus_create_file(const TableSchema *schema, const char *fileName)
{
    BF_Block *header_block; // block 0 that contains the file header
    BF_Block_Init(&header_block);

    // creating the file
    CALL_BF(BF_CreateFile(fileName));

    // opening to initialize the file
    int file_handle;
    CALL_BF(BF_OpenFile(fileName, &file_handle));

    // allocating block 0 and writing the header
    CALL_BF(BF_AllocateBlock(file_handle, header_block));

    BPlusMeta *header_temp = malloc(sizeof(BPlusMeta));
    if (!header_temp) return -1;

    memcpy(header_temp->magic_num, BP_MAGIC_NUM, sizeof(BP_MAGIC_NUM));
    header_temp->block_count = 1; // including header_block
    header_temp->record_count = 0;
    memcpy(&(header_temp->schema), schema, sizeof(TableSchema));
    //header_temp->max_records_per_block = (int)((BF_BLOCK_SIZE - METADATA_SIZE) / header_temp->schema.record_size); // uncomment later

    memcpy(BF_Block_GetData(header_block), header_temp, sizeof(BPlusMeta)); // memcpy to avoid unaligned address problems
    free(header_temp);

    /* setting the block dirty, unpinning and destroying */
    BF_Block_SetDirty(header_block);
    CALL_BF(BF_UnpinBlock(header_block));

    /* closing the file */
    BF_CloseFile(file_handle);

    /* memory cleanup */
    BF_Block_Destroy(&header_block);

    return 0;
}

int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
    return -1;
}

int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
    return -1;
}

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{
    return -1;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  
  *out_record=NULL;
    return -1;
}

