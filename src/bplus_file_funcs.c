#include "bplus_file_funcs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define bplus_ERROR -1

#define CALL_BF(call)         \
    {                           \
        BF_ErrorCode code = call; \
        if (code != BF_OK)        \
        {                         \
            BF_PrintError(code);    \
            return bplus_ERROR;     \
        }                         \
    }

const char BF_MAGIC_NUM[4] = { 0x80, 0xAA, 'B', 'P' }; // this identifies the file format

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

    memcpy(header_temp->magic_num, BF_MAGIC_NUM, sizeof(BF_MAGIC_NUM));
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

int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata) {

    BF_Block *header_block;

    // Opening B+_Tree File
    CALL_BF(BF_OpenFile(fileName, file_desc));

    // Receiving data from block 0(where the metadata is stored)
    BF_Block_Init(&header_block);
    CALL_BF(BF_GetBlock(*file_desc, 0, header_block));
    char *header_data = BF_Block_GetData(header_block);

    // checking magic number
    BPlusMeta *temp = malloc(sizeof(BPlusMeta));
    memcpy(temp, header_data, sizeof(BPlusMeta)); // memcpy to avoid alignment issues
    int magic_num_is_valid = (memcmp(temp->magic_num, BF_MAGIC_NUM, sizeof(BF_MAGIC_NUM)) == 0);
    free(temp);
    if (!magic_num_is_valid) return -1;

    // Copying metadata from block 0 to the given pointer
    *metadata = malloc(sizeof(BPlusMeta));
    if (!(*metadata)) {
        return -1;
    }
    memcpy(*metadata, header_data, sizeof(BPlusMeta));
    // The pointer receives a *copy* of the actual metadata, it does not point to block 0 itself
    // Therefore, the metadata in block 0 remains untouched and independent from any other function calls
    // This is done as a security measure against metadata corruption, or errors caused by copying unaligned data
    // This *copy* is always updated when the B+_Tree File is opened with this function, so there are no problems with data consistency

    // Clearing memory and setting dangling pointers to NULL
    CALL_BF(BF_UnpinBlock(header_block));
    BF_Block_Destroy(&header_block);
    header_block = NULL;

    // free(header_data);
    // header_data = NULL;

    return 0;
}

int bplus_close_file(const int file_desc, BPlusMeta *metadata) {

    CALL_BF(BF_CloseFile(file_desc));

    // Since the metadata pointer was used with a *copy* of block 0, which was independent of the Block File Structure,
    // the pointer is freed and set to NULL to avoid issues with memory allocation and pointer dangling
    free(metadata);
    metadata = NULL;

    return 0;
}


int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record) {
    return -1;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record **out_record) {
    *out_record = NULL;
    return -1;
}
