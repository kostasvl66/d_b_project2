#include "bplus_file_funcs.h"
#include <stdio.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return bplus_ERROR;     \
    }                         \
  }

// int HeapFile_Create(const char *fileName) {

//     BF_Block *header_block;       /* block 0 that contains the file header */
//     BF_Block *first_record_block; /* block 1 that is ready to receive records */
//     BF_Block_Init(&header_block);
//     BF_Block_Init(&first_record_block);

//     /* creating the file */
//     CALL_BF(BF_CreateFile(fileName));

//     /* opening to initialize the file */
//     int file_handle;
//     CALL_BF(BF_OpenFile(fileName, &file_handle));

//     /* allocating blocks 0 and 1 and writing the header */
//     CALL_BF(BF_AllocateBlock(file_handle, header_block));
//     CALL_BF(BF_AllocateBlock(file_handle, first_record_block));

//     HeapFileHeader *header_temp = malloc(sizeof(HeapFileHeader));
//     if (!header_temp)
//         return 0;
//     header_temp->block_count = 2; /* header_block and first_record_block */
//     header_temp->record_count = 0;
//     header_temp->records_per_block = (int)((BF_BLOCK_SIZE - METADATA_SIZE) / sizeof(Record));
//     header_temp->record_size = sizeof(Record);
//     header_temp->last_block_append_offset = 0;

//     memcpy(BF_Block_GetData(header_block), header_temp, sizeof(HeapFileHeader)); /* memcpy to avoid unaligned address problems */
//     free(header_temp);

//     /* initializing the first_record_block with its metadata at the end;
//        the metadata for each block is only an int record_count */
//     char *record_block_start = BF_Block_GetData(first_record_block);

//     int record_count = 0;                                                     /* 0 records initially */
//     memcpy(record_block_start + METADATA_OFFSET, &record_count, sizeof(int)); /* memcpy to avoid unaligned address problems */

//     /* setting the blocks dirty, unpinning and destroying */
//     BF_Block_SetDirty(header_block);
//     CALL_BF(BF_UnpinBlock(header_block));

//     BF_Block_SetDirty(first_record_block);
//     CALL_BF(BF_UnpinBlock(first_record_block));

//     /* closing the file */
//     BF_CloseFile(file_handle);

//     /* memory cleanup */
//     BF_Block_Destroy(&header_block);
//     BF_Block_Destroy(&first_record_block);

//     return 1;
// }

int bplus_create_file(const TableSchema *schema, const char *fileName)
{
  return -1;
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

