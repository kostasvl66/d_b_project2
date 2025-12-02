#include "../include/bf.h"
#include "../include/bplus_datanode.h"
#include "../include/bplus_file_structs.h"
// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων toy Ευρετηρίου.

#define CALL_BF(call)         \
    {                           \
        BF_ErrorCode code = call; \
        if (code != BF_OK)        \
        {                         \
            BF_PrintError(code);    \
            return -1;     \
        }                         \
    }

int is_data_block(char *block_start)
{
    int block_type;
    memcpy(&block_type, block_start, sizeof(int));
    return (block_type == BLOCK_TYPE_DATA);
}

void data_block_print(char *block_start, BPlusMeta* metadata)
{
    char *block_ptr = block_start; // block_ptr will be moving forward

    int block_type;
    memcpy(&block_type, block_ptr, sizeof(int));
    if (block_type != BLOCK_TYPE_DATA) {
        perror("Not a data block\n");
        return;
    }
    block_ptr += sizeof(int); // moving the pointer to the next data to print

    for (int i = 0; i < 20; i++) printf("-");
    printf("\n");

    DataNodeHeader *header = malloc(sizeof(DataNodeHeader));
    memcpy(header, block_ptr, sizeof(DataNodeHeader));
    printf("record_count = %d\n", header->record_count);
    printf("parent_index = %d\n", header->parent_index);
    printf("next_index = %d\n\n", header->next_index);
    printf("min_record_key = %d\n\n", header->min_record_key);
    free(header);
    block_ptr += sizeof(DataNodeHeader);

    int *index_array = malloc(metadata->max_records_per_block * sizeof(int));
    memcpy(index_array, block_ptr, metadata->max_records_per_block * sizeof(int));
    printf("Index array: ");
    for (int i = 0; i < metadata->max_records_per_block; i++)
        printf("%d, ", index_array[i]);
    printf("\n\n");
    free(index_array);
    block_ptr += sizeof(metadata->max_records_per_block * sizeof(int));

    printf("Records:\n");
    for (int i = 0; i < header->record_count; i++) {
        Record *rec = malloc(sizeof(Record));
        memcpy(rec, block_ptr, sizeof(Record));
        printf("%d -> ", i);
        record_print(&metadata->schema, rec);
        printf("\n");
        free(rec);
        block_ptr += sizeof(Record);
    }
    printf("\n");
    
    printf("Unused space: %tdB\n", BF_BLOCK_SIZE - (block_ptr - block_start));

    for (int i = 0; i < 20; i++) printf("-");
    printf("\n");
}

DataNodeHeader *data_block_get_header(char *block_start)
{
    char *target_start = block_start + sizeof(int);

    DataNodeHeader *result = malloc(sizeof(DataNodeHeader));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(DataNodeHeader));
    return result;
}

int *data_block_get_index_array(char *block_start, BPlusMeta *metadata)
{
    char *target_start = block_start + sizeof(int) + sizeof(DataNodeHeader);

    int *result = malloc(metadata->max_records_per_block * sizeof(int));
    if (!result) return NULL;

    memcpy(result, target_start, metadata->max_records_per_block * sizeof(int));
    return result;
}

Record *data_block_get_unordered_record(char *block_start, BPlusMeta *metadata, int index)
{
    if (index >= metadata->max_records_per_block)
        return NULL;

    int index_array_length = metadata->max_records_per_block;
    char *record0_start = block_start + sizeof(int) + sizeof(DataNodeHeader) + index_array_length * sizeof(int);
    char *target_start = record0_start + index * sizeof(Record);

    Record *result = malloc(sizeof(Record));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(Record));
    return result;
}

Record *data_block_get_record(char *block_start, DataNodeHeader *block_header, int *index_array, BPlusMeta *metadata, int index)
{
    if (index >= block_header->record_count)
        return NULL;

    int index_array_length = metadata->max_records_per_block;
    char *record0_start = block_start + sizeof(int) + sizeof(DataNodeHeader) + index_array_length * sizeof(int);

    // index of record in the unsorted "heap" of records
    int heap_index = index_array[index];
    char *target_start = record0_start + heap_index * sizeof(Record);

    Record *result = malloc(sizeof(Record));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(Record));
    return result;
}

int data_block_has_available_space(DataNodeHeader *block_header, BPlusMeta *metadata)
{
    return (block_header->record_count < metadata->max_records_per_block);
}