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

int is_data_block(const char *block_start)
{
    int block_type;
    memcpy(&block_type, block_start, sizeof(int));
    return (block_type == BLOCK_TYPE_DATA);
}

void set_data_block(char *block_start)
{
    int block_type_data = BLOCK_TYPE_DATA;
    memcpy(block_start, &block_type_data, sizeof(int));
}

void data_block_print(const char *block_start, const BPlusMeta *metadata)
{
    const char *block_ptr = block_start; // block_ptr will be moving forward

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

DataNodeHeader *data_block_read_header(const char *block_start)
{
    const char *target_start = block_start + sizeof(int);

    DataNodeHeader *result = malloc(sizeof(DataNodeHeader));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(DataNodeHeader));
    return result;
}

int *data_block_read_index_array(const char *block_start, const BPlusMeta *metadata)
{
    const char *target_start = block_start + sizeof(int) + sizeof(DataNodeHeader);

    int *result = malloc(metadata->max_records_per_block * sizeof(int));
    if (!result) return NULL;

    memcpy(result, target_start, metadata->max_records_per_block * sizeof(int));
    return result;
}

Record *data_block_read_unordered_record(const char *block_start, const BPlusMeta *metadata, int index)
{
    if (index >= metadata->max_records_per_block)
        return NULL;

    int index_array_length = metadata->max_records_per_block;
    const char *record0_start = block_start + sizeof(int) + sizeof(DataNodeHeader) + index_array_length * sizeof(int);
    const char *target_start = record0_start + index * sizeof(Record);

    Record *result = malloc(sizeof(Record));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(Record));
    return result;
}

Record *data_block_read_record(const char *block_start, const DataNodeHeader *block_header, const int *index_array,
                               const BPlusMeta *metadata, int index)
{
    if (index >= block_header->record_count)
        return NULL;

    int index_array_length = metadata->max_records_per_block;
    const char *record0_start = block_start + sizeof(int) + sizeof(DataNodeHeader) + index_array_length * sizeof(int);

    // index of record in the unsorted "heap" of records
    int heap_index = index_array[index];
    const char *target_start = record0_start + heap_index * sizeof(Record);

    Record *result = malloc(sizeof(Record));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(Record));
    return result;
}

void data_block_read_heap_as_array(const char *block_start, const DataNodeHeader *block_header,
                                   const BPlusMeta *metadata, Record *record_array)
{
    int index_array_length = metadata->max_records_per_block;
    const char *record0_start = block_start + sizeof(int) + sizeof(DataNodeHeader) + index_array_length * sizeof(int);

    memcpy(record_array, record0_start, block_header->record_count * sizeof(Record));
}

int data_block_has_available_space(const DataNodeHeader *block_header, const BPlusMeta *metadata)
{
    return (block_header->record_count < metadata->max_records_per_block);
}

void data_block_write_header(char *block_start, const DataNodeHeader *header)
{
    char *target_start = block_start + sizeof(int);
    memcpy(target_start, header, sizeof(DataNodeHeader));
}

void data_block_write_index_array(char *block_start, const BPlusMeta *metadata, const int *index_array)
{
    char *target_start = block_start + sizeof(int) + sizeof(DataNodeHeader);
    memcpy(target_start, index_array, metadata->max_records_per_block * sizeof(int));
}

int data_block_write_unordered_record(char *block_start, const BPlusMeta *metadata, int index, const Record *record)
{
    if (index >= metadata->max_records_per_block)
        return -1;
    
    int index_array_length = metadata->max_records_per_block;
    char *record0_start = block_start + sizeof(int) + sizeof(DataNodeHeader) + index_array_length * sizeof(int);
    char *target_start = record0_start + index * sizeof(Record);

    memcpy(target_start, record, sizeof(Record));
    return 0;
}

// internal binary search to use inside data_block_search_insert_pos(); both start and end are inclusive
// returns the same as data_block_search_insert_pos()
int data_block_binary_search_insert_pos(const char *block_start, const DataNodeHeader *block_header, const int *index_array,
                                        const BPlusMeta *metadata, int start, int end, int new_key)
{
    if (start > end) // new record goes in the start
        return start; 
    
    if (start == end) { // there is only one "unsearched" record remaining
        Record *remaining_record = data_block_read_record(block_start, block_header, index_array, metadata, start);
        if (!remaining_record) return DATA_BLOCK_SEARCH_ERROR;

        int remaining_record_key = record_get_key(&(metadata->schema), remaining_record);
        if (remaining_record_key == new_key) { // key already exists
            free(remaining_record);
            return -1;
        }
        else if (remaining_record_key > new_key) {
            // record must go in the current position, and the larger ones are to be shifted one place to the right
            free(remaining_record);
            return start;
        }
        else {
            // record must go in the next position, and the larger ones are to be shifted one place to the right
            free(remaining_record);
            return start + 1;
        }
    }

    // more than one "unsearched" records
    int mid = (int)((start + end) / 2); // using the floor of the division
    Record *record_at_mid = data_block_read_record(block_start, block_header, index_array, metadata, mid);
    if (!record_at_mid) return DATA_BLOCK_SEARCH_ERROR;

    int record_key_at_mid = record_get_key(&(metadata->schema), record_at_mid);
    if (record_key_at_mid == new_key) { // key already exists
        free(record_at_mid);
        return -1;
    }
    else if (record_key_at_mid > new_key) {
        free(record_at_mid);
        return data_block_binary_search_insert_pos(block_start, block_header, index_array, metadata, start, mid - 1, new_key);
    }
    else {
        free(record_at_mid);
        return data_block_binary_search_insert_pos(block_start, block_header, index_array, metadata, mid + 1, end, new_key);
    }
}

int data_block_search_insert_pos(const char *block_start, const DataNodeHeader *block_header, const int *index_array,
                                 const BPlusMeta *metadata, int new_key)
{
    return data_block_binary_search_insert_pos(block_start, block_header, index_array, metadata,
                                               0, block_header->record_count - 1, new_key);
}