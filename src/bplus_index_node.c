#include "../include/bf.h"
#include "../include/bplus_index_node.h"
#include "../include/bplus_file_structs.h"
// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων Δεδομένων.

#define CALL_BF(call)         \
    {                           \
        BF_ErrorCode code = call; \
        if (code != BF_OK)        \
        {                         \
            BF_PrintError(code);    \
            return -1;     \
        }                         \
    }

int is_index_block(char *block_start)
{
    int block_type;
    memcpy(&block_type, block_start, sizeof(int));
    return (block_type == BLOCK_TYPE_INDEX);
}

void index_block_print(char *block_start, BPlusMeta* metadata)
{
    char *block_ptr = block_start; // block_ptr will be moving forward

    int block_type;
    memcpy(&block_type, block_ptr, sizeof(int));
    if (block_type != BLOCK_TYPE_INDEX) {
        perror("Not an index block\n");
        return;
    }
    block_ptr += sizeof(int); // moving the pointer to the next data to print

    for (int i = 0; i < 20; i++) printf("-");
    printf("\n");

    IndexNodeHeader *header = malloc(sizeof(IndexNodeHeader));
    memcpy(header, block_ptr, sizeof(IndexNodeHeader));
    printf("index_count = %d\n", header->index_count);
    printf("parent_index = %d\n", header->parent_index);
    printf("min_record_key = %d\n\n", header->min_record_key);
    free(header);
    block_ptr += sizeof(IndexNodeHeader);

    printf("Indexes and keys in ascending order:\n");

    int leftmost_index;
    memcpy(&leftmost_index, block_ptr, sizeof(int));
    printf("index: %d\n", leftmost_index);
    block_ptr += sizeof(int);

    for (int i = 0; i < header->index_count; i++) {
        IndexNodeEntry entry;
        memcpy(&entry, block_ptr, sizeof(IndexNodeEntry));
        printf("key: %d\n", entry.key);
        printf("index: %d\n", entry.right_index);
        block_ptr += sizeof(IndexNodeEntry);
    }
    printf("\n");

    printf("Unused space: %tdB\n", BF_BLOCK_SIZE - (block_ptr - block_start));

    for (int i = 0; i < 20; i++) printf("-");
    printf("\n");
}

IndexNodeHeader *index_block_get_header(char *block_start)
{
    char *target_start = block_start + sizeof(int);

    IndexNodeHeader *result = malloc(sizeof(IndexNodeHeader));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(IndexNodeHeader));
    return result;
}

int index_block_get_leftmost_index(char *block_start)
{
    char *target_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);

    int result;
    memcpy(&result, target_start, sizeof(int));
    return result;
}

IndexNodeEntry *index_block_get_entry(char *block_start, IndexNodeHeader *block_header, int index)
{
    int entry_count = block_header->index_count - 1; // leftmost index is not considered an entry
    if (index >= entry_count)
        return NULL;

    char *entry0_start = block_start + sizeof(int) + sizeof(IndexNodeHeader) + sizeof(int);
    char *target_start = entry0_start + index * sizeof(IndexNodeEntry);

    IndexNodeEntry *result = malloc(sizeof(IndexNodeEntry));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(IndexNodeEntry));
    return result;
}

void index_block_get_entries_as_array(char *block_start, IndexNodeHeader *block_header, IndexNodeEntry *entry_array)
{
    char *leftmost_index_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);
    char *entry1_start = leftmost_index_start + sizeof(int) + sizeof(IndexNodeEntry);

    // entry_array[0] corresponds to the block's leftmost index
    entry_array[0].key = block_header->min_record_key;
    memcpy(&(entry_array[0].right_index), leftmost_index_start, sizeof(int));

    // copying the rest of the entries
    memcpy(entry_array + sizeof(IndexNodeEntry), entry1_start, (block_header->index_count - 1) * sizeof(IndexNodeEntry));
}

int index_block_has_available_space(IndexNodeHeader *block_header, BPlusMeta *metadata)
{
    return (block_header->index_count < metadata->max_indexes_per_block);
}

// writes header in the IndexNodeHeader part of the block
void index_block_write_header(char *block_start, IndexNodeHeader *header)
{
    char *target_start = block_start + sizeof(int);
    memcpy(target_start, header, sizeof(IndexNodeHeader));
}

// writes leftmost_index in the leftmost index part of the block
void index_block_write_leftmost_index(char *block_start, int leftmost_index)
{
    char *target_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);
    memcpy(target_start, &leftmost_index, sizeof(int));
}

void index_block_write_array_as_entries(char *block_start, IndexNodeHeader *block_header, IndexNodeEntry *entry_array, int count)
{
    if (count < 1) return;

    char *leftmost_index_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);
    char *entry1_start = leftmost_index_start + sizeof(int) + sizeof(IndexNodeEntry);

    // entry_array[0] corresponds to the block's leftmost index
    block_header->min_record_key = entry_array[0].key;
    memcpy(leftmost_index_start, &(entry_array[0].right_index), sizeof(int));

    // copying the rest of the entries
    memcpy(entry1_start, entry_array + sizeof(IndexNodeEntry), (count - 1) * sizeof(IndexNodeEntry));
}