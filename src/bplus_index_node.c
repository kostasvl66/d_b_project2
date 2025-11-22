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

int index_block_has_available_space(IndexNodeHeader *block_header, BPlusMeta *metadata)
{
    return (block_header->index_count < metadata->max_indexes_per_block);
}