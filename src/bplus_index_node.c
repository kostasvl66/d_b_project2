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

void print_index_block(BF_Block* block, BPlusMeta* metadata)
{
    char *block_ptr = BF_Block_GetData(block);
    char *block_start = block_ptr;

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