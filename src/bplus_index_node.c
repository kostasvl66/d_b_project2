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

int is_index_block(const char *block_start)
{
    int block_type;
    memcpy(&block_type, block_start, sizeof(int));
    return (block_type == BLOCK_TYPE_INDEX);
}

void set_index_block(char *block_start)
{
    int block_type_index = BLOCK_TYPE_INDEX;
    memcpy(block_start, &block_type_index, sizeof(int));
}

void index_block_print(const char *block_start, const BPlusMeta* metadata)
{
    char *block_ptr = (char *)block_start; // block_ptr will be moving forward

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
    block_ptr += sizeof(IndexNodeHeader);
    
    printf("Indexes and keys in ascending order:\n");
    
    int leftmost_index;
    memcpy(&leftmost_index, block_ptr, sizeof(int));
    printf("index: %d\n", leftmost_index);
    block_ptr += sizeof(int);
    for (int i = 0; i < header->index_count - 1; i++) {
        IndexNodeEntry entry;
        memcpy(&entry, block_ptr, sizeof(IndexNodeEntry));
        printf("key: %d\n", entry.key);
        printf("index: %d\n", entry.right_index);
        block_ptr += sizeof(IndexNodeEntry);
    }
    printf("\n");
    free(header);
    
    printf("Unused space: %td Bytes\n", BF_BLOCK_SIZE - (block_ptr - block_start));

    for (int i = 0; i < 20; i++) printf("-");
    printf("\n");
}

IndexNodeHeader *index_block_read_header(const char *block_start)
{
    const char *target_start = block_start + sizeof(int);

    IndexNodeHeader *result = malloc(sizeof(IndexNodeHeader));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(IndexNodeHeader));
    return result;
}

int index_block_read_leftmost_index(const char *block_start)
{
    const char *target_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);

    int result;
    memcpy(&result, target_start, sizeof(int));
    return result;
}

IndexNodeEntry *index_block_read_entry(const char *block_start, const IndexNodeHeader *block_header, int index)
{
    int entry_count = block_header->index_count - 1; // leftmost index is not considered an entry
    if (index >= entry_count)
        return NULL;

    const char *entry0_start = block_start + sizeof(int) + sizeof(IndexNodeHeader) + sizeof(int);
    const char *target_start = entry0_start + index * sizeof(IndexNodeEntry);

    IndexNodeEntry *result = malloc(sizeof(IndexNodeEntry));
    if (!result) return NULL;

    memcpy(result, target_start, sizeof(IndexNodeEntry));
    return result;
}

void index_block_read_entries_as_array(const char *block_start, const IndexNodeHeader *block_header, IndexNodeEntry *entry_array)
{
    const char *leftmost_index_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);
    const char *entry1_start = leftmost_index_start + sizeof(int);

    // entry_array[0] corresponds to the block's leftmost index
    entry_array[0].key = block_header->min_record_key;
    memcpy(&(entry_array[0].right_index), leftmost_index_start, sizeof(int));

    // copying the rest of the entries
    memcpy(&entry_array[1], entry1_start, (block_header->index_count - 1) * sizeof(IndexNodeEntry));
}

int index_block_has_available_space(const IndexNodeHeader *block_header, const BPlusMeta *metadata)
{
    return (block_header->index_count < metadata->max_indexes_per_block);
}

void index_block_write_header(char *block_start, const IndexNodeHeader *header)
{
    char *target_start = block_start + sizeof(int);
    memcpy(target_start, header, sizeof(IndexNodeHeader));
}

void index_block_write_leftmost_index(char *block_start, int leftmost_index)
{
    char *target_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);
    memcpy(target_start, &leftmost_index, sizeof(int));
}

void index_block_write_array_as_entries(char *block_start, IndexNodeHeader *block_header,
                                        const IndexNodeEntry *entry_array, int count)
{
    if (count < 1) return;

    char *leftmost_index_start = block_start + sizeof(int) + sizeof(IndexNodeHeader);
    char *entry1_start = leftmost_index_start + sizeof(int);

    // entry_array[0] corresponds to the block's leftmost index
    block_header->min_record_key = entry_array[0].key;
    memcpy(leftmost_index_start, &(entry_array[0].right_index), sizeof(int));

    // copying the rest of the entries
    memcpy(entry1_start, &entry_array[1], (count - 1) * sizeof(IndexNodeEntry));
}

// internal binary search to use inside index_block_search_insert_pos(); both start and end are inclusive
// returns the same as index_block_search_insert_pos()
int index_block_binary_search_insert_pos(const char *block_start, const IndexNodeHeader *block_header, int start, int end, int new_key)
{
    if (start > end) // new entry goes in start
        return start; 
    
    if (start == end) { // there is only one "unsearched" entry remaining
        IndexNodeEntry *remaining_entry = index_block_read_entry(block_start, block_header, start);
        if (!remaining_entry) return INDEX_BLOCK_SEARCH_ERROR;

        if (remaining_entry->key == new_key) { // key already exists
            free(remaining_entry);
            return -1;
        }
        else if (remaining_entry->key > new_key) {
            // entry must go in the current position, and the larger ones are to be shifted one place to the right
            free(remaining_entry);
            return start;
        }
        else {
            // entry must go in the next position, and the larger ones are to be shifted one place to the right
            free(remaining_entry);
            return start + 1;
        }
    }

    // more than one "unsearched" entries
    int mid = (int)((start + end) / 2); // using the floor of the division
    IndexNodeEntry *entry_at_mid = index_block_read_entry(block_start, block_header, mid);
    if (!entry_at_mid) return INDEX_BLOCK_SEARCH_ERROR;

    if (entry_at_mid->key == new_key) { // key already exists
        free(entry_at_mid);
        return -1;
    }
    else if (entry_at_mid->key > new_key) {
        free(entry_at_mid);
        return index_block_binary_search_insert_pos(block_start, block_header, start, mid - 1, new_key);
    }
    else {
        free(entry_at_mid);
        return index_block_binary_search_insert_pos(block_start, block_header, mid + 1, end, new_key);
    }
}

int index_block_search_insert_pos(const char *block_start, const IndexNodeHeader *block_header, int new_key)
{
    return index_block_binary_search_insert_pos(block_start, block_header, 0, block_header->index_count - 1, new_key);
}

// internal binary search to use inside index_block_key_search(); both start and end are inclusive
// returns the same as index_block_key_search()
int index_block_key_binary_search(const char *block_start, const IndexNodeHeader *block_header, int start, int end, int key)
{
    if (start > end) // key is in leftmost index
        return -1;
        
    if (start == end) { // there is only one "unsearched" entry remaining
        IndexNodeEntry *remaining_entry = index_block_read_entry(block_start, block_header, start);
        if (!remaining_entry) return INDEX_BLOCK_SEARCH_ERROR;

        if (remaining_entry->key == key) { // key is in the current index
            free(remaining_entry);
            return start;
        }
        else if (remaining_entry->key > key) { // key is in the previous index (can be -1)
            free(remaining_entry);
            return start - 1;
        }
        else {
            free(remaining_entry); // key is in the current index
            return start;
        }
    }

    // more than one "unsearched" entries
    int mid = (int)((start + end) / 2); // using the floor of the division
    IndexNodeEntry *entry_at_mid = index_block_read_entry(block_start, block_header, mid);
    if (!entry_at_mid) return INDEX_BLOCK_SEARCH_ERROR;

    if (entry_at_mid->key == key) { // key is in the mid index
        free(entry_at_mid);
        return mid;
    }
    else if (entry_at_mid->key > key) {
        free(entry_at_mid);
        return index_block_key_binary_search(block_start, block_header, start, mid - 1, key);
    }
    else {
        free(entry_at_mid);
        return index_block_key_binary_search(block_start, block_header, mid + 1, end, key);
    }
}

int index_block_key_search(const char *block_start, const IndexNodeHeader *block_header, int key)
{
    int current_entry_count = block_header->index_count - 1;
    return index_block_key_binary_search(block_start, block_header, 0, current_entry_count - 1, key);
}