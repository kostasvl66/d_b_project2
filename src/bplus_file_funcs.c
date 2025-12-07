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

#define SAFE_CALL(call, ctx)\
    {\
        if (call == -1) {\
            cleanup_context(&ctx);\
            return -1;\
        }\
    }

const char BF_MAGIC_NUM[4] = { 0x80, 0xAA, 'B', 'P' }; // this identifies the file format

// helper functions (not defined in bplus_file_funcs.h)

// ceiling for only for positive x
int get_ceiling(float x) 
{
    int floor_x = (int)x;
    if (x == (float)floor_x)
        return floor_x;
    else
        return floor_x + 1;
}

// starting from the root block (with root_index), searches for the data block that could contain a record with key as PK
// found_block must be already initialized, and gets the found block's handle
// found_block_index gets the found block's index
// returns 0 on success, -1 otherwise
int tree_search_data_block(int root_index, int key, int file_desc, BF_Block *found_block, int *found_block_index)
{
    // the leaf-node found block index is the root index of the innermost call
    *found_block_index = root_index;

    // getting the root block and its data
    CALL_BF(BF_GetBlock(file_desc, root_index, found_block));
    char *block_start = BF_Block_GetData(found_block);

    // if block is a data block, then it is found (remains pinned)
    if (is_data_block(block_start))
        return 0;

    // else it is an index block and must be searched
    IndexNodeHeader *block_header = index_block_read_header(block_start); // getting the header
    if (!block_header) {
        CALL_BF(BF_UnpinBlock(found_block));
        return -1;
    }

    // determining the new_root_index to follow
    int new_root_index;
    int position = index_block_key_search(block_start, block_header, key);
    if (position == INDEX_BLOCK_SEARCH_ERROR) {
        CALL_BF(BF_UnpinBlock(found_block));
        free(block_header);
        return -1;
    }

    if (position == -1) {
        // continue in leftmost index
        new_root_index = index_block_read_leftmost_index(block_start);
        free(block_header);
    }
    else {
        // continue in the entry index at the given position
        IndexNodeEntry *entry = index_block_read_entry(block_start, block_header, position);
        if (!entry) {
            CALL_BF(BF_UnpinBlock(found_block));
            free(block_header);
            return -1;
        }

        new_root_index = entry->right_index;
        free(block_header);
        free(entry);
    }

    // continuing the search in new_root_index
    CALL_BF(BF_UnpinBlock(found_block));
    return tree_search_data_block(new_root_index, key, file_desc, found_block, found_block_index);
}

// starting from non leaf node block, which is an index block (with non_leaf_node_index), its min_record_key is updated
// to new_min; then its parent is found and the parent's min_record_key is also updated; this is done recursively up to the root
// temp_block must be already initialized, and should be destroyed after the call
// returns 0 on success, -1 otherwise
int bubble_up_min_record_key(int non_leaf_node_index, int new_min, int file_desc, BF_Block *temp_block)
{
    // getting the non leaf node block and its data
    CALL_BF(BF_GetBlock(file_desc, non_leaf_node_index, temp_block));
    char *temp_block_start = BF_Block_GetData(temp_block);

    IndexNodeHeader *temp_block_header = index_block_read_header(temp_block_start);
    if (!temp_block_header) {
        CALL_BF(BF_UnpinBlock(temp_block));
        return -1;
    }

    temp_block_header->min_record_key = new_min; // updating min
    int parent_index = temp_block_header->parent_index; // storing parent index to visit next

    // writing back the header
    index_block_write_header(temp_block_start, temp_block_header);
    BF_Block_SetDirty(temp_block);

    // unpinning and cleanup
    CALL_BF(BF_UnpinBlock(temp_block));
    free(temp_block_header);

    // checking if there is parent or this is root
    if (parent_index == -1)
        return 0;
    else
        return bubble_up_min_record_key(parent_index, new_min, file_desc, temp_block);
}

// bplus functions

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
    header_temp->max_records_per_block = (int)((BF_BLOCK_SIZE - sizeof(DataNodeHeader) - sizeof(int)) / (sizeof(Record) + sizeof(int)));
    //header_temp->max_indexes_per_block = 1 + (int)((BF_BLOCK_SIZE - sizeof(IndexNodeHeader) - 2 * sizeof(int)) / sizeof(IndexNodeEntry));
    header_temp->max_indexes_per_block = 3;
    header_temp->root_index = -1; // this means that the B+ tree has currenty no root

    memcpy(BF_Block_GetData(header_block), header_temp, sizeof(BPlusMeta)); // memcpy to avoid unaligned address problems
    free(header_temp);

    // setting the block dirty and unpinning
    BF_Block_SetDirty(header_block);
    CALL_BF(BF_UnpinBlock(header_block));

    // closing the file
    BF_CloseFile(file_handle);

    // memory cleanup
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

// helper functions specifically for bplus_record_insert
// these that return int, always return 0 on success, -1 on failure

struct context {
    // bplus_record_insert arguments
    int file_desc;
    BPlusMeta *metadata;
    const Record *record;

    // variables
    BF_Block *header_block;
    char *header_block_start;
    BPlusMeta *internal_metadata;

    int inserted_key;
    int inserted_block_index;

    BF_Block *found_block;
    int found_block_index;
    char *found_block_start;
    DataNodeHeader *found_block_header;
    int *found_block_index_array;
    int found_block_insert_pos;

    Record *temp_heap;
    int *temp_index_array;
    int second_half_start;

    BF_Block *new_data_block;
    int new_data_block_index;
    char *new_data_block_start;
    DataNodeHeader *new_data_block_header;
    int *new_data_block_index_array;

    int parent_index_block_has_data_block_children;
    BF_Block *parent_index_block;
    int parent_index_block_index;
    char *parent_index_block_start;
    IndexNodeHeader *parent_index_block_header;
    int parent_index_block_insert_pos;
    IndexNodeEntry *parent_index_block_entry_array;

    BF_Block *new_parent_index_block;
    int new_parent_index_block_index;
    char *new_parent_index_block_start;
    IndexNodeHeader *new_parent_index_block_header;
    
    BF_Block *index_block;
    int index_block_index;
    char *index_block_start;
    IndexNodeHeader *index_block_header;

    BF_Block *new_index_block;
    int new_index_block_index;
    char *new_index_block_start;
    IndexNodeHeader *new_index_block_header;

    IndexNodeEntry *temp_entry_array;
};

void cleanup_context(struct context *ctx)
{
    // setting dirty, unpinning and destroying (conditionally)
    // all the following BF_Block pointers are initialized to NULL at the very start
    // if any of them is not NULL, they have been changed by helper functions and it is safe to BF_Block_Destroy them
    // else, destroying NULL block pointers is undefined, so it is avoided
    if (ctx->header_block) {
        BF_Block_SetDirty(ctx->header_block);
        BF_UnpinBlock(ctx->header_block);
        BF_Block_Destroy(&(ctx->header_block));
    }

    if (ctx->found_block) {
        BF_Block_SetDirty(ctx->found_block);
        BF_UnpinBlock(ctx->found_block);
        BF_Block_Destroy(&(ctx->found_block));
    }

    if (ctx->new_data_block) {
        BF_Block_SetDirty(ctx->new_data_block);
        BF_UnpinBlock(ctx->new_data_block);
        BF_Block_Destroy(&(ctx->new_data_block));
    }

    if (ctx->parent_index_block) {
        BF_Block_SetDirty(ctx->parent_index_block);
        BF_UnpinBlock(ctx->parent_index_block);
        BF_Block_Destroy(&(ctx->parent_index_block));
    }

    if (ctx->index_block) {
        BF_Block_SetDirty(ctx->index_block);
        BF_UnpinBlock(ctx->index_block);
        BF_Block_Destroy(&(ctx->index_block));
    }

    if (ctx->new_index_block) {
        BF_Block_SetDirty(ctx->new_index_block);
        BF_UnpinBlock(ctx->new_index_block);
        BF_Block_Destroy(&(ctx->new_index_block));
    }

    if (ctx->new_parent_index_block) {
        BF_Block_SetDirty(ctx->new_parent_index_block);
        BF_UnpinBlock(ctx->new_parent_index_block);
        BF_Block_Destroy(&(ctx->new_parent_index_block));
    }
    
    // memory cleanup
    // the following pointers are all initialized to NULL at the very start
    // if any is not NULL, it has been changed by helper functions and free is safe with that
    // else, free(NULL) is also safe
    free(ctx->internal_metadata);

    free(ctx->found_block_header);
    free(ctx->found_block_index_array);

    free(ctx->temp_heap);
    free(ctx->temp_index_array);

    free(ctx->new_data_block_header);
    free(ctx->new_data_block_index_array);

    free(ctx->parent_index_block_header);
    free(ctx->parent_index_block_entry_array);

    free(ctx->new_parent_index_block_header);

    free(ctx->index_block_header);

    free(ctx->new_index_block_header);

    free(ctx->temp_entry_array);
}

int load_internal_metadata(struct context *ctx)
{
    // getting block 0 and the internal_metadata
    BF_Block_Init(&(ctx->header_block));

    CALL_BF(BF_GetBlock(ctx->file_desc, 0, ctx->header_block));
    ctx->header_block_start = BF_Block_GetData(ctx->header_block);
    ctx->internal_metadata = malloc(sizeof(BPlusMeta));
    if (!(ctx->internal_metadata))
        return -1;

    memcpy(ctx->internal_metadata, ctx->header_block_start, sizeof(BPlusMeta));
    return 0;
}

int create_data_block_root(struct context *ctx)
{
    // there is no root, so it must be made as a data block, which will store the record directly
    BF_Block *root_block;
    BF_Block_Init(&root_block);

    // allocating the new block
    CALL_BF(BF_AllocateBlock(ctx->file_desc, root_block));
    char *root_block_start = BF_Block_GetData(root_block);

    // updating internal_metadata
    ctx->internal_metadata->block_count++;
    ctx->internal_metadata->record_count++;
    ctx->internal_metadata->root_index = ctx->internal_metadata->block_count - 1;
    memcpy(ctx->header_block_start, ctx->internal_metadata, sizeof(BPlusMeta));
    memcpy(ctx->metadata, ctx->internal_metadata, sizeof(BPlusMeta)); // updating the external metadata

    // storing the value to return
    ctx->inserted_block_index = ctx->internal_metadata->root_index;

    // writing the block's header
    set_data_block(root_block_start);

    DataNodeHeader *root_block_header = malloc(sizeof(DataNodeHeader));
    if (!root_block_header)
        return -1;
    
    root_block_header->record_count = 1;
    root_block_header->parent_index = -1; // root has no parent
    root_block_header->next_index = -1; // no siblings
    root_block_header->min_record_key = ctx->inserted_key;

    data_block_write_header(root_block_start, root_block_header);

    // writing the block's first record (in heap and index array)
    if (data_block_write_unordered_record(root_block_start, ctx->internal_metadata, 0, ctx->record) == -1) {
        CALL_BF(BF_UnpinBlock(root_block));
        BF_Block_Destroy(&root_block);
        free(root_block_header);
        return -1;
    }

    int *root_index_array = malloc(ctx->internal_metadata->max_records_per_block * sizeof(int));
    if (!root_index_array) {
        CALL_BF(BF_UnpinBlock(root_block));
        BF_Block_Destroy(&root_block);
        free(root_block_header);
        return -1;
    }
    
    root_index_array[0] = 0; // the first ordered record is the first in heap
    data_block_write_index_array(root_block_start, ctx->internal_metadata, root_index_array);

    BF_Block_SetDirty(root_block);
    CALL_BF(BF_UnpinBlock(root_block));
    BF_Block_Destroy(&root_block);
    free(root_block_header);
    free(root_index_array);
    return 0;
}

int find_matching_data_block(struct context *ctx)
{
    // searching for the data block that could contain a record with inserted_key as PK
    BF_Block_Init(&(ctx->found_block)); // initializing the data block that the search will find

    if (tree_search_data_block(ctx->internal_metadata->root_index, ctx->inserted_key,
            ctx->file_desc, ctx->found_block, &(ctx->found_block_index)) == -1
    ) return -1;

    return 0;
}

int find_data_block_insert_pos(struct context *ctx)
{
    ctx->found_block_start = BF_Block_GetData(ctx->found_block);
    ctx->found_block_header = data_block_read_header(ctx->found_block_start);
    if (!(ctx->found_block_header)) return -1;

    ctx->found_block_index_array = data_block_read_index_array(ctx->found_block_start, ctx->internal_metadata);
    if (!(ctx->found_block_index_array)) return -1;

    // searching for the position that the record could be inserted at
    ctx->found_block_insert_pos = data_block_search_insert_pos(ctx->found_block_start, ctx->found_block_header,
                                      ctx->found_block_index_array, ctx->internal_metadata, ctx->inserted_key);

    if (ctx->found_block_insert_pos == -1) // new record already exists
        return -1;

    return 0;
}

int insert_record_to_data_block(struct context *ctx)
{
    ctx->found_block_header->record_count++;
    ctx->internal_metadata->record_count++;
    memcpy(ctx->header_block_start, ctx->internal_metadata, sizeof(BPlusMeta));
    memcpy(ctx->metadata, ctx->internal_metadata, sizeof(BPlusMeta)); // updating the external metadata

    // first writing to the end of the heap
    int heap_append_pos = ctx->found_block_header->record_count - 1;
    if (data_block_write_unordered_record(ctx->found_block_start, ctx->internal_metadata,
            heap_append_pos, ctx->record) == -1
    ) return -1;

    // shifting the indexes of index array starting from position found_block_insert_pos, to make space for the new index
    memmove(
        &(ctx->found_block_index_array[ctx->found_block_insert_pos + 1]),
        &(ctx->found_block_index_array[ctx->found_block_insert_pos]),
        (ctx->found_block_header->record_count - 1 - ctx->found_block_insert_pos) * sizeof(int)
    );

    // setting index array's new free position to hold the new record's heap_append_pos
    ctx->found_block_index_array[ctx->found_block_insert_pos] = heap_append_pos;

    // if the new record's key is smaller than block's min key, it must be updated
    if (ctx->inserted_key < ctx->found_block_header->min_record_key) {
        ctx->found_block_header->min_record_key = ctx->inserted_key;
        
        // if the data block has a parent (an index node), the parent's min key must also be updated
        // that update must "bubble up" from parent to parent, to the root
        if (ctx->found_block_header->parent_index != -1) {
            BF_Block *temp_block;
            BF_Block_Init(&temp_block);
            bubble_up_min_record_key(ctx->found_block_header->parent_index, ctx->inserted_key, ctx->file_desc, temp_block);
            BF_Block_Destroy(&temp_block);
        }
    }

    // writing the header and index array back to the block
    data_block_write_header(ctx->found_block_start, ctx->found_block_header);
    data_block_write_index_array(ctx->found_block_start, ctx->internal_metadata, ctx->found_block_index_array);

    // storing the value to return
    ctx->inserted_block_index = ctx->found_block_index;

    return 0;
}

int prepare_for_new_data_block(struct context *ctx)
{
    // creating a temp_heap with one more space than found block's heap
    ctx->temp_heap = malloc((ctx->internal_metadata->max_records_per_block + 1) * sizeof(Record));
    if (!(ctx->temp_heap)) return -1;

    // copying found block's heap to temp_heap, leaving the last element empty
    data_block_read_heap_as_array(ctx->found_block_start, ctx->found_block_header, ctx->internal_metadata, ctx->temp_heap);

    // adding the new record to the end of the temp_heap, which is currently empty
    memcpy(&(ctx->temp_heap[ctx->internal_metadata->max_records_per_block]), ctx->record, sizeof(Record));

    // creating a temp_index_array with one more space than found block's index array
    ctx->temp_index_array = malloc((ctx->internal_metadata->max_records_per_block + 1) * sizeof(int));
    if (!(ctx->temp_index_array)) return -1;

    // copying found block's index array to temp_index_array, leaving the last element empty
    memcpy(ctx->temp_index_array, ctx->found_block_index_array, ctx->internal_metadata->max_records_per_block * sizeof(int));

    // shifting temp_index_array's elements starting in position found_block_insert_pos by one element;
    // this makes space for the new index
    memmove(
        &(ctx->temp_index_array[ctx->found_block_insert_pos + 1]),
        &(ctx->temp_index_array[ctx->found_block_insert_pos]),
        (ctx->found_block_header->record_count - ctx->found_block_insert_pos) * sizeof(int)
    );

    // adding the new record's heap index to the position found_block_insert_pos of temp_index_array
    ctx->temp_index_array[ctx->found_block_insert_pos] = ctx->internal_metadata->max_records_per_block;

    // defining the first position of temp_index_array from which the new data block (to be made) will start
    // the first half will be larger by 1 or equal to the second half
    ctx->second_half_start = get_ceiling((ctx->internal_metadata->max_records_per_block + 1) / 2.0f);

    return 0;
}

int create_new_data_block(struct context *ctx)
{
    BF_Block_Init(&(ctx->new_data_block));

    // allocating the new block and getting its data
    CALL_BF(BF_AllocateBlock(ctx->file_desc, ctx->new_data_block));
    ctx->new_data_block_start = BF_Block_GetData(ctx->new_data_block);

    // updating metadata
    ctx->internal_metadata->block_count++;
    ctx->internal_metadata->record_count++;
    memcpy(ctx->header_block_start, ctx->internal_metadata, sizeof(BPlusMeta));
    memcpy(ctx->metadata, ctx->internal_metadata, sizeof(BPlusMeta)); // updating the external metadata

    // calculating new_data_block_index for later
    ctx->new_data_block_index = ctx->internal_metadata->block_count - 1;

    // setting to data block and allocating header and index array
    set_data_block(ctx->new_data_block_start);

    ctx->new_data_block_header = malloc(sizeof(DataNodeHeader));
    if (!(ctx->new_data_block_header))
        return -1;

    ctx->new_data_block_index_array = malloc(ctx->internal_metadata->max_records_per_block * sizeof(int));
    if (!(ctx->new_data_block_index_array))
        return -1;

    return 0;
}

int split_content_between_data_blocks(struct context *ctx)
{
    // the first (old) data block is found_block and second (new) data block is new_data_block

    int first_half_count = ctx->second_half_start;
    int second_half_count = (ctx->internal_metadata->max_records_per_block + 1) - first_half_count;

    // finding in which block index the inserted record is to go
    int first_key_in_second_half = record_get_key(&(ctx->internal_metadata->schema),
                                        &(ctx->temp_heap[ctx->temp_index_array[ctx->second_half_start]]));
    if (ctx->inserted_key >= first_key_in_second_half)
        ctx->inserted_block_index = ctx->new_data_block_index;
    else
        ctx->inserted_block_index = ctx->found_block_index;

    // updating first data block
    for (int i = 0; i < ctx->second_half_start; i++) {
        Record *rec = &(ctx->temp_heap[ctx->temp_index_array[i]]);

        // overwriting the old heap
        if (data_block_write_unordered_record(ctx->found_block_start, ctx->internal_metadata, i, rec) == -1)
            return -1;

        ctx->found_block_index_array[i] = i;
    }
    data_block_write_index_array(ctx->found_block_start, ctx->internal_metadata, ctx->found_block_index_array);

    // updating second data block (the last i is the last position in temp_index_array, which has one more element)
    for (int i = ctx->second_half_start; i < ctx->internal_metadata->max_records_per_block + 1; i++) {
        Record *rec = &(ctx->temp_heap[ctx->temp_index_array[i]]);

        int local_i = i - ctx->second_half_start; // 0-based position for the second block's index array and heap

        // writing to the new block's heap
        if (data_block_write_unordered_record(ctx->new_data_block_start, ctx->internal_metadata, local_i, rec) == -1)
            return -1;

        ctx->new_data_block_index_array[local_i] = local_i;
    }
    data_block_write_index_array(ctx->new_data_block_start, ctx->internal_metadata, ctx->new_data_block_index_array);

    // updating headers
    ctx->found_block_header->record_count = first_half_count;
    ctx->new_data_block_header->record_count = second_half_count;

    ctx->new_data_block_header->parent_index = -1; // this will change later

    int found_block_old_next_index = ctx->found_block_header->next_index;
    ctx->new_data_block_header->next_index = found_block_old_next_index;
    ctx->found_block_header->next_index = ctx->new_data_block_index;

    int found_block_old_min_record_key = ctx->found_block_header->min_record_key;
    int found_block_new_min_record_key = record_get_key(&(ctx->internal_metadata->schema), &(ctx->temp_heap[ctx->temp_index_array[0]]));
    if (found_block_old_min_record_key != found_block_new_min_record_key) {

        // min key must change both for the found_block and for its parents, up to the root
        ctx->found_block_header->min_record_key = found_block_new_min_record_key;

        if (ctx->found_block_header->parent_index != -1) {
            BF_Block *temp_block;
            BF_Block_Init(&temp_block);
            bubble_up_min_record_key(ctx->found_block_header->parent_index, found_block_new_min_record_key, ctx->file_desc, temp_block);
            BF_Block_Destroy(&temp_block);
        }
    }

    ctx->new_data_block_header->min_record_key = first_key_in_second_half;
    
    // writing back the headers; parent index of the new block will change in any case, but still it is helpful
    data_block_write_header(ctx->found_block_start, ctx->found_block_header);
    data_block_write_header(ctx->new_data_block_start, ctx->new_data_block_header);

    // freeing temp_heap and temp_index_array because they have no more use
    free(ctx->temp_heap);
    free(ctx->temp_index_array);
    ctx->temp_heap = NULL;
    ctx->temp_index_array = NULL;

    return 0;
}

int create_index_block_root_above_data_blocks(struct context *ctx)
{
    BF_Block *root_index_block;
    BF_Block_Init(&root_index_block);

    CALL_BF(BF_AllocateBlock(ctx->file_desc, root_index_block));
    char *root_index_block_start = BF_Block_GetData(root_index_block);

    // updating internal_metadata
    ctx->internal_metadata->block_count++;
    ctx->internal_metadata->root_index = ctx->internal_metadata->block_count - 1;
    memcpy(ctx->header_block_start, ctx->internal_metadata, sizeof(BPlusMeta));
    memcpy(ctx->metadata, ctx->internal_metadata, sizeof(BPlusMeta)); // updating the external metadata

    // updating the block's header
    set_index_block(root_index_block_start);

    IndexNodeHeader *root_index_block_header = malloc(sizeof(IndexNodeHeader));
    if (!root_index_block_header) {
        CALL_BF(BF_UnpinBlock(root_index_block));
        BF_Block_Destroy(&root_index_block);
        return -1;
    }
    
    root_index_block_header->index_count = 2;
    root_index_block_header->parent_index = -1; // root has no parent
    // root_index_block_header->min_record_key is updated later **internally** by index_block_write_array_as_entries()

    // assigning the entries and lefmost index
    IndexNodeEntry entry_array[2];

    entry_array[0].key = ctx->found_block_header->min_record_key; // this will become min key of root_index_block
    entry_array[0].right_index = ctx->found_block_index; // this will become leftmost index of root_index_block

    entry_array[1].key = ctx->new_data_block_header->min_record_key; // this will be the first key in root_index_block
    entry_array[1].right_index = ctx->new_data_block_index; // this will be the index in the right of the first key

    // writing back both leftmost index and entries, which is done in this one call
    index_block_write_array_as_entries(root_index_block_start, root_index_block_header, entry_array, 2);

    // writing back the header
    index_block_write_header(root_index_block_start, root_index_block_header);

    // updating the data block children to point to the new root
    ctx->found_block_header->parent_index = ctx->internal_metadata->root_index;
    data_block_write_header(ctx->found_block_start, ctx->found_block_header);

    ctx->new_data_block_header->parent_index = ctx->internal_metadata->root_index;
    data_block_write_header(ctx->new_data_block_start, ctx->new_data_block_header);

    BF_Block_SetDirty(root_index_block);
    CALL_BF(BF_UnpinBlock(root_index_block));
    BF_Block_Destroy(&root_index_block);
    free(root_index_block_header);
    return 0;
}

int initialize_parent_index_block(struct context *ctx)
{
    // getting the parent of found block
    BF_Block_Init(&(ctx->parent_index_block));
    CALL_BF(BF_GetBlock(ctx->file_desc, ctx->found_block_header->parent_index, ctx->parent_index_block));
    ctx->parent_index_block_start = BF_Block_GetData(ctx->parent_index_block);

    ctx->parent_index_block_index = ctx->found_block_header->parent_index;

    // getting the header
    ctx->parent_index_block_header = index_block_read_header(ctx->parent_index_block_start);
    if (!(ctx->parent_index_block_header))
        return -1;

    return 0;
}

int find_index_block_insert_pos(struct context *ctx)
{
    // inserted_key can be safely overwritten, its old value is not needed anymore
    if (ctx->parent_index_block_has_data_block_children)
        ctx->inserted_key = ctx->new_data_block_header->min_record_key;
    else
        ctx->inserted_key = ctx->new_index_block_header->min_record_key;

    // getting the insert position as a 0-based entry position, which excludes the leftmost index
    int entry_pos = index_block_search_insert_pos(ctx->parent_index_block_start, ctx->parent_index_block_header, ctx->inserted_key);
    if (entry_pos == INDEX_BLOCK_SEARCH_ERROR || entry_pos == -1)
        return -1;

    // the typical insert position is adjusted to include leftmost index as position 0, but in practice it cannot be 0
    // this is done for compatibility with helper functions that are used later
    ctx->parent_index_block_insert_pos = entry_pos + 1;

    return 0;
}

int insert_index_to_index_block(struct context *ctx)
{
    ctx->parent_index_block_header->index_count++;
    index_block_write_header(ctx->parent_index_block_start, ctx->parent_index_block_header);

    // getting the entry array
    ctx->parent_index_block_entry_array = malloc(ctx->parent_index_block_header->index_count * sizeof(IndexNodeEntry));
    if (!(ctx->parent_index_block_entry_array))
        return -1;

    index_block_read_entries_as_array(ctx->parent_index_block_start, ctx->parent_index_block_header, ctx->parent_index_block_entry_array);
    
    // shifting the entries of entry array starting from position parent_index_block_insert_pos, to make space for the new entry
    memmove(
        &(ctx->parent_index_block_entry_array[ctx->parent_index_block_insert_pos + 1]),
        &(ctx->parent_index_block_entry_array[ctx->parent_index_block_insert_pos]),
        (ctx->parent_index_block_header->index_count - 1 - ctx->parent_index_block_insert_pos) * sizeof(IndexNodeEntry)
    );

    // writing the new entry in entry array's insert position
    IndexNodeEntry inserted_entry;
    inserted_entry.key = ctx->inserted_key;
    if (ctx->parent_index_block_has_data_block_children)
        inserted_entry.right_index = ctx->new_data_block_index;
    else
        inserted_entry.right_index = ctx->new_index_block_index;

    memcpy(&(ctx->parent_index_block_entry_array[ctx->parent_index_block_insert_pos]), &inserted_entry, sizeof(IndexNodeEntry));

    // writing back the entry array
    index_block_write_array_as_entries(ctx->parent_index_block_start, ctx->parent_index_block_header,
        ctx->parent_index_block_entry_array, ctx->parent_index_block_header->index_count);

    // update parent index of added child
    if (ctx->parent_index_block_has_data_block_children) {
        ctx->new_data_block_header->parent_index = ctx->parent_index_block_index;
        data_block_write_header(ctx->new_data_block_start, ctx->new_data_block_header);
    }
    else {
        ctx->new_index_block_header->parent_index = ctx->parent_index_block_index;
        index_block_write_header(ctx->new_index_block_start, ctx->new_index_block_header);
    }

    return 0;
}

int prepare_for_new_parent_index_block(struct context *ctx)
{
    // find start of second block

    // allocating temp_entry_array to hold one more entry
    ctx->temp_entry_array = malloc((ctx->internal_metadata->max_indexes_per_block + 1) * sizeof(IndexNodeEntry));
    if (!(ctx->temp_entry_array))
        return -1;

    // copying parent index block's entries (including leftmost index) to temp_entry_array, leaving the last element empty
    index_block_read_entries_as_array(ctx->parent_index_block_start, ctx->parent_index_block_header, ctx->temp_entry_array);

    // shifting temp_entry_array's elements starting in position parent_index_block_insert_pos by one element;
    // this makes space for the new entry
    memmove(
        &(ctx->temp_entry_array[ctx->parent_index_block_insert_pos + 1]),
        &(ctx->temp_entry_array[ctx->parent_index_block_insert_pos]),
        (ctx->parent_index_block_header->index_count - ctx->parent_index_block_insert_pos) * sizeof(IndexNodeEntry)
    );

    // writing the new entry in temp_entry_array's insert position
    IndexNodeEntry inserted_entry;
    inserted_entry.key = ctx->inserted_key;
    if (ctx->parent_index_block_has_data_block_children)
        inserted_entry.right_index = ctx->new_data_block_index;
    else
        inserted_entry.right_index = ctx->new_index_block_index;

    memcpy(&(ctx->temp_entry_array[ctx->parent_index_block_insert_pos]), &inserted_entry, sizeof(IndexNodeEntry));

    // defining the first position of temp_entry_array from which the new index block (to be made) will start
    // the first half will be larger by 1 or equal to the second half
    ctx->second_half_start = get_ceiling((ctx->internal_metadata->max_indexes_per_block + 1) / 2.0f);

    return 0;
}

int create_new_parent_index_block(struct context *ctx)
{
    BF_Block_Init(&(ctx->new_parent_index_block));
    
    // allocating the new index block
    CALL_BF(BF_AllocateBlock(ctx->file_desc, ctx->new_parent_index_block));
    ctx->new_parent_index_block_start = BF_Block_GetData(ctx->new_parent_index_block);

    // update metadata
    ctx->internal_metadata->block_count++;
    memcpy(ctx->header_block_start, ctx->internal_metadata, sizeof(BPlusMeta));
    memcpy(ctx->metadata, ctx->internal_metadata, sizeof(BPlusMeta)); // updating the external metadata

    // calculating new data block's index
    ctx->new_parent_index_block_index = ctx->internal_metadata->block_count - 1;

    // setting to index block and allocating header
    set_index_block(ctx->new_parent_index_block_start);

    ctx->new_parent_index_block_header = malloc(sizeof(IndexNodeHeader));
    if (!(ctx->new_parent_index_block_header))
        return -1;

    return 0;
}

int split_content_between_index_blocks(struct context *ctx)
{
    // free temp_entry_array and don't forget to NULL it

    // the first (old) index block is parent_index_block and second (new) index block is new_parent_index_block

    int first_half_count = ctx->second_half_start;
    int second_half_count = (ctx->internal_metadata->max_indexes_per_block + 1) - first_half_count;

    // finding in which index block index the inserted entry is to go
    int inserted_entry_block_index;
    int first_key_in_second_half = ctx->temp_entry_array[ctx->second_half_start].key;
    if (ctx->inserted_key >= first_key_in_second_half)
        inserted_entry_block_index = ctx->new_parent_index_block_index;
    else
        inserted_entry_block_index = ctx->parent_index_block_index;

    // updating first data block
    index_block_write_array_as_entries(ctx->parent_index_block_start, ctx->parent_index_block_header,
        ctx->temp_entry_array, first_half_count);

    // updating second data block
    index_block_write_array_as_entries(ctx->new_parent_index_block_start, ctx->new_parent_index_block_header,
        &(ctx->temp_entry_array[ctx->second_half_start]), second_half_count);

    // updating headers for parent_index_block and new_parent_index_block
    ctx->parent_index_block_header->index_count = first_half_count;
    ctx->new_parent_index_block_header->index_count = second_half_count;

    ctx->new_parent_index_block_header->parent_index = -1; // this will change later
    
    // ctx->new_parent_index_block_header->min_record_key is updated internally by each index_block_write_array_as_entries() call

    // writing back the headers for parent_index_block and new_parent_index_block
    index_block_write_header(ctx->parent_index_block_start, ctx->parent_index_block_header);
    index_block_write_header(ctx->new_parent_index_block_start, ctx->new_parent_index_block_header);

    // updating headers of children
    if (ctx->parent_index_block_has_data_block_children) {
        // updating header of the inserted child
        ctx->new_data_block_header->parent_index = inserted_entry_block_index;
        data_block_write_header(ctx->new_data_block_start, ctx->new_data_block_header);

        // unpinning and destroying found_block and new_data_block, and freeing related data, as they are needed no more;
        // this is also done to consistently pin and unpin all children of new_parent_index_block,
        // as "double pin" has unspecified behavior
        BF_Block_SetDirty(ctx->found_block);
        BF_UnpinBlock(ctx->found_block);
        BF_Block_Destroy(&(ctx->found_block));
        free(ctx->found_block_header);
        free(ctx->found_block_index_array);
        ctx->found_block = NULL;
        ctx->found_block_header = NULL;
        ctx->found_block_index_array = NULL;

        BF_Block_SetDirty(ctx->new_data_block);
        BF_UnpinBlock(ctx->new_data_block);
        BF_Block_Destroy(&(ctx->new_data_block));
        free(ctx->new_data_block_header);
        free(ctx->new_data_block_index_array);
        ctx->new_data_block = NULL;
        ctx->new_data_block_header = NULL;
        ctx->new_data_block_index_array = NULL;

        // updating header of all new_parent_index_block's children
        BF_Block *temp_block;
        BF_Block_Init(&temp_block);

        for (int i = ctx->second_half_start; i < ctx->internal_metadata->max_indexes_per_block + 1; i++) {
            CALL_BF(BF_GetBlock(ctx->file_desc, ctx->temp_entry_array[i].right_index, temp_block));
            char *temp_block_start = BF_Block_GetData(temp_block);

            DataNodeHeader *temp_block_header = data_block_read_header(temp_block_start);
            if (!temp_block_header) return -1;

            temp_block_header->parent_index = ctx->new_parent_index_block_index;
            data_block_write_header(temp_block_start, temp_block_header);

            free(temp_block_header);
            CALL_BF(BF_UnpinBlock(temp_block));
        }

        BF_Block_Destroy(&temp_block);
    }
    else {
        // updating header of the inserted child
        ctx->new_index_block_header->parent_index = inserted_entry_block_index;
        index_block_write_header(ctx->new_index_block_start, ctx->new_index_block_header);

        // unpinning and destroying index_block and new_index_block, and freeing related data, as they are needed no more;
        // this is also done to consistently pin and unpin all children of new_parent_index_block,
        // as "double pin" has unspecified behavior
        BF_Block_SetDirty(ctx->index_block);
        BF_UnpinBlock(ctx->index_block);
        BF_Block_Destroy(&(ctx->index_block));
        free(ctx->index_block_header);
        ctx->index_block = NULL;
        ctx->index_block_header = NULL;

        BF_Block_SetDirty(ctx->new_index_block);
        BF_UnpinBlock(ctx->new_index_block);
        BF_Block_Destroy(&(ctx->new_index_block));
        free(ctx->new_index_block_header);
        ctx->new_index_block = NULL;
        ctx->new_index_block_header = NULL;

        // updating header of all new_parent_index_block's children
        BF_Block *temp_block;
        BF_Block_Init(&temp_block);

        for (int i = ctx->second_half_start; i < ctx->internal_metadata->max_indexes_per_block + 1; i++) {
            CALL_BF(BF_GetBlock(ctx->file_desc, ctx->temp_entry_array[i].right_index, temp_block));
            char *temp_block_start = BF_Block_GetData(temp_block);

            IndexNodeHeader *temp_block_header = index_block_read_header(temp_block_start);
            if (!temp_block_header) return -1;

            temp_block_header->parent_index = ctx->new_parent_index_block_index;
            index_block_write_header(temp_block_start, temp_block_header);

            free(temp_block_header);
            CALL_BF(BF_UnpinBlock(temp_block));
        }

        BF_Block_Destroy(&temp_block);
    }

    // freeing temp_entry_array as it is not needed anymore
    free(ctx->temp_entry_array);
    ctx->temp_entry_array = NULL;

    return 0;
}

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{   
    // this contains the "context variables" needed by this function;
    // it is used to pass the whole context to each helper function;
    // each helper function can update the context, so that other ones can use it later
    struct context ctx = { 0 }; // all members are initialized to 0 (pointers to NULL)

    // initializing argument-members of the context
    ctx.file_desc = file_desc;
    ctx.metadata = metadata;
    ctx.record = record;

    // getting the internal B+ tree metadata
    SAFE_CALL(load_internal_metadata(&ctx), ctx);
    ctx.inserted_key = record_get_key(&(ctx.internal_metadata->schema), record); // for convenience

    // checking if there is a root
    if (ctx.internal_metadata->root_index == -1) { // there is no root yet
        SAFE_CALL(create_data_block_root(&ctx), ctx);
        cleanup_context(&ctx);
        return ctx.inserted_block_index;
    }
    
    // else there is a root
    
    // find the data block that can contain the inserted_key
    SAFE_CALL(find_matching_data_block(&ctx), ctx);

    // find the hypothetical insert position for the new record in the matching data block, even if it doesn't have free space
    SAFE_CALL(find_data_block_insert_pos(&ctx), ctx);

    // checking if the matching data block actually has free space
    if (data_block_has_available_space(ctx.found_block_header, ctx.internal_metadata)) {
        // inserting the record to the data block
        SAFE_CALL(insert_record_to_data_block(&ctx), ctx);
        cleanup_context(&ctx);
        return ctx.inserted_block_index;
    }

    // else the data block has no free space

    // create temporary buffers before splitting the data block contents
    SAFE_CALL(prepare_for_new_data_block(&ctx), ctx);

    // create the new data block, still without contents
    SAFE_CALL(create_new_data_block(&ctx), ctx);

    // update the old and new block with the new contents
    SAFE_CALL(split_content_between_data_blocks(&ctx), ctx);

    // if the old block has no parent, the first index block must be made, and it will be the new root
    if (ctx.found_block_header->parent_index == -1) {
        SAFE_CALL(create_index_block_root_above_data_blocks(&ctx), ctx);
        cleanup_context(&ctx);
        return ctx.inserted_block_index;
    }

    // else the old block does have a parent, and the new block must be assigned to a parent too

    // assign found block's parent to parent_index_block
    ctx.parent_index_block_has_data_block_children = 1;
    SAFE_CALL(initialize_parent_index_block(&ctx), ctx);

    do {
        if (!ctx.parent_index_block_has_data_block_children) {
            // assigning index block's parent to index block
            // TODO
        }

        // find the hypothetical insert position for the new block's index in index block, even if it doesn't have free space
        SAFE_CALL(find_index_block_insert_pos(&ctx), ctx);

        if (index_block_has_available_space(ctx.parent_index_block_header, ctx.internal_metadata)) {
            // inserting the index to the parent index block
            SAFE_CALL(insert_index_to_index_block(&ctx), ctx);
            cleanup_context(&ctx);
            return ctx.inserted_block_index;
        }

        // else the parent index block has no free space
        
        // create temporary buffer before splitting the index block contents
        SAFE_CALL(prepare_for_new_parent_index_block(&ctx), ctx);

        // create the new index block, still without contents
        SAFE_CALL(create_new_parent_index_block(&ctx), ctx);

        // update the old and new index block with the new contents
        SAFE_CALL(split_content_between_index_blocks(&ctx), ctx);
        cleanup_context(&ctx);
        return ctx.inserted_block_index;

        // updating flag after the first iteration
        if (ctx.parent_index_block_has_data_block_children)
            ctx.parent_index_block_has_data_block_children = 0;

    } while (ctx.parent_index_block_header->parent_index != -1);


    cleanup_context(&ctx);
    return ctx.inserted_block_index;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata,
                      const int key, Record **out_record) {
  *out_record = NULL;

  BPlusMeta *tree_info;
  BF_Block *info_block;

  // Receiving B+_Tree File metadata
  BF_Block_Init(&info_block);
  CALL_BF(BF_GetBlock(file_desc, 0, info_block));

  tree_info = malloc(sizeof(BPlusMeta));
  char *tree_info_start = BF_Block_GetData(info_block);
  memcpy(tree_info, tree_info_start, sizeof(BPlusMeta));

  // Receiving the block index of the root from the metadata header
  int root_pos = tree_info->root_index;

  // Receiving data block which potentially contains the key being searched
  BF_Block *res_block;
  BF_Block_Init(&res_block);
  int *block_index = malloc(sizeof(int));
  tree_search_data_block(root_pos, key, file_desc, res_block, block_index);

  // Accessing the acquired data block
  char *data_block_start = BF_Block_GetData(res_block);

  // Receiving the specific data block's header and metadata, specifically the
  // number of records currently stored in the block
  DataNodeHeader *data_block_header = data_block_read_header(data_block_start);
  int number_of_records = data_block_header->record_count;

  // Receiving a list of the available indexes, sorted from the lowest to
  // highest key value
  int *indices = malloc(number_of_records * sizeof(int));
  indices = data_block_read_index_array(data_block_start, tree_info);

  // Iterating through the accessed data block for the record with the key value
  // being searched for
  Record *rec = malloc(sizeof(Record));

  for (int i = 0; i < number_of_records; i++) {
    rec = data_block_read_record(data_block_start, data_block_header, indices,
                                 tree_info, i);
    int pk = record_get_key(&tree_info->schema, rec);
    if (pk == key) {
      *out_record = rec;

      // Clearing memory
      BF_UnpinBlock(info_block);
      BF_UnpinBlock(res_block);
      free(indices);

      return 0;
    }

    free(rec);
  }

  // Clearing memory
  BF_UnpinBlock(info_block);
  BF_UnpinBlock(res_block);
  free(indices);

  return -1;
}