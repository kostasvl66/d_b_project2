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
    header_temp->max_indexes_per_block = 1 + (int)((BF_BLOCK_SIZE - sizeof(IndexNodeHeader) - 2 * sizeof(int)) / sizeof(IndexNodeEntry));
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
    char *new_data_block_start;
    DataNodeHeader *new_data_block_header;
    int *new_data_block_index_array;
};

void cleanup_context(struct context *ctx)
{
    // setting dirty, unpinning and destroying (conditionally)
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
    
    // memory cleanup
    free(ctx->internal_metadata);

    free(ctx->found_block_header);
    free(ctx->found_block_index_array);

    free(ctx->temp_heap);
    free(ctx->temp_index_array);

    free(ctx->new_data_block_header);
    free(ctx->new_data_block_index_array);
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
    memcpy(ctx->metadata, ctx->internal_metadata, sizeof(BPlusMeta)); // updating external metadata

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
    BF_Block_Init(ctx->new_data_block);

    // allocating the new block and getting its data
    CALL_BF(BF_AllocateBlock(ctx->file_desc, ctx->new_data_block));
    ctx->new_data_block_start = BF_Block_GetData(ctx->new_data_block);

    // TODO: calculate and store the block's index, read header and index array
}

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{   
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

    

    cleanup_context(&ctx);
    return ctx.inserted_block_index;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record **out_record) {
    *out_record = NULL;
    return -1;
}
