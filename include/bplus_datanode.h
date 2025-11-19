#ifndef BP_DATANODE_H
#define BP_DATANODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

typedef struct {
    int record_count; // number of records currently stored in the data blocks
    int parent_index; // index of the parent block (index node)
    int next_index; // index to the adjacent (to the right) data node
} DataNodeHeader;

#endif