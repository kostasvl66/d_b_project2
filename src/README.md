# Υλοποίηση των BPlus συναρτήσεων

- Υλοποιήσεις από Εμμανουήλ-Ταξιάρχη Οζίνη (sdi2300147): `bplus_create_file`, `bplus_record_insert`
- Υλοποιήσεις από Κωνσταντίνο Γεώργιο Βλαζάκη (sdi2300017): `bplus_open_file`, `bplus_close_file`, `bplus_record_find`

## Επεξήγηση αναπαράστασης B+ δέντρου
Κάθε block χαρακτηρίζεται ως *data block* η *index block*. Εξαίρεση είναι το block 0 που περιέχει τα metadata και επειδή είναι στατικό δεν παρέχει τρόπο ελέγχου για το είδος του.

### data blocks

Παρακάτω φαίνεται η εσωτερική δομή που έχει καθοριστεί για κάθε *data block*, όπως φαίνεται και στον κώδικα:
```c
/* The structure of a data block is the following; for each [][] pair there is no padding between
** (START)[int][DataNodeHeader][int[max_records_per_block]][Record][Record]...[Record][possibly unused space](END)
** - int is BLOCK_TYPE_DATA for data block, BLOCK_TYPE_INDEX for index block
** - DataNodeHeader is the data block header
** - int[max_records_per_block] is an array of indexes to records, remains sorted so that the records themselves need not be sorted;
**                              Only the first n values are valid, if n is the current number of records in the block
** - Record (0) ... Record (k) with k < max_records_per_block are record data, each new one appended at the end;
**                             because of that, this heap part is unsorted; their sorted order is defined using
**                             the index array, which is always updated as needed
** - possibly unused space is either space not yet used by future records or a remainder < sizeof(Record)
*/
```

Η δομή `DataNodeHeader` είναι για την αποθήκευση του header του data block και ορίζεται ως:
```c
typedef struct {
    int record_count; // number of records currently stored in the data blocks
    int parent_index; // index of the parent block (index node)
    int next_index; // index to the adjacent (to the right) data node
    int min_record_key; // the minimum key of all records in the block; useful in insertion
} DataNodeHeader;
```

### index blocks

Παρακάτω φαίνεται η εσωτερική δομή που έχει καθοριστεί για κάθε *index block*, όπως φαίνεται και στον κώδικα:
```c
/* The structure of an index block is the following; for each [][] pair there is no padding between
** (START)[int][IndexNodeHeader][int][IndexNodeEntry][IndexNodeEntry]...[IndexNodeEntry][possibly unused space](END)
** - int (first) is BLOCK_TYPE_DATA for data block, BLOCK_TYPE_INDEX for index block
** - IndexNodeHeader is the index block header
** - int (second) is leftmost index of the block, such that for each key accessible via that index: key < first_entry.key,
**                                                where first_entry is the leftmost (smallest) IndexNodeEntry in the block
** - IndexNodeEntry (0) ... IndexNodeEntry (k) with k < max_indexes_per_block are key-index entries,
**                                             such that for each key accessible via entry.index, key >= entry.key for any entry;
**                                             when a new one is inserted, some others are shifted to maintain ordering
** - possibly unused space is either space not yet used by future entries or a remainder < sizeof(IndexNodeEntry)
*/
```

Η δομή `IndexNodeHeader` είναι για την αποθήκευση του header του *index block* και ορίζεται ως:
```c
typedef struct {
    // index_count is the number of children indexes currently stored
    // number of keys is always index_count - 1
    // number of entries (IndexNodeEntry) is also index_count - 1 (leftmost index is not an "entry")
    int index_count;
    int parent_index; // index of the parent block (index node)
    int min_record_key; // minimum key accessible via the leftmost index; useful in insertion
} IndexNodeHeader;
```

Επίσης η δομή `IndexNodeEntry` χρησιμοποιείται για την αποθήκευση ζευγών κλειδιού και δεξιού δείκτη στα *index blocks*, και για τον αριστερότερο δείκτη γίνεται ξεχωριστή αποθήκευση ενός `int` που αναφέρεται ως `leftmost_index`. Η δομή `IndexNodeEntry` ορίζεται ως:
```c
typedef struct {
    // a key such that current_entry.right_index keys >= key and prev_entry.right_index keys < key,
    // where prev_entry is any IndexNodeEntry at the left of current_entry;
    // the leftmost index of an index node is defined separately of this struct
    int key;
    int right_index;
} IndexNodeEntry;
```

### BPlusMeta
Για τα metadata, αποθηκεύονται τα εξής δεδομένα:
```c
typedef struct {
    char magic_num[4]; // identifies the file format
    int block_count; // total number of blocks in the file
    int record_count; // total number of records in the file
    int max_records_per_block; // maximum number of records in a data block
    int max_indexes_per_block; // maximum number of indexes in an index block to its children
    int root_index; // index of the B+ root (index block)
    TableSchema schema; // info for the stored schema (includes record size)
} BPlusMeta;
```
Σημειώνεται ότι το *magic number* που έχει επιλεχθεί για την αναγνώριση του αρχείου είναι κυρίως αυθαίρετα επιλεγμένο. Παρόλα αυτά έχει γίνει τυπικά έλεγχος έτσι ώστε να μην συμπίπτει με άλλα γνωστά *magic numbers* που καθορίζονται από διαδεδομένους τύπους αρχείων.

Τόσο για τα *data blocks* όσο και για τα *index blocks* έχουν οριστεί πολλές συναρτήσεις στα αντίστοιχα αρχεία που βοηθούν στην αφαιρετική χρήση του παραπάνω τρόπου αποθήκευσης. Λεπτομέρειες για την κάθε συνάρτηση υπάρχουν στα αντίστοιχα σχόλια.

### blocks μέσα στο αρχείο
Για την αποθήκευση blocks (οποιουδήποτε είδους) μέσα στο B+ file, ακολουθούνται οι εξής συμβάσεις:
```c
/* The structure of a B+ Tree file is the following:
** (START)[Block0][Block1][Block2]...[BlockN](END) where N is block_count - 1
** - Block0 always contains the BPlusMeta
** - The next blocks can be either data blocks or index blocks and are always appended at the end of the file
** The position (0-based) of each block in the file is defined as its index. Each block stores indexes that 
** act as pointers to other blocks, and these connections shape the B+ Tree. The index of the root can be found in BPlusMeta.
** - When a block has no children or parent, the related indexes are defined to be -1. This is equivalent to NULL pointers.
** - When the B+ tree gets its first record, the root is the data block itself that contains this record. Only **after** this very
** first block has not enough space, does an index block appear as its parent and new root
*/
```

## bplus_create_file
Για την υλοποίηση της `bplus_create_file`, ανοίγει το αρχείο, δημιουργείται το block 0 που θα περιέχει τα metadata, δίνονται τιμές στα metadata και ξανακλείνει το αρχείο.

## bplus_record_insert
Η συνάρτηση αυτή αναλαμβάνει πολύ μεγάλο όγκο λειτουργιών και ήταν αναγκαία η δημιουργία πολλών βοηθητικών συναρτήσεων (δηλωμένες **μόνο** στο bplus_file_funcs.c και όχι στην βιβλιοθήκη .h), έτσι ώστε να μπορεί να δομηθεί πιο οργανωμένα. Οι συναρτήσεις αυτές επικοινωνούν μέσω ενός αντικειμένου context το οποίο κρατά όλες τις μεταβλητές που μοιράζονται οι βοηθητικές συναρτήσεις. Στην συνάρτηση bplus_record_insert (δηλαδή κυρίως στις επιμέρους βοηθητικές συναρτήσεις της), χρησιμοποιούνται και οι διάφορες βοηθητικές συναρτήσεις των data_block και index_block.

Κατά την εκτέλεση της `bplus_record_insert` σε μεγάλα δέντρα εμφανίζεται κάποιες φορές **bug** στην σωστή ενημέρωση της κατάστασης των κόμβων, με αποτέλεσμα σε τέτοιες περιπτώσεις η δομή να μην είναι έγκυρη, με ό,τι αυτό συνεπάγεται. Αυτό συγκεκριμένα παρατηρείται για περίπου 300 εγγραφές και ανω.
Ωστόσο για σχετικά μικρά μεγέθη που η εκτύπωση των κόμβων είναι πρακτική (10 - 20 εγγραφές), η κατάσταση των κόμβων έχει παρατηρηθεί σε πολλές δοκιμές, και τουλάχιστον μέσα σε αυτά τα όρια μεγέθους, η δομή αποθηκεύεται ακριβώς όπως θα έπρεπε.

## bplus_find_record
Η **bplus_find_record** χρησιμοποιείται για την εύρεση κάποιας εγγραφής στο Β+-Δέντρο, με βάση το κλειδί του.

Τα ορίσματα που λαμβάνει είναι τα εξής:
- **file_desc**: Ο περιγραφέας του **B+-Tree** αρχείου στο οποίο θα εφαρμοστεί αναζήτηση.
- **metadata**: Τα μεταδεδομένα του συγκεκριμένου B+-Δέντρου.
- **key**: Το κλειδί της εγγραφής προς αναζήτηση.
- **out_record**: Δείκτης που οδηγεί σε έναν άλλο δείκτη, ο οποίος με την σειρά του οδηγεί στην **διεύθυνση** της αναζητούμενης εγγραφής στην μνήμη. Η συνάρτηση τον θέτει κατάλληλα ώστε να οδηγεί στην εγγραφή που θα βρεθεί (ή τον θέτει ως NULL εάν δεν βρεθεί).

Υλοποίηση:
- Λαμβάνει τα μεταδεδομένα του B+-Tree αρχείου από το Block 0, και τα αποθηκεύει στον δείκτη "**tree_info**". Δεν χρησιμοποιεί τα μεταδεδομένα που δίνονται μέσω του δείκτη "**metadata**", καθώς εκεί περιέχεται ένα **αντίγραφο** του Block 0, αντί για την **διεύθυνση** του Block 0 στην μνήμη.
- Λαμβάνει το Block Index του Block που αποτελεί την ρίζα του Β+-Δέντρου, μέσα από τα μεταδεδομένα που **παραλήφθηκαν προηγουμένως**.
- Χρησιμοποιεί την βοηθητική συνάρτηση "**tree_search_data_block**", η οποία αποθηκεύει στον δείκτη "**res_block**" την **διεύθυνση** του **Block** δεδομένων στο οποίο πρέπει να βρίσκεται η εγγραφή με το επιθυμητό κλειδί. Επιπλέον, λαμβάνει τα μεταδεδομένα αυτού του Block, μέσω της βοηθητικής συνάρτησης "**data_block_read_header**". Πιο συγκεκριμένα, διαβάζει από τα μεταδεδομένα τον αριθμό εγγραφών που έχουν αποθηκευτεί στο αντίστοιχο Block.
- Τρέχει έναν βρόχο **`for`** **από** το 0 **έως** τον αριθμό των αποθηκευμένων εγγραφών, ο οποίος:
	- Λαμβάνει κάθε εγγραφή αποθηκευμένη στο Block, ελέγχει αν το κλειδί της εγγραφής ταιριάζει με το κλειδί που ψάχνουμε, και αν ναι, θέτει τον δείκτη "**out_record**" ώστε να οδηγεί στην συγκεκριμένη εγγραφή. Εκεί τερματίζει η συνάρτηση επιστρέφοντας την τιμή "**0**", εφόσον έχει πετύχει η εύρεση της αναζητούμενης εγγραφής.
	- Στο τέλος κάθε επανάληψης, ο δείκτης "**rec**", που οδηγεί σε κάθε εγγραφή που ελέγχουμε, πρέπει να εκκαθαριστεί με **`free`** ώστε να μην παραμείνουν δεσμευμένα κομμάτια μνήμης τα οποία δεν χρησιμοποιούνται.
- Τέλος, εάν η συνάρτηση δεν έχει επιστρέψει με τιμή "**0**" μέσω του βρόχου **`for`**, τότε επιστρέφει με τιμή "**-1**", καθώς δεν έχει βρεθεί εγγραφή που να αντιστοιχεί στο κλειδί προς αναζήτηση.
