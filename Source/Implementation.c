#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

// We define maximum initial size of a hash table (i.e. a partition) as 512 for partitioning -
#define MAX_HT_SIZE 512
// the log (number of bits) of this is 9.
#define LOG_MAX_HT_SIZE 9

//DEFINE COMMONLY REFERRED SIZES OF DATATYPES
// the size of a long int
#define SIZE_OF_LONG sizeof(long int)
// the size of a bool
#define SIZE_OF_BOOL sizeof(bool)
// size of a subdb struct instance
#define SIZE_OF_SUBDB sizeof(SubDB)
// size of a mainDB struct instance
#define SIZE_OF_MAINDB sizeof(mainDB)
//size of a hash table struct instance
#define SIZE_OF_HASHTABLE sizeof(hash_table)


//The specified API
typedef void* SortMergeJoinDatabase;
SortMergeJoinDatabase SortMergeJoinAllocateDatabase(unsigned long totalNumberOfEdgesInTheEnd);
void SortMergeJoinInsertEdge(SortMergeJoinDatabase database, int fromNodeID, int toNodeID,
                             int edgeLabel);
int SortMergeJoinRunQuery(SortMergeJoinDatabase database, int edgeLabel1, int edgeLabel2,
                          int edgeLabel3);
void SortMergeJoinDeleteEdge(SortMergeJoinDatabase database, int fromNodeID, int toNodeID,
                             int edgeLabel);
void SortMergeJoinDeleteDatabase(SortMergeJoinDatabase database);

typedef void* HashjoinDatabase;
HashjoinDatabase HashjoinAllocateDatabase(unsigned long totalNumberOfEdgesInTheEnd);
void HashjoinInsertEdge(HashjoinDatabase database, int fromNodeID, int toNodeID, int edgeLabel);
int HashjoinRunQuery(HashjoinDatabase database, int edgeLabel1, int edgeLabel2, int edgeLabel3);
void HashjoinDeleteEdge(HashjoinDatabase database, int fromNodeID, int toNodeID, int edgeLabel);
void HashjoinDeleteDatabase(HashjoinDatabase database);

typedef void* ExperimentDatabase;
ExperimentDatabase ExperimentAllocateDatabase(unsigned long totalNumberOfEdgesInTheEnd);
void ExperimentInsertEdge(ExperimentDatabase database, int fromNodeID, int toNodeID,
                           int edgeLabel);
int ExperimentRunQuery(ExperimentDatabase database, int edgeLabel1, int edgeLabel2,
                        int edgeLabel3);
void ExperimentDeleteEdge(ExperimentDatabase database, int fromNodeID, int toNodeID,
                           int edgeLabel);
void ExperimentDeleteDatabase(ExperimentDatabase database);


//-------------------------------- SUB DATABASE ---------------------------------------------
//-------------------------------------------------------------------------------------------
//A subdb is generated from the main database. It always has two columns - left and right. It is sortable on
//either column, and can be joined with other subdbs along opposite columns (i.e. left->right or right->left)
//to produce a result subdb. Generating a subdb from the main database is equivalent to performing a select
//based on label = a value, on a projection over 2 chosen columns. There is no limit on number of rows in a SubDB.

//Here, even though we have two columns, we use a linear array and calculate the array indices from 2D indices (2*row + column).

// Define a sub database struct called SubDB - this will be used to perform joins.
typedef struct s{
    // Pointer to storage array.
    long int* table;
    // variable to keep track of allocated row capacity of table.
    size_t capacity;
    // variable to track number of rows inserted.
    size_t filled;
} SubDB;

//----- FUNCTIONS:------------------------------------------------------------------------

//allocate space for a sub-database (like a constructor), specifying number of rows to allocate.
void allocate_subdb(SubDB* s, size_t rows)
{
  // allocate space for SubDB (each row is two long ints)
  s->table = (long int*)malloc(rows*2*SIZE_OF_LONG);
  // Check if allocation was successful.
  if(s->table == NULL)
  {
    printf("error: malloc of SubDB failed.\n");
  }
  else
  {
    //update capacity and filled
    s->capacity = rows;
    s->filled = 0;
  }
}

//---------- SORTING UTILITY FUNCTIONS (QUICK SORT) ----------------------------

// Used to determine partition for quicksort -  parameter bool along_left chooses whether
// sorting is of left (set to true) or right column (set to false).
// 'low' and 'high' denote the range of row indices that we are going to sort.

// We use pointer arithmetic here as it's easy to understand and faster.
long int partition(long int* arr, long int low, long int high, bool along_left){
    //If along left is selected (i.e. sorting along left column), then the column index (i.e. offset) is 0, otherwise 1.
    int offset = along_left? 0 : 1;
    // the row of the pivot is the middle row between low and high rows i.e. (low + high)/2.
    size_t pivot_row_index = (low+high)>>1;
    //The pivot is the left/right element (based on along_left) in the pivot row.
    long int pivot = *(arr + 2*pivot_row_index + offset);
    //From now we operate on pointers to the underlying 1D array, so we must multiply and
    //increment row indices by 2 (i.e. the column size).
    long int* i = arr + ((low - 1)<<1) + offset;
    long int* j = arr + ((high + 1)<<1) + offset;
    // Essentially we want to go on looping until all elements are on the correct side of the pivot
    while(true)
    {
      // loop from below until we get an element >=  pivot
      do {
        i+=2;
      } while(*(i) < pivot);
      // loop from above until we get an element <= pivot
      do {
        j-=2;
      } while(*(j) > pivot);
      //if i >= j our work is done and we return row index corresponding to j, as partition
      // (i.e. distance of j from array start/2)
      if(i >= j)
      {
        return ((j - arr)>>1);
      }
      // else swap rows at indices i and j to ensure the pivot rule is repected.
     //Calculate pointers to elements to be swapped.
     long int* j_left = j - offset;   long int*  j_right = j_left + 1;
     long int* i_left = i - offset;   long int*  i_right = i_left + 1;
     //temporary store
     long int temp_left = 0, temp_right = 0;
     // store row at j in temporary variables
     temp_right = *(j_right);  temp_left = *(j_left);
     //store row at i in position j.
     *(j_right) = *(i_right); *(j_left) = *(i_left);
     // move temporary variables into row at i
     *(i_right) = temp_right;  *(i_left) = temp_left;
    }
}

// Sort array using recursive quicksort and partition
void sort(long int* arr, long int low, long int high, bool along_left){
    if(low < high){
        size_t index = partition(arr, low, high, along_left);
        sort(arr, low, index, along_left);
        sort(arr, index+1, high, along_left);
    }
}

//sort table using the above functions, accepting pointer to the relevant SubDB.
void sort_table_subdb(SubDB* s, bool along_left){
  size_t size = s->filled;
  //sort as far as elements have been filled into array, no further.
  sort(s->table, 0, size-1, along_left);
};

//------------------------------------------------------------------------------

//deallocate the sub-database (like a destructor)
void deallocate_subdb(SubDB* s)
{
  free(s->table);
  //update other struct values for consistency
  s->capacity = 0; s->filled = 0;
}

//this function allocates a new table of double the size and copies over original elements.
void reallocate_subdb(SubDB* s)
{
  //Here we just want the total number of allocated elements in the original table (rows * 2)
  size_t table_1D_size = (s->capacity)<<1;
  //define new capacity (double of old capacity) and reallocate.
  size_t capacity = 2*table_1D_size*SIZE_OF_LONG;
  long int* new_tbl = (long int*)malloc(capacity);
  //store pointer to old table:
  long int* old_tbl = s->table;
  //copy elements from old table into new one.
  for(size_t i = 0; i < table_1D_size; i++)
  {
    *(new_tbl + i) = *(old_tbl + i);
  }
  //free old table memory
  free(old_tbl);
  //transfer s->table pointer to new table
  s->table = new_tbl;
  //update values (capacity has doubled)
  s->capacity <<= 1;
}

//insert element into subdb. Call reallocate function above if capacity filled.
void insert_subdb(SubDB* s, long int l, long int r)
{
  //reallocate subdb if we are filled to capacity
  if(s->filled == s->capacity)
  {
    reallocate_subdb(s);
  }
  //the row index to write to is given by s->filled. To write to SubDB array with 2
  //elements per row, multiply row index by 2.
  // We directly calculate pointer to row where we need to insert.
  long int* row_pointer = ( s->table + ((s->filled)<<1) );
  // left and right values being added.
  *(row_pointer) = l; *(row_pointer + 1) = r;
  //increment filled
  s->filled += 1;
}

// access left element at a given row position in the subdb
long int access_left_subdb(const SubDB* s, size_t pos)
{
  if(pos < s->filled)
  {
    return *( s->table + (pos<<1) );
  }
  return 0;
}

// access right element at a given row position in subdb
long int access_right_subdb(const SubDB* s, size_t pos)
{
  if(pos < s->filled)
  {
    return *(s->table + (pos<<1) + 1);
  }
  return 0;
}

// This function is useful to avoid the third join because after the second join we just
// need to count how many of the rows have left and right equal.
size_t check_equal_lr_subdb(const SubDB* s){
  size_t count = 0;
  // store pointer to end of table
  long int* table_end = s->table + (s->filled << 1);
  //iterate over rows of the table.
  for(long int* row = s->table; row != table_end; row+=2){
    // if left and right equal increment count
    if(*(row) == *(row + 1)){
      count++;
    }
  }
  return count;
}
//--------------------------------- END OF SUBDB -----------------------------------------------------------
//----------------------------------------------------------------------------------------------------------



//--------------------------------- MAIN DATABASE --------------------------------------------------------
//--------------------------------------------------------------------------------------------------------
//This is a container for the actual database which will store all the labelled edges. It can be inserted,
//deleted and queried. It can dynamically reallocate to grow. Deletion is through marking elements deleted,
//and a cleanup of deleted elements occurs when a threshold is crossed. Finally, for joins and selects, a SubDB
//can be created on specific columns based on equality with the label column.

//The rows are stored in NSM. There is also a separate array, adjacent in memory, to record which rows are deleted.

// define structure for main database.
typedef struct db
{
  // Define pointer to dynamically allocated array to hold table.
  long int* table;
  // Define pointer to array that marks rows as deleted.
  bool* deleted;
  // Define size of a tuple i.e. number of columns
  int tuple_size;
  // Keep count of number of filled rows in table (whether marked deleted or not)
  size_t used_rows;
  // Store current capacity of db (i.e. size of the "deleted" array defined above) - the
  // maximum provision of rows before a reallocation is required.
  size_t db_capacity;
  // Keep count of number of rows marked for delete in table
  size_t no_deleted;

} MainDB;

// find how many undeleted filled rows we have in the DB
size_t undeleted_rows_maindb(const MainDB* m)
{
  return m->used_rows - m->no_deleted;
}

// allocate memory for main database, specifying intended row & column capacity as an input parameter.
// Setup struct values (like a constructor).
void allocate_maindb(MainDB* m, size_t rows, size_t columns)
{
  //set values based on provided info
  m->used_rows = 0;
  m->db_capacity = rows;
  m->tuple_size = columns;
  // allocate table and delete array in continuous memory block.
  size_t memsize = rows*SIZE_OF_LONG*columns + rows*SIZE_OF_BOOL;
  //m->table points to beginning of memory block of size = rows*columns long ints
  m->table = (long int*)malloc(memsize);
  // to flag an unsuccessful allocation
  if(m->table == NULL)
  {
    printf("error: malloc of MainDB table failed.\n");
  }
  // no tuples are deleted so far so set to zero.
  m->no_deleted = 0;
  //m->deleted points after m->table ends in the above memory block.
  //This is where the deleted array begins.
  m->deleted = (bool*)(m->table + rows*columns);
}

//deallocate memory for database (destructor)
void deallocate_maindb(MainDB* m)
{
  // free the memory block.
  free(m->table);
  //update values for consistency.
  m->used_rows = 0; m->db_capacity = 0; m->no_deleted = 0;
}

//used to double capacity of DB and copy over original elements - called if
//the DB is filled to capacity.
void reallocate_maindb(MainDB* m)
{
  // store regularly referred variables locally.
  size_t oldsize = m->db_capacity;
  size_t tuplesize = m->tuple_size;
  // define new size (double of old size)
  size_t newsize = oldsize<<1;
  // allocate new continuous memory block.
  size_t memsize = newsize*SIZE_OF_LONG*tuplesize + newsize*SIZE_OF_BOOL;
  long int* new_tbl = (long int*)malloc(memsize);
  //store pointer to old table
  long int* old_tbl = m->table;
  //pointers to new and old delete recording arrays
  bool* new_del = (bool*)(new_tbl + newsize*tuplesize);
  bool* old_del = m->deleted;
  //copy elements of old array and delete recorder to new array
  for(size_t i = 0; i < oldsize; i++)
  {
    size_t v = i*tuplesize;

    *(new_tbl + v) = *(old_tbl + v);
    *(new_tbl + v + 1) = *(old_tbl + v + 1);
    *(new_tbl + v + 2) = *(old_tbl + v + 2);

    *(new_del + i) = *(old_del + i);
  }
  //free old array
  free(old_tbl);
  //set table pointer of DB to new larger array
  m->table = new_tbl;
  //set delete recording array pointer of DB to new larger array
  m->deleted = new_del;
  //---------------------------------------------
  //update new capacity of DB (double old capacity)
  m->db_capacity = newsize;
}

//remove all elements marked for delete when called - DB capacity not modified.
void clean_maindb(MainDB* m)
{
  //store the following to avoid repeated references.
  size_t used_rows = m->used_rows;
  size_t tuplesize = m->tuple_size;
  size_t capacity = m->db_capacity;
  //allocate a "copy" of old array - same memory size.
  long int* new_tbl = (long int*)malloc( capacity*SIZE_OF_LONG*tuplesize + capacity*SIZE_OF_BOOL );
  long int* old_tbl = m->table;
  //pointer to delete recording section of new table
  bool* new_del = (bool*)(new_tbl + capacity*tuplesize);
  // delete recording section of old table
  bool* old_del = m->deleted;
  //copy undeleted rows into new array.
  size_t j = 0;
  for(size_t i = 0; i < used_rows; i++)
  {
    //check if deleted is set to false
    if( *(old_del + i) == false)
    {
      size_t vj = j*tuplesize;
      size_t vi = i*tuplesize;
      // move elements into new memory area
      *(new_tbl + vj) = *(old_tbl + vi);
      *(new_tbl + vj +1) = *(old_tbl + vi + 1);
      *(new_tbl + vj +2) = *(old_tbl + vi + 2);
      j++;
    }
    //we just need to set all rows as undeleted.
    *(new_del + i) = false;
  }
  //delete old array
  free(old_tbl);
  //set database table pointer to new array
  m->table = new_tbl;
  //Set m->deleted to new delete pointer
  m->deleted = new_del;
  //-------------------------------------------
  // update values to account for removed rows.
  m->used_rows -= m->no_deleted;
  m->no_deleted = 0;
}

//insert new edge into the db - call reallocation function if filled to capacity.
void insert_maindb(MainDB* m, int fromNode,  int toNode, int edgeLabel)
{
  // reallocate if filled to capacity.
  if(m->used_rows == m->db_capacity)
  {
    reallocate_maindb(m);
  }
  size_t index = m->used_rows;
  int ts = m->tuple_size;
  // store table pointer to avoid repeated pointer references to table.
  long int* tbl = m->table;
  //insert into table new data.
  *(tbl + index*ts) = fromNode;
  *(tbl + index*ts + 1) = toNode;
  *(tbl + index*ts + 2) = edgeLabel;
  //insert into delete marker the info that new tuple is undeleted.
  *(m->deleted + index) = false;
  //increment no of filled rows
  m->used_rows += 1;
}

//mark as deleted in the db given the edge that has the given nodes and label.
void delete_maindb(MainDB* m, long int fromNode, long int toNode, long int edgeLabel)
{
  // store pointer references locally
  int tuplesize = m->tuple_size;
  size_t tablesize = (m->used_rows)*tuplesize;
  //store data pointers locally.
  long int* table = m->table;
  bool* deleted = m->deleted;
  //iterate over table array - treating it as 1D.
  size_t i = 0;
  while(i < tablesize){
      if(( *(table + i) == fromNode) && ( *(table + i + 1) == toNode) && ( *(table + i + 2) == edgeLabel)){ //finding the matching edge(s)
          // Since the "deleted" vector has one entry per row, we have to divide i by tuple size to obtain the relevant index.
          size_t index_in_deleted = i/tuplesize;
          // if not deleted, mark as deleted.
          if( *(deleted + index_in_deleted) == false)
          {
            *(deleted + index_in_deleted) = true;
            m->no_deleted += 1;
          }
      }
      i += tuplesize; //go on to the next edge to check if it needs to be deleted
  }

  //Threshold set at 50%. If > 50% elements deleted, initiate a cleanup of delete-marked elements.
  if( ( (m->no_deleted)<<1 ) > m->used_rows)
  {
    clean_maindb(m);
  }
}

// Create SubDB, specifying which MainDB columns go to left and right columns. Also specify an edge label
//so that we can perform an equality-based select.
SubDB* create_subdb(const MainDB* m, int column_id_left, int column_id_right, long int edgeLabel){
  // At most, size of SubDB equals number of non-deleted rows in the DB. This is how much we allocate.
  size_t subdb_size = undeleted_rows_maindb(m);
  SubDB* s = (SubDB*)malloc(SIZE_OF_SUBDB);
  allocate_subdb(s, subdb_size);
  //store locally to avoid repeated pointer reference
  size_t used_rows = m->used_rows;
  size_t tuple_size = m->tuple_size;
  long int* table = m->table;
  bool* deleted = m->deleted;
  //iterate over available rows in the DB (includes deleted rows as we can't distinguish these beforehand).
  for(size_t i = 0; i < used_rows; i++){
    size_t v = i*tuple_size;
    // condition for adding to subdb - equality with edge label and undeleted row.
    if(( *(table + v + 2) == edgeLabel) && ( *(deleted + i) == false))
    {
      long int left = *(table + v + column_id_left);
      long int right = *(table + v + column_id_right);
      //insert into subdb
      insert_subdb(s, left, right);
    }
  }
  return s;
}

//Can access any element in the DB given the row and column indices.
long int access_element_maindb(const MainDB* m, size_t row, size_t column)
{
  // in case out of range return 0
  if((row >= m->used_rows)||(column >= m->tuple_size))
  {
    return 0;
  }
  return *(m->table + (m->tuple_size)*row + column);
}

//--------------------------------END OF MAINDB-----------------------------------------------
//--------------------------------------------------------------------------------------------

//----------------------------------HASH TABLE------------------------------------------------
//--------------------------------------------------------------------------------------------
//The hash table required for a hash-join. For the hash function, we use the finaliser of murmur3 hash function.
//Linear probing is used.

//Like a SubDB, a hash table can have unlimited rows but two columns - left (offset from row = 0) and right (offset from row = 1).
//The left column has a long int value that is hashed/searched/joined on. The right column has an associated long int value
//useful for subsequent joins (e.g. the two values can have a from-to relationship on an edge).

//define hash table container
typedef struct ht{
  // To point to dynamically allocated hash table memory.
  long int* table;
  // bool array to indicate whether table position is occupied (like a metadata for the hash table).
  //This is stored in an adjacent memory location to table.
  bool* occupied;
  // size of allocated hash table.
  size_t size;
  // indicates how many of the hash table entries have been occupied.
  size_t fill_factor;
} hash_table;

//Uses murmur3 64-bit hash function finaliser to effectively distribute across
//the hash table (v is the value to be hashed)

// SOURCE: https://github.com/aappleby/smhasher
size_t hashfunction_ht(const hash_table* ht, long int v)
{
  v += 1ULL;
  v ^= v>>33ULL;
  v *= 0xFF51AFD7ED558CCDULL;
  v ^= v>>33ULL;
  v *= 0xC4CEB9FE1A85EC53ULL;
  v ^= v>>33ULL;
  //modulo with hash table size
  return v&(ht->size - 1);
}

//The function used to allocate the hash table (constructor). Required size is specified.
void allocate_ht(hash_table* ht, size_t size)
{
   //The actual size we allocate is the smallest power of 2 >= size. This
   //makes it faster to perform hashes by using bitwise operators instead of modulo.
   size_t p2_size = 1;
   while(p2_size < size)
   {
     p2_size = p2_size << 1;
   }
   //Set values
   ht->size = p2_size;
   ht->fill_factor = 0;
   // allocate space for hash table + occupancy indicator array (2 entries per row gives the 2)
   size_t memsize = p2_size*2*SIZE_OF_LONG + p2_size*SIZE_OF_BOOL;
   ht->table = (long int*)malloc(memsize);
   //flag a failed allocation
   if(ht->table == NULL)
   {
     printf("error: malloc of hash table memory failed.\n");
   }
   // Occupied array starts where the hash table array ends. Set pointer accordingly.
   bool* occupied = (bool*)( ht->table + (p2_size<<1) );
   //mark all elements as unoccupied
   bool* occupied_end = occupied + p2_size;
   for(bool* b = occupied; b != occupied_end; b++)
   {
     *(b) = false;
   }
   //set ht->occupied to the above pointer 'occupied'
   ht->occupied = occupied;
}


//deallocate hash table (destructor)
void deallocate_ht(hash_table* ht)
{
  //free the memory
  free(ht->table);
  // update values for consistency.
  ht->size = 0; ht->fill_factor = 0;
}

// reallocate the hash table in case we want to avoid collisions, by doubling size.
void reallocate_space_ht(hash_table* ht)
{
 //store important variables.
 size_t fillfactor = ht->fill_factor;
 size_t oldsize = ht->size;
 size_t newsize = oldsize<<1;

 //now set up hash table and occupied indicator of new size
 size_t memsize = newsize*2*SIZE_OF_LONG + newsize*SIZE_OF_BOOL;
 long int* new_tbl = (long int*)malloc(memsize);
 //set the pointer to the new occupancy indicating array
 bool* new_occ = (bool*)(new_tbl + (newsize<<1));

 // Set new hash table to all unoccupied before reentering previously existing elements.
 bool* occupied_end = new_occ + newsize;
 for(bool* b = new_occ; b != occupied_end; b++)
 {
   *(b) = false;
 }

 //Need to set new hash table size value earlier since hash function is based on this.
 ht->size = newsize;

 // Store pointers to old table and occupied arrays to avoid repeated pointer references.
 long int* old_tbl = ht->table;
 bool* old_occ = ht->occupied;

 // Insert previously existing elements back into table based on new hash function and linear probing.
 // We iterate over each row of the old table.
 for(size_t row = 0; row < oldsize; row++)
 {
   // Used to access array of old hash table as a 1D array (array index = row*2 + column)
   size_t tbl_1D_index = row<<1;
   // Check if entry in old hash table exists. In this case we should put it in the new table.
   if( *(old_occ + row) == true )
   {
     //This variable is used to access different rows in the new table during the insertion probing.
     // It is initialised to the hash of the left column value in the current row.
     size_t row_position = hashfunction_ht( ht, *(old_tbl + tbl_1D_index) );
     // Used in a faster bitwise version of modulus with tablesize i.e. like %tablesize but faster.
     size_t bitmask = newsize - 1;
     // Insert using linear probing - we probe over the rows of the table.
     while( *(new_occ + row_position) == true )
     {
       row_position = (row_position + 1)&bitmask;
     }
     //To insert in actual table array, we need to multiply row_position by 2
     size_t tbl_1D_insert = row_position<<1;
     //Put element in
     //right column:
     *(new_tbl + tbl_1D_insert + 1) = *(old_tbl + tbl_1D_index + 1);
     //left column:
     *(new_tbl + tbl_1D_insert) = *(old_tbl + tbl_1D_index);
     //set occupied:
     *(new_occ + row_position) = true;
   }
 }
 //set h->table pointer to new_tbl, h->occupied to new_occ, freeing the old memory.
 free(old_tbl);
 ht->occupied = new_occ;
 ht->table = new_tbl;
}


//insert value into hash table using hashing/linear probing
// value = the left-column value that is hashed/probed; sec_val = right column additional value.
void insert_value_ht(hash_table* ht, long int value, long int sec_val)
{
  // store size of the hash table to avoid repeatedly referring to the pointer.
  size_t tablesize = ht->size;
  //reallocate table if our fill factor > 50% of table size.
  if( ( (ht->fill_factor)<<1 ) > tablesize )
  {
    reallocate_space_ht(ht);
    // double tablesize variable to reflect doubled size
    tablesize << 1;
  }
  //This variable is used to access different elements in the table during the insertion probing.
  // It is initialised to the hash of parameter 'value'.
  size_t row_position = hashfunction_ht(ht, value);
  // Used in a faster version of modulus with tablesize i.e. %tablesize but faster.
  size_t bitmask = tablesize - 1;
  // Store pointer to hash table to avoid repeated references.
  long int* table = ht->table;
  bool* occupied = ht->occupied;
  //Probe hash table rows in the 'occupied' array until we find an unoccupied space.
  while( *(occupied + row_position) == true )
  {
    //implement linear probing (the & operation is equivalent to modulus with table size).
    row_position = (row_position + 1)&bitmask;
  }
  //To insert in table, we need to multiply row id by 2
  size_t tbl_1D_insert = row_position<<1;
  //Put elements in -
  //right column
  *(table + tbl_1D_insert + 1) = sec_val;
  //left column
  *(table + tbl_1D_insert) = value;
  //mark as occupied
  *(occupied + row_position) = true;
  // increment fill factor of hash table
  ht->fill_factor++;
}

//Returns the element's left-column value at the specified row index in the table.
//The left column is the value used in hashing/searching/joining.
long int access_value_ht(const hash_table* ht, size_t hashvalue)
{
  // return value
  return *( ht->table + (hashvalue<<1) );
}

//Returns the element's right-column value at the specified row index.
//The right column value is additional to the hashed value on the left.
long int access_secondary_value_ht(const hash_table* ht, size_t hashvalue)
{
  // return value
  return *( ht->table + (hashvalue<<1) + 1 );
}

//Determines whether the row position in hash table value is occupied or not.
bool occupied_ht(const hash_table* ht, size_t hashvalue)
{
  // return value
  return *(ht->occupied + hashvalue);
}

//--------------------------------HASH TABLE END-------------------------------------
//-----------------------------------------------------------------------------------


//--------------------------HASH-JOIN IMPLEMENTATION-----------------------------------------------
//Hash-joins (equi-join) two SubDBs along the right column of the left subDB and left column of the right SubDB.
//Result: SubDB with left column of left input, right column of right input SubDB.

SubDB* hash_join(const SubDB* left, const SubDB* right){
    //to hold the result that is returned.
    SubDB* output = (SubDB*)malloc(SIZE_OF_SUBDB);
    //define indices to iterate over input SubDB's - on build and probe side.
    size_t build_index = 0, probe_index = 0;

    // We want to make the smaller SubDB the build side.
    bool left_smaller = left->filled < right->filled;

    // Pointers to denote build and probe sides.
    const SubDB* build = left_smaller? left : right;
    const SubDB* probe = left_smaller? right : left;

    // To hold sizes of build and probe sides.
    size_t build_size = build->filled, probe_size = probe->filled;

    // Function pointers to access elements in join and non-join columns of each SubDB - these may be left or right columns and vary
    // in build and probe side based on which SubDB is smaller - hence need for polymorphism.
    long int (*select_fn_1)(const SubDB* a, size_t index) = left_smaller? access_right_subdb : access_left_subdb;
    long int (*select_fn_2)(const SubDB* a, size_t index) = left_smaller? access_left_subdb : access_right_subdb;

    // we allocate at least twice the number of elements in the build side for the hash table.
    // If build side has no elements, just allocate a small value to size.
    size_t ht_size = build_size > 0? 2 * build_size: 4;

    // For purposes of potential partitioning, we find the smallest power of 2 >= ht_size
    size_t p2_size = 1;
    while(p2_size < ht_size)
    {
      p2_size = p2_size << 1;
    }

    // Define an array of hash table pointers to hash tables that will act as partitions.
    hash_table** partitions;
    // define bitmask for probing a partition
    size_t partition_bitmask;

    //The number of partitions can then be found by a bitwise right shift (comparing with maximum initial hash table size).
    size_t no_of_partitions = p2_size >> LOG_MAX_HT_SIZE;

    // Even if we are below the maximum size, we still need to allocate an array of size 1 for the single hash table.
    if(no_of_partitions == 0)
    {
      partitions = (hash_table**)malloc(sizeof(hash_table*));
      hash_table* htemp = (hash_table*)malloc(SIZE_OF_HASHTABLE);
      allocate_ht(htemp, p2_size);
      partitions[0] = htemp;
      // Define number of partitions as 1 because we have 1 partition now, even if it's smaller.
      no_of_partitions = 1;
      partition_bitmask = p2_size - 1;
    }
    else
    {
      //One entry in array for each partition.
      partitions = (hash_table**)malloc(sizeof(hash_table*)*no_of_partitions);
      //Initialise each partition
      for(size_t i = 0; i < no_of_partitions; i++)
      {
        hash_table* htemp = (hash_table*)malloc(SIZE_OF_HASHTABLE);
        //size of each hash table is maximum allowed size for a hash table
        allocate_ht(htemp, MAX_HT_SIZE);
        partitions[i] = htemp;
      }
      partition_bitmask = MAX_HT_SIZE - 1;
    }

   //Will be used to decide which partition to assign value (bitwise modulus).
   size_t initial_bitmask = no_of_partitions - 1;

     //building hashtable on build side
    while(build_index < build_size){
        //obtain values to insert from build side.
        long int value = (*select_fn_1)(build, build_index);
        long int sec_val = (*select_fn_2)(build, build_index);
        // Initially decide the partition
        hash_table* h = partitions[value&initial_bitmask];

        //insert into the partition
        insert_value_ht(h, value, sec_val);
        build_index++;
    }

    //-------------------------------------------------------------------------
    //based on size of build side above, allocate output subDB.
    allocate_subdb(output, build_size);

    // -------------------------------------------------------------------------
    //probe the hashtable from probe (i.e. right) side to find joins
    while(probe_index < probe_size){
        // store the value to be searched
        long int value = (*select_fn_2)(probe, probe_index);

        // Find the partition to search.
        hash_table* h = partitions[value&initial_bitmask];

        // This variable is used to actually probe the table by accessing elements.
        // We initialise it to hash of join value
        size_t row_position = hashfunction_ht(h, value);

        //We go on iterating until we find an empty space as there may be more than
        //one instances of the required value.
        while(occupied_ht(h, row_position))
        {
          //if we obtain an equality, then add to our output.
          if(access_value_ht(h, row_position) == value)
          {
            // Based on which sides are build and probe we add to output differently
            if(left_smaller)
            {
              // Non-joined value of left (build) subdb goes in left column of output subdb. Non-joined value of right (probe) goes in right column of output.
              insert_subdb(output, access_secondary_value_ht(h, row_position), (*select_fn_1)(probe, probe_index));
            }
            else
            {
              // Non-joined value of left (probe) subdb goes in left column of output subdb. Non-joined value of right (build) goes in right column of output.
              insert_subdb(output, (*select_fn_1)(probe, probe_index), access_secondary_value_ht(h, row_position));
            }
          }
          //increment postion according to linear probing.
          row_position = (row_position + 1)&partition_bitmask;
        }
        probe_index++;
    }

    // deallocate all partitions.
    hash_table** partitions_end = partitions + no_of_partitions;
    for(hash_table** i = partitions; i != partitions_end; i++)
    {
      deallocate_ht(*i);
      free(*i);
    }
    // deallocate partitions array
    free(partitions);
    return output;
}

//------------------------------END OF HASH-JOIN------------------------------------------


//-------------------------------SORT-MERGE JOIN IMPLEMENTATION---------------------------
//Implements sort-merge join on two sorted SubDB's - right column of left SubDB equi-joined with left
//column of right SubDB to give a result SubDB in which: left column -> left column of left SUbDB, right column
//-> right column of right SubDB.

SubDB* sort_merge(SubDB* left, SubDB* right)
{
  //to hold result
  SubDB* output = (SubDB*)malloc(SIZE_OF_SUBDB);

  //define indices to iterate over SubDB's
  size_t left_index = 0, right_index = 0;

  //store sizes of each sub DB
  size_t l_size = left->filled, r_size = right->filled;

  //based on sizes above, allocate output subDB. Choose l_size by default.
  allocate_subdb(output, l_size);

  //iterate over SUbDB's
  while((left_index < l_size)&&(right_index < r_size))
  {
    long int l_value = access_right_subdb(left, left_index), r_value = access_left_subdb(right, right_index);

    // if left is less move left index forward.
    if(l_value < r_value)
    {
      while(access_right_subdb(left, left_index) == l_value)
      {
        left_index++;
        //leave loop if we overshoot the size of left sub DB
        if(left_index == l_size)
        { break; }
      }
    }
    // if right is less move right index forward
    else if(r_value < l_value)
    {
      while(access_left_subdb(right, right_index) == r_value)
      {
        right_index++;
        //leave loop if we overshoot the size of left sub DB
        if(right_index == r_size)
        { break; }
      }
    }
    // final case is if both are equal - double loop to capture all possible value matches
    // (in other words, cartesian product of matching left and right).
    else
    {
      // store the current value of l_value = r_value, to refer to later.
      long int val = l_value;
      // store initial value of right_index
      size_t right_initial = right_index;
      //iterate over left to perform cartesian product
      while(l_value == val)
      {
        //set right index and right value back to initial value
        right_index = right_initial;
        r_value = val;
        // iterate over right
        while(r_value == val)
        {
          //add values to output
          insert_subdb(output, access_left_subdb(left, left_index), access_right_subdb(right, right_index));

          right_index++;
          // break if we reach the end of the data structure
          if(right_index == r_size) { break; }
          //else update rvalue for next iteration
          r_value = access_left_subdb(right, right_index);
        }
        left_index++;
        // break if we reach the end of the data structure
        if(left_index == l_size) { break; }
        //else update lvalue for next iteration
        l_value = access_right_subdb(left, left_index);
      }
    }
  }
  return output;
}

//------------------------------SORT MERGE JOIN END-------------------------------------------



//-------------------------------------------------------------------------------------------------------------------------------
//------------------------------------------INTERFACE IMPLEMENTATION-------------------------------------------------------------

//-------------SORT-MERGE-------------------------------------------------------

SortMergeJoinDatabase SortMergeJoinAllocateDatabase(unsigned long totalNumberOfEdgesInTheEnd)
{
  //Allocate space for MainDB struct instance
  SortMergeJoinDatabase s = malloc(sizeof(MainDB));
  MainDB* m = (MainDB*)s; //implicit type conversion allowed by C but not by cmake environment.
  //Allocate table array etc, within mainDB.
  int columns = 3;
  allocate_maindb(m, totalNumberOfEdgesInTheEnd, columns);
  //return pointer
  return s;
}

void SortMergeJoinInsertEdge(SortMergeJoinDatabase database, int fromNodeID, int toNodeID, int edgeLabel)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //insert (same API as SortMergeJoinInsertEdge)
  insert_maindb(m, fromNodeID, toNodeID, edgeLabel);
}

int SortMergeJoinRunQuery(SortMergeJoinDatabase database, int edgeLabel1, int edgeLabel2, int edgeLabel3)
{
  //implicit type conversion allowed by C but not by given cmake environment.
  MainDB* m = (MainDB*)database;
  // Create subDBs based on edgeLabels 1 and 2
  SubDB* s1 = create_subdb(m, 0, 1, edgeLabel1);
  SubDB* s2 = create_subdb(m, 0, 1, edgeLabel2);
  // sort SubDB s1 along right column
  sort_table_subdb(s1, false);
  // sort SubDB s2 along left column
  sort_table_subdb(s2, true);
  // Perform sort-merge number 1
  SubDB* sr12 = sort_merge(s1, s2);
  // deallocate subDB's that are no longer needed after first join.
  deallocate_subdb(s1); free(s1);
  deallocate_subdb(s2); free(s2);
  // Create SubDB based on third edgeLabel
  SubDB* s3 = create_subdb(m, 0, 1, edgeLabel3);
  // Sort SubDB sr12 along right column
  sort_table_subdb(sr12, false);
  // Sort SubDB s3 along left column
  sort_table_subdb(s3, true);
  // Perform sort-merge number 2
  SubDB* out = sort_merge(sr12, s3);
  // deallocate sr12 and sr3 SubDB's - they are not needed.
  deallocate_subdb(sr12); free(sr12);
  deallocate_subdb(s3); free(s3);
  //Count equal left-right columns in 'out' - faster than a third join.
  size_t result = check_equal_lr_subdb(out);
  //free SubDB memory of 'out'
  deallocate_subdb(out); free(out);

  //return
  return result;
}

void SortMergeJoinDeleteEdge(SortMergeJoinDatabase database, int fromNodeID, int toNodeID, int edgeLabel)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //delete (same API as SortMergeJoinDeleteEdge)
  delete_maindb(m, fromNodeID, toNodeID, edgeLabel);
}

void SortMergeJoinDeleteDatabase(SortMergeJoinDatabase database)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //deallocate table array etc, within the MainDB. Set all size values to zero.
  deallocate_maindb(m);
  //free memory space occupied by struct instance.
  free(database);
}


//-----------------------------HASH-JOIN----------------------------------------

HashjoinDatabase HashjoinAllocateDatabase(unsigned long totalNumberOfEdgesInTheEnd)
{
  //Allocate space for MainDB struct instance
  HashjoinDatabase h = malloc(sizeof(MainDB));
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)h;
  //Allocate space for table array etc within MainDB instance.
  int columns = 3;
  allocate_maindb(m, totalNumberOfEdgesInTheEnd, columns);
  //Return pointer.
  return h;
}

void HashjoinInsertEdge(HashjoinDatabase database, int fromNodeID, int toNodeID, int edgeLabel)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //Insert (same API as HashjoinInsertEdge)
  insert_maindb(m, fromNodeID, toNodeID, edgeLabel);
}

int HashjoinRunQuery(HashjoinDatabase database, int edgeLabel1, int edgeLabel2, int edgeLabel3)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //Create subDBs based on labels 1 and 2
  SubDB* s1 = create_subdb(m, 0, 1, edgeLabel1);
  SubDB* s2 = create_subdb(m, 0, 1, edgeLabel2);
  //Perform first hash join
  SubDB* sr12 = hash_join(s1, s2);
  //deallocate unneeded subDBs
  deallocate_subdb(s1); free(s1);
  deallocate_subdb(s2); free(s2);
  //Create third and final subDB
  SubDB* s3 = create_subdb(m, 0, 1, edgeLabel3);
  //Perform second hash join - we now have info from all tables
  SubDB* out = hash_join(sr12, s3);
  //deallocate sr12 and s3 SubDB's
  deallocate_subdb(sr12); free(sr12);
  deallocate_subdb(s3); free(s3);
  //Count equal left-right columns on out- faster than a third join.
  size_t result = check_equal_lr_subdb(out);
  //deallocate out SubDB
  deallocate_subdb(out); free(out);

  //return
  return result;
}

void HashjoinDeleteEdge(HashjoinDatabase database, int fromNodeID, int toNodeID, int edgeLabel)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //Delete (same API as HashjoinDeleteEdge)
  delete_maindb(m, fromNodeID, toNodeID, edgeLabel);
}

void HashjoinDeleteDatabase(HashjoinDatabase database)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //deallocate table array and other data structures within MainDB. Set all size vales to zero.
  deallocate_maindb(m);
  //free memory occupied by struct instance.
  free(database);
}

//-------------------------EXPERIMENT (CURRENTLY USES HASH JOIN)------------------------------------------

ExperimentDatabase ExperimentAllocateDatabase(unsigned long totalNumberOfEdgesInTheEnd)
{
  //Allocate space for MainDB struct instance
  ExperimentDatabase c = malloc(sizeof(MainDB));
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)c;
  //Allocate space for table array etc within MainDB instance.
  int columns = 3;
  allocate_maindb(m, totalNumberOfEdgesInTheEnd, columns);
  //Return pointer.
  return c;
}

void ExperimentInsertEdge(ExperimentDatabase database, int fromNodeID, int toNodeID, int edgeLabel)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //Insert (same API as ExperimentInsertEdge)
  insert_maindb(m, fromNodeID, toNodeID, edgeLabel);
}

int ExperimentRunQuery(ExperimentDatabase database, int edgeLabel1, int edgeLabel2, int edgeLabel3)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //Create subDBs based on labels 1 and 2
  SubDB* s1 = create_subdb(m, 0, 1, edgeLabel1);
  SubDB* s2 = create_subdb(m, 0, 1, edgeLabel2);
  //Perform first hash join
  SubDB* sr12 = hash_join(s1, s2);
  //deallocate unneeded subDBs
  deallocate_subdb(s1); free(s1);
  deallocate_subdb(s2); free(s2);
  //Create third and final subDB
  SubDB* s3 = create_subdb(m, 0, 1, edgeLabel3);
  //Perform second hash join - we now have info from all tables
  SubDB* out = hash_join(sr12, s3);
  //deallocate sr12 and s3 SubDB's
  deallocate_subdb(sr12); free(sr12);
  deallocate_subdb(s3); free(s3);
  //Count equal left-right columns - faster than a third join.
  size_t result = check_equal_lr_subdb(out);
  //deallocate out SubDB
  deallocate_subdb(out); free(out);

  //return
  return result;
}

void ExperimentDeleteEdge(ExperimentDatabase database, int fromNodeID, int toNodeID, int edgeLabel)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //Delete (same API as ExperimentDeleteEdge)
  delete_maindb(m, fromNodeID, toNodeID, edgeLabel);
}

void ExperimentDeleteDatabase(ExperimentDatabase database)
{
  //implicit type conversion allowed by C but not by cmake environment.
  MainDB* m = (MainDB*)database;
  //deallocate table array and other data structures within MainDB. Set all size vales to zero.
  deallocate_maindb(m);
  //free memory occupied by struct instance.
  free(database);
}

//------------------------------------------END--------------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------
