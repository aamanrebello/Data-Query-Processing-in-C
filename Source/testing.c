#include <stdio.h>
#include "Implementation.h"

int main()
{
  //Test 1 - single triangle with all edge numbers same - use sort-merge join
  void* sortMergeDb = SortMergeJoinAllocateDatabase(3);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 1, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 2, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 2, 0, 0);

  int result1 = SortMergeJoinRunQuery(sortMergeDb, 0, 0, 0);
  printf("\nRESULT 1: %ld", result1);

  SortMergeJoinDeleteDatabase(sortMergeDb);


  //Test 2 - Single triangle with different edge numbers - use hash join
  void* hashDb = HashjoinAllocateDatabase(3);
  HashjoinInsertEdge(hashDb, 0, 1, 0);
  HashjoinInsertEdge(hashDb, 1, 2, 1);
  HashjoinInsertEdge(hashDb, 2, 0, 2);

  int result2 = HashjoinRunQuery(hashDb, 0, 1, 2);
  printf("\nRESULT 2: %ld", result2);

  HashjoinDeleteDatabase(hashDb);


  // Test 3 - Multiple triangles - sort-merge join
  sortMergeDb = SortMergeJoinAllocateDatabase(11);

  SortMergeJoinInsertEdge(sortMergeDb, 0, 1, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 2, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 3, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 3, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 4, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 2, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 4, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 2, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 2, 0, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 4, 0, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 0, 2);

  int result3 = SortMergeJoinRunQuery(sortMergeDb, 0, 1, 2);
  printf("\nRESULT 3: %ld", result3);

  SortMergeJoinDeleteDatabase(sortMergeDb);

  //Test 4 - "Multiple edges with deletion - hash join"
  hashDb = HashjoinAllocateDatabase(12);

  HashjoinInsertEdge(hashDb, 0, 1, 0);
  HashjoinInsertEdge(hashDb, 0, 2, 0);
  HashjoinInsertEdge(hashDb, 0, 3, 0);
  HashjoinInsertEdge(hashDb, 1, 3, 1);
  HashjoinInsertEdge(hashDb, 1, 4, 1);
  HashjoinInsertEdge(hashDb, 1, 2, 1);
  HashjoinInsertEdge(hashDb, 3, 4, 1);
  HashjoinInsertEdge(hashDb, 3, 2, 1);
  HashjoinInsertEdge(hashDb, 2, 0, 2);
  HashjoinDeleteEdge(hashDb, 1, 3, 1);
  HashjoinDeleteEdge(hashDb, 3, 4, 1);
  HashjoinInsertEdge(hashDb, 4, 0, 2);
  HashjoinInsertEdge(hashDb, 3, 0, 2);
  HashjoinInsertEdge(hashDb, 3, 4, 1);

  int result4 = HashjoinRunQuery(hashDb, 0, 1, 2);
  printf("\nRESULT 4: %ld", result4);

  HashjoinDeleteDatabase(hashDb);

  //Test 5 - Multiple edges with deletion - sort merge join"
  
  //(specifying a lower number is ok since the DB will reallocate like a vector) 
  sortMergeDb = SortMergeJoinAllocateDatabase(10);

  SortMergeJoinInsertEdge(sortMergeDb, 0, 1, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 1, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 2, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 2, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 3, 0);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 3, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 0, 3, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 3, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 3, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 4, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 4, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 2, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 1, 2, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 4, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 4, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 2, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 2, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 2, 0, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 2, 0, 3);
  SortMergeJoinDeleteEdge(sortMergeDb, 1, 3, 1);
  SortMergeJoinDeleteEdge(sortMergeDb, 3, 4, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 4, 0, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 4, 0, 3);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 0, 2);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 0, 3);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 4, 1);
  SortMergeJoinInsertEdge(sortMergeDb, 3, 4, 3);

  int result5 = SortMergeJoinRunQuery(sortMergeDb, 0, 1, 2);
  printf("\nRESULT 5: %ld", result5);

  SortMergeJoinDeleteDatabase(sortMergeDb);
}
