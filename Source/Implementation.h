#ifndef HEADER_FILE
#define HEADER_FILE

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

#endif