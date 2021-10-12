#pragma once
#include <functional>
extern "C" {
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
}

struct SortMergeJoinImplementation {
  std::function<void(void* sVoid, int from, int to, int label)> const insertEdge =
      ::SortMergeJoinInsertEdge;
  std::function<void(void* sVoid, int fromNode, int toNode, int label)> const deleteEdge =
      ::SortMergeJoinDeleteEdge;
  std::function<int(void* vstore, int label1, int label2, int label3)> const runQuery =
      ::SortMergeJoinRunQuery;
  std::function<void*(unsigned long size)> const allocateDatabase = ::SortMergeJoinAllocateDatabase;
  std::function<void(void* sVoid)> const deleteDatabase = ::SortMergeJoinDeleteDatabase;
};

struct HashjoinImplementation {
  std::function<void(void* sVoid, int from, int to, int label)> const insertEdge =
      ::HashjoinInsertEdge;
  std::function<void(void* sVoid, int fromNode, int toNode, int label)> const deleteEdge =
      ::HashjoinDeleteEdge;
  std::function<int(void* vstore, int label1, int label2, int label3)> const runQuery =
      ::HashjoinRunQuery;
  std::function<void*(unsigned long size)> const allocateDatabase = ::HashjoinAllocateDatabase;
  std::function<void(void* sVoid)> const deleteDatabase = ::HashjoinDeleteDatabase;
};

struct ExperimentImplementation {
  std::function<void(void* sVoid, int from, int to, int label)> const insertEdge =
      ::HashjoinInsertEdge;
  std::function<void(void* sVoid, int fromNode, int toNode, int label)> const deleteEdge =
      ::HashjoinDeleteEdge;
  std::function<int(void* vstore, int label1, int label2, int label3)> const runQuery =
      ::HashjoinRunQuery;
  std::function<void*(unsigned long size)> const allocateDatabase = ::HashjoinAllocateDatabase;
  std::function<void(void* sVoid)> const deleteDatabase = ::HashjoinDeleteDatabase;
};
