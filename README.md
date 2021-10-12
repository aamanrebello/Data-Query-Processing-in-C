# Database Query Implementations
Implementations of sort-merge join and hash join (as used in RDBMS) algorithms in C.

## Context

The query operates on a graph with uniquely numbered nodes and directed edges which are also uniquely numbered as in the below diagram.

![image](GraphVisual.png)

It performs a query that counts the number of edge triangles having a particular sequence of edge numbers. For example, for an edge number sequence of '0,1,2', the result of the query would be 2 for the above diagram.

The graph is represented as a relational database table, with the following schema to represent every edge in the graph.

```

Start Node No.    |       End Node No.    |     Edge No.    |
                  |                       |                 |
                  |                       |                 |
                  
```

The query may then be evaluated by joining copies of the above table based in equality in the columns `Start Node No` and `Edge Node No` (here, the join algorithm implementations are [sort-merge join](https://en.wikipedia.org/wiki/Sort-merge_join) and [hash-join](https://en.wikipedia.org/wiki/Hash_join)).

## The Code

The code is in the [Source](Source) folder:
- [*Implementation.c*](Source/Implementation.c) contains the implementations of both the algorithms. It provides for setting up the database (implemented as an in-memory array encapsulated in a struct), inserting and removing edges. The code is itself highly documented.
- [*Implementation.h*](Source/ShapeCount.h) provides an external API that a program can use to manipulate the query processing system. There is a set of functions for setting up the database and querying it using sort-merge joins. There is a similar set of functions for doing so with hash-join. The functions whose identifiers contain 'Competition' can be ignored - these currently reimplement hash-join and are intended for further exploration of query processing techniques.
- [*testing.c*](Source/testing.c) DOES NOT DEPEND ON THE OTHER FILES. It can be directly run as a normal C program. It is mostly a copy of *ShapeCount.c* except for the `main()` function at the bottom. It illustrates how a database can be allocated, populated and queried in this system (without using the API defined above).

## Prerequisites 

The only prerequisite is a C compiler e.g. GCC.

# How to Run
Using GCC to run *testing.c*: enter the following in the Command Prompt from within the *Source* directory. 
```
gcc testing.c -o executable
```
Followed by:
`./executable` in Linux/MacOS or
`executable.exe` in Windows.

Alternately, include *Implementation.h* in your C program and use the API.

