tez
===

tez is a generic data-processing pipeline engine envisioned as a low-level engine for higher abstractions 
such as Apache Hadoop Map-Reduce, Apache Pig, Apache Hive etc.

At it's heart, tez is very simple and has just two components:

*   The data-processing pipeline engine where-in one can plug-in input, processing and output implementations to 
    perform arbitrary data-processing. Every 'task' in tez has the following:
   -   Input to consume key/value pairs from.
   -   Processor to process them.
   -   Output to collect the processed key/value pairs.


*  A master for the data-processing application, where-by one can put together arbitrary data-processing 'tasks' 
   described above into a task-DAG to process data as desired. 
   The generic master is implemented as a Apache Hadoop YARN ApplicationMaster.
