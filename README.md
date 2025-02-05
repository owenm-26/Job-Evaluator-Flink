[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/fN8Ytt_v)
# Cluster data analysis

In this discussion, we will be using a subset of traces from a large Google cluster of 12.5k machines. Make sure to carefully read the [data specification and download instructions](https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md) as well as the [format and schema document](https://drive.google.com/file/d/0B5g07T_gRDg9Z0lsSTEtTWtpOW8/view?resourcekey=0-cozD56gA4fUDdrkHnLJSrQ).

For the purpose of this assignment, we will use parts 0, 1, and 2 of the Job and Task Events tables. This data is already populated in your data folder.

**Do not extract the .gz files!**

After reading the resources above, open the `clusterdata.utils.AppBase class` and point the variables `pathToJobEventData` and `pathToTaskEventData` to the appropriate locations.


## Tasks
You will have to implement two Flink application under files `exercise1/JobEventCount.java` and `exercise1/JobSchedulingLatency.java`. You might get duplicate events. For example, you can get 2 submit events for the same job id. You do not need consider crash cases (i.e. fault tolerance) and you can store data in memory. 

Refer to the Flink documentation to find suitable operators for the given tasks: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/ 

No other files should be changed or modified and the only code needed are the implementations for the two methods provided. There are TODO labels showing the desired outcome and what to do for each method.

After completion, you should be able to test your code using tests `JobEventCountTest.java` and `exercise1/JobSchedulingLatencyTest.java`. During testing, change `env.setParallelism(1);` in the application files to reflect larger than 1 Parallelism factor.
