# Real-Time Data Stream Processing with Apache Flink  

## Project Overview  
This project demonstrates the implementation of two Apache Flink applications focused on real-time data stream processing. The goal was to develop scalable, performant solutions for event counting and latency measurement while adhering to strict requirements and constraints.  

The two applications implemented are:  
1. **Job Event Counter** (`JobEventCount.java`) - Counts job-related events, handling duplicate entries.  
2. **Job Scheduling Latency Calculator** (`JobSchedulingLatency.java`) - Computes scheduling latencies from event data.  

Both implementations highlight the use of Flink’s DataStream API and its operators for real-time data processing.  

---

## Features  
- **Efficient Event Handling:** Processes and deduplicates multiple event types for accurate results.  
- **Real-Time Analytics:** Utilizes Flink’s stream processing framework to calculate metrics with low latency.  
- **Scalable Parallelism:** Configured for parallel execution to scale with higher data loads.  
- **Test-Driven Development:** Thoroughly tested using Flink-based unit tests to ensure correctness and reliability.  

---

## Key Requirements  
- **No External Dependencies:** All code was implemented within the designated files without modifying other parts of the project.  
- **In-Memory Storage:** Solutions are optimized to store data in memory, without persisting to external storage.  
- **Fault Tolerance Not Required:** Simplified assumptions exclude crash recovery scenarios.  

---

## How It Works  
### 1. **Job Event Counter**  
This application processes a stream of job-related events, such as submissions, updates, and completions, to count the number of events for each job ID.  

Key tasks include:  
- Identifying and handling duplicate events.  
- Using Flink operators like `KeyBy` and `Reduce` for aggregation.  

### 2. **Job Scheduling Latency Calculator**  
This application computes the time difference between job submission and job scheduling events, producing accurate latency metrics.  

Key tasks include:  
- Correlating events with matching job IDs.  
- Using operators such as `KeyBy` and `ProcessFunction` for stateful computations.  
