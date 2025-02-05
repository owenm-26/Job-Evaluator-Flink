package clusterdata.exercise1;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashMap;


/**
 * Measure the time between submitting and scheduling each job in the cluster.
 * For each jobID, record the timestamp of the SUBMIT(0) event and look for a subsequent SCHEDULE(1) event.
 * Once received, output their time difference.
 * <p>
 * Note: If a job is submitted multiple times, then measure the latency since the first submission.
 */
public class JobSchedulingLatency extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", AppBase.pathToJobEventData);

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int servingSpeedFactor = 60;

        // set the parallelism
        env.setParallelism(4);

        // start the data generator
        DataStream<JobEvent> events = env
                .addSource(jobSourceOrTest(new JobEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        // DONE: process the job events of types `EventType.SCHEDULE` and `EventType.SUBMIT`, measuring
        //  the time difference between submission and scheduling.
        // Complete: jobIdWithLatency = ...

//        filter out non SCHEDULE and SUBMIT
        DataStream<JobEvent> filteredStream = events.filter(new FilterByEventType());

//        Divide by jobId
        KeyedStream<JobEvent, Long> keyedEvents = filteredStream
                .keyBy(val -> val.jobId);

// Flat Map in order to emit tuple when it is complete
        DataStream<Tuple2<Long, Long>> flatMappedEvents = keyedEvents.flatMap(new SaveTimestamps());

        printOrTest(flatMappedEvents);

        // execute the dataflow
        env.execute("Job Scheduling Latency");
    }


//    Flatmap
    public static class SaveTimestamps implements FlatMapFunction<JobEvent, Tuple2<Long, Long>> {
    HashMap<Long, Tuple2<Long, Long>> map = new HashMap<>();

    @Override
    public void flatMap(JobEvent job, Collector<Tuple2<Long, Long>> out) throws Exception {
        // 1) We get a SUBMIT
        if (job.eventType.equals(EventType.SUBMIT)) {
            if (map.containsKey(job.jobId)) {
                Tuple2<Long, Long> storedVal = map.get(job.jobId);
                // a) We have a SUBMIT already that is later
                if (storedVal.f0 != -1L && storedVal.f0 > job.timestamp) {
                    map.put(job.jobId, Tuple2.of(job.timestamp, -1L));
                }
                // b) We have a SCHEDULE but not a SUBMIT --> emit
                else if (storedVal.f0 == -1L && storedVal.f1 != -1L) {
                    out.collect(Tuple2.of(job.jobId,  storedVal.f1 - job.timestamp));
                }
                // c) We do not have a SUBMIT but we have a SCHEDULE
                else {
                    storedVal.f0 = job.timestamp;
                    map.put(job.jobId, storedVal);
                }
            }
            // We don't have either
            else {
                map.put(job.jobId, Tuple2.of(job.timestamp, -1L));
            }
        }


        // 1) We get a SCHEDULE
        else {
            if (job.eventType.equals(EventType.SCHEDULE)) {
                if (map.containsKey(job.jobId)) {
                    Tuple2<Long, Long> storedVal = map.get(job.jobId);
                    // a) We have a SCHEDULE already that is earlier
                    if (storedVal.f1 != -1L && storedVal.f0 < job.timestamp) {
                        map.put(job.jobId, Tuple2.of(-1L, job.timestamp));
                    }
                    // b) We have a SUBMIT but not a SCHEDULE --> emit
                    else if (storedVal.f1 == -1L && storedVal.f0 != -1L) {
                        out.collect(Tuple2.of(job.jobId,  job.timestamp - storedVal.f0));
                    }
                    // c) We do not have a SCHEDULE but we have a SUBMIT
                    else {
                        storedVal.f1 = job.timestamp;
                        map.put(job.jobId, storedVal);
                    }
                }
                // We don't have either
                else {
                    map.put(job.jobId, Tuple2.of(-1L, job.timestamp));
                }
            }
        }
    }
}

//    Only gets job events that are SUBMIT or SCHEDULE
    private static class FilterByEventType implements FilterFunction<JobEvent>{
        @Override
        public boolean filter(JobEvent job){
            return (job.eventType.equals(EventType.SUBMIT) || job.eventType.equals(EventType.SCHEDULE));

            }
        }
    }


