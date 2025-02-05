package clusterdata.exercise1;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;


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

        // TODO: process the job events of types `EventType.SCHEDULE` and `EventType.SUBMIT`, measuring
        //  the time difference between submission and scheduling.
        // Complete: jobIdWithLatency = ...

//        filter out non SCHEDULE and SUBMIT
        DataStream<JobEvent> filteredStream = events.filter(new FilterByEventType());

//        Divide by jobId
        KeyedStream<JobEvent, Long> keyedEvents = filteredStream
                .keyBy(val -> val.jobId);

//        convert to tuple
        DataStream<Tuple2<Long, Long>> tupleMappedEvents = keyedEvents.map(new EventToTuple());

// Flat Map in order to emit tuple when it is complete

        printOrTest(tupleMappedEvents);

        // execute the dataflow
        env.execute("Job Scheduling Latency");
    }


//    Transform Event into empty Tuple for later use
    public static class EventToTuple implements MapFunction<JobEvent, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> map(JobEvent job) {
        return Tuple2.of(0L, 0L);
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


