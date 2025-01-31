package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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

        printOrTest(jobIdWithLatency);

        // execute the dataflow
        env.execute("Job Scheduling Latency");
    }


    
}
