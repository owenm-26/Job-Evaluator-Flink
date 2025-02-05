package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * This application continuously outputs the number of events per job id
 * seen so far in the job events stream.
 */
public class JobEventCount extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", AppBase.pathToJobEventData);

        final int servingSpeedFactor = 60; // events of 1 minute are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // start the data generator
        DataStream<JobEvent> events = env
                .addSource(jobSourceOrTest(new JobEventSource(input, servingSpeedFactor)))
                .setParallelism(1);



        // TODO: process events to count the how many events for each jobId
        // Complete: result = ...

//        Assign values of 1
        DataStream<Tuple2<Integer, Integer>> valuedEvents = events
                .map(new MyMapFunction())
                .name("valuedEvents");

//        Separate by Keys
        KeyedStream<Tuple2<Integer, Integer>, Integer> keySeperated = valuedEvents
                .keyBy(value -> value.f0);

//        Count by keys
        DataStream<Tuple2<Integer, Integer>> counted = keySeperated
                .reduce(new MyReduceFunction())
                        .name("countedEvents");



        printOrTest(counted);

        // execute the dataflow
        env.execute("Continuously count job events");
    }

//    Sums all the jobs' values
    private static class MyReduceFunction implements ReduceFunction< Tuple2<Integer, Integer>> {
        @Override
        public  Tuple2<Integer, Integer> reduce( Tuple2<Integer, Integer> val1,  Tuple2<Integer, Integer> val2){
            return Tuple2.of(val1.f0, val1.f1.intValue() + val2.f1.intValue());
        }
    }

//    Assigns each job a value of 1
    private static class MyMapFunction implements MapFunction<JobEvent, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2 map(JobEvent object){
            return Tuple2.of(object.jobId, 1);
        }
    }
}

