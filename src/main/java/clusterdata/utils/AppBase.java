package clusterdata.utils;

import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class AppBase {
    public static SourceFunction<JobEvent> jobEvents = null;
    public static SourceFunction<TaskEvent> taskEvents = null;
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 4;


    // DONE: change to your own path
    public final static String pathToJobEventData = "/Users/owenmariani/Desktop/School/junior-year/cs551/disc-3-owenm-26/data/job_events";
    public final static String pathToTaskEventData = "/Users/owenmariani/Desktop/School/junior-year/cs551/disc-3-owenm-26/data/task_events";


    public static SourceFunction<JobEvent> jobSourceOrTest(SourceFunction<JobEvent> source) {
        if (jobEvents == null) {
            return source;
        }
        return jobEvents;
    }

    public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
        if (strings == null) {
            return source;
        }
        return strings;
    }

    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
}
