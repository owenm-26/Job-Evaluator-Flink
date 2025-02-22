package clusterdata.exercise1;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.testing.JobEventTestBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JobSchedulingLatencyTest extends JobEventTestBase<JobEvent> {

    static Testable javaExercise = () -> JobSchedulingLatency.main(new String[]{});

    @Test
    public void testEventCount() throws Exception {

        JobEvent a_submit = testEvent(1, 0, EventType.SUBMIT);
        JobEvent b_submit = testEvent(2, 1, EventType.SUBMIT);
        JobEvent a_schedule = testEvent(1, 5, EventType.SCHEDULE);
        JobEvent c_fail = testEvent(3, 6, EventType.FAIL);
        JobEvent c_schedule = testEvent(3, 8, EventType.SCHEDULE);
        JobEvent a_submit2 = testEvent(1, 15, EventType.SUBMIT);
        JobEvent b_schedule = testEvent(2, 16, EventType.SCHEDULE);
        JobEvent d_submit = testEvent(4, 17, EventType.SUBMIT);
        JobEvent d_lost = testEvent(4, 19, EventType.LOST);
        JobEvent d_schedule = testEvent(4, 25, EventType.SCHEDULE);

        TestJobEventSource source = new TestJobEventSource(
                a_submit,
                b_submit,
                a_schedule,
                c_fail,
                c_schedule,
                a_submit2,
                b_schedule,
                d_submit,
                d_lost,
                d_schedule);

        List<?> results = results(source);
        System.out.println(results);
        assertEquals(3, results.size());
        assertEquals(true, results.contains(Tuple2.of(1L, 5L)));
        assertEquals(true, results.contains(Tuple2.of(2L, 15L)));
        assertEquals(true, results.contains(Tuple2.of(4L, 8L)));
    }

    private JobEvent testEvent(long id, long timestamp, EventType et) {
        return new JobEvent(id, timestamp, et, "Kate",
                "job42", "logicalJob42", 0, "");
    }

    protected List<?> results(TestJobEventSource source) throws Exception {
        return runApp(source, new TestSink<>(), javaExercise);
    }

}