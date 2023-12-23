package com.ddsr.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author ddsr, created it at 2023/12/23 21:01
 */
public class HandleTimeOutPartialPatterns {
    public static class Event {
        private final String name;
        // Other fields and methods

        public Event(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

    static final OutputTag<String> timedOutPartialMatchTag = new OutputTag<String>("timed-out-partial-match") {
    };

    public static class MyPatternProcessFunction extends PatternProcessFunction<Event, String>
            implements TimedOutPartialMatchHandler<Event> {

        @Override
        public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
            // Handle complete matches here
            String matchStr = match.values().stream()
                    .flatMap(List::stream)
                    .map(Event::getName)
                    .collect(Collectors.joining(", "));
            out.collect("Complete match: " + matchStr);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) {
            // Handle timed out partial matches here, emit to side-output
//            for (Event event : match.get("start")) {
//                ctx.output(timedOutPartialMatchTag, event);
//            }

            ctx.output(timedOutPartialMatchTag, match.toString());
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        SingleOutputStreamOperator<Event> eventStream = ds.map(Event::new);
        // Define the event pattern, take care of the keyBy operation of eventstream
        // As the keyBy operation, so the test case : aaa, aaa (input them in 3 seconds)
        // Output: main==>> Complete match: aaa, aaa
        // timedOutPartialMatch==>> {start=[Event{name='aaa'}]}, because of the within window of the second aaa
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) {
                        // Define your condition here
                        return value.getName().equals("aaa");
                    }
                })
                .next("next")
                .where(SimpleCondition.of(value -> value.getName().equals("aaa")))
                .within(Time.seconds(3));

        // Apply the pattern to the input event stream
        PatternStream<Event> patternStream = CEP.pattern(eventStream.keyBy(Event::getName), pattern).inProcessingTime();

        // Process the detected patterns
        SingleOutputStreamOperator<String> mainDataStream = patternStream.process(new MyPatternProcessFunction());

        mainDataStream.print("main==>");

        // Obtain the side-output stream and print its elements
        DataStream<String> timedOutPartialMatchStream = mainDataStream.getSideOutput(timedOutPartialMatchTag);
        timedOutPartialMatchStream.print("timedOutPartialMatch==>");

        env.execute("CEP Handling Timed Out Partial Patterns");
    }
}






