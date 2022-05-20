package com.flink.window;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowType {

    public static void main(String[] args) throws Exception
    {

        /*
        * Built-ins
        *
        * Monotonously Increasing Timestamps
        *
        * Flink’s watermark merging mechanism will generate correct watermarks whenever parallel streams are shuffled, unioned, connected, or merged.
        * */
        WatermarkStrategy.forMonotonousTimestamps();

        /*
         * Built-ins
         *
         * Fixed Amount of Lateness
         *
         * maximum amount of time an element is allowed to be late before being ignored when computing the final result for the given window
         * Lateness corresponds to the result of t - t_w, where t is the (event-time) timestamp of an
         * element, and t_w that of the previous watermark. If lateness > 0 then the element is considered late
         * and is, by DEFAULT, ignored
         * */

        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1));

        /*
        * Allowed Lateness
        *
        *  Elements that arrive after the watermark has passed the end of the window + allowed time
        *  are still added to the window.
        *
        * Depending on the trigger used, a late but not dropped element may cause the window to fire again.
        * This is the case for the EventTimeTrigger
        *
        * y default, the allowed lateness is set to 0. That is, elements that arrive behind the watermark will be dropped.
        *
        * DataStream<T> input = ...;

            input
                .keyBy(<key selector>)
                .window(<window assigner>)
                .allowedLateness(<time>) <---here
                .<windowed transformation>(<window function>); <--here
        *
        *
        * */


        /*
        *
        * Getting late data as a side output
         *
         *
         * Using Flink’s side output feature you can get a stream of the data that was discarded as late.
           You first need to specify that you want to get late data using sideOutputLateData(OutputTag)
           * on the windowed stream.
           * Then, you can get the side-output stream on the result of the windowed operation:
        *
        * E.g
        *
        * final OutputTag<T> lateOutputTag = new OutputTag<T>("late-data"){};

            DataStream<T> input = ...;

            SingleOutputStreamOperator<T> result = input
                .keyBy(<key selector>)
                .window(<window assigner>)
                .allowedLateness(<time>)
                .sideOutputLateData(lateOutputTag) <--here
                .<windowed transformation>(<window function>);

            DataStream<T> lateStream = result.getSideOutput(lateOutputTag); <--here
        *
        *
        * The elements emitted by a late firing should be treated as updated results of a previous computation, i.e., your data stream will contain multiple results for the same computation. Depending on your application,
        * you need to take these duplicated results into account or deduplicate them.
        *
        * */

        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> data = env.socketTextStream("localhost", 9091);


        WatermarkStrategy<Tuple2<Long, String>> ws = WatermarkStrategy
                .<Tuple2<Long, String>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.f0)
                .withIdleness(Duration.ofMinutes(1));  //Dealing With Idle Sources


        DataStream<Tuple2<Long, String>> sum = data.map(new MapFunction<String, Tuple2<Long, String>>()
                {
                    public Tuple2<Long, String> map(String s)
                    {
                        String[] words = s.split(",");
                        return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
                    }
                })
                .assignTimestampsAndWatermarks(ws)
                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<Long, String>>()
                {
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> t1, Tuple2<Long, String> t2)
                    {
                        int num1 = Integer.parseInt(t1.f1);
                        int num2 = Integer.parseInt(t2.f1);
                        int sum = num1 + num2;
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2<Long, String>(t.getTime(), "" + sum);
                    }
                });
        sum.print();

        // execute program
        env.execute("Window Demo Watermark");
    }

    /**
     * This generator generates watermarks assuming that elements arrive out of order,
     * but only to a certain degree. The latest elements for a certain timestamp t will arrive
     * at most n milliseconds after the earliest elements for timestamp t.
     */
    public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<Tuple2<Long, String>> {

        private final long maxOutOfOrderness = 3500; // 3.5 seconds

        private long currentMaxTimestamp;

        @Override
        public void onEvent(Tuple2<Long, String> event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // emit the watermark as current highest timestamp minus the out-of-orderness bound
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }

    }

    /**
     * This generator generates watermarks that are lagging behind processing time
     * by a fixed amount. It assumes that elements arrive in Flink after a bounded delay.
     */
    public class TimeLagWatermarkGenerator implements WatermarkGenerator<Tuple2<Long, String>> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public void onEvent(Tuple2<Long, String> event, long eventTimestamp, WatermarkOutput output) {
            // don't need to do anything because we work on processing time
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
        }
    }


    /*
    *       WatermarkStrategy<Tuple2<Long, String>> ws = WatermarkStrategy
                .<Tuple2<Long, String>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.f0);
    *
    *           above code implements the interface below.
    *
    *  public interface WatermarkStrategy<T>
            extends TimestampAssignerSupplier<T>,
            WatermarkGeneratorSupplier<T> {


        @Override
        TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context);

        @Override
        WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
    *
    * OBS:
    *
    * Specifying a TimestampAssigner is optional and in most cases you don’t actually want to specify one. For example,
    * when using Kafka or Kinesis you would get timestamps directly from the Kafka/Kinesis records.
    *
    *
    * There are two places in Flink applications where a WatermarkStrategy can be used:
    * 1) directly on sources and.
    *
    * E.g kafka example
    *
    * FlinkKafkaConsumer<MyType> kafkaSource = new FlinkKafkaConsumer<>("myTopic", schema, props);
        kafkaSource.assignTimestampsAndWatermarks(
        WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(20)));

        DataStream<MyType> stream = env.addSource(kafkaSource);
    *
    *Note, that we don’t provide a TimestampAssigner in the example, the timestamps of the Kafka records themselves will be used instead.
    *
    * 2) after non-source operation.
    *
    * E.g
    *
    * DataStream<MyEvent> withTimestampsAndWatermarks = stream
        .filter( event -> event.severity() == WARNING )
        .assignTimestampsAndWatermarks(<watermark strategy>);
    *
    *this way takes a stream and produce a new stream with timestamped elements and watermarks. If the original stream had timestamps and/or watermarks already, the timestamp assigner overwrites them.
    *
    *
    *
    * */
    /*

    }*/
}
