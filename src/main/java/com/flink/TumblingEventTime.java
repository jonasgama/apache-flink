package com.flink;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;


public class TumblingEventTime {

    public static void main(String[] args) throws Exception
    {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //watermark strategy
        WatermarkStrategy<Tuple2<Long, String>> ws = WatermarkStrategy.<Tuple2<Long, String>>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<String> data = env.socketTextStream("localhost", 9091);

        DataStream<Tuple2 <Long, String>> sum = data.map(new MapFunction<String,Tuple2 <Long,String>> () {
            public Tuple2 < Long, String > map(String s) {
                String[] words = s.split(",");
                /* <timestamp>,<random-number> */
                return new Tuple2 < Long, String > (Long.parseLong(words[0]), words[1]);
            }
        })
        //will take the first element of the record as the timestamp, (event time)
        //considering that the event has the timestamp!
        .assignTimestampsAndWatermarks(ws)
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(5))) //using window all operator because it is a non-key record
        .reduce(new ReduceFunction<Tuple2 <Long,String>> () {
            public Tuple2 < Long, String > reduce(Tuple2 < Long, String > t1, Tuple2 < Long, String > t2) {
                //sum the first record random number with the second one.
                int num1 = Integer.parseInt(t1.f1);
                int num2 = Integer.parseInt(t2.f1);
                int sum = num1 + num2;
                //compute the time of the calc
                Timestamp t = new Timestamp(System.currentTimeMillis());
                return new Tuple2 <Long, String> (t.getTime(), "" + sum);
            }
        });


        sum.addSink(StreamingFileSink
                .forRowFormat(new Path("/Users/jonasgama/Documents/repos/apache-flink/window"),
                        new SimpleStringEncoder< Tuple2 < Long, String >>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

       // sum.print();
        // execute program
        env.execute("Window Tumbling Event time");

    }
}
