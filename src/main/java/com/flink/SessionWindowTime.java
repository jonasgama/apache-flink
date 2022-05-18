package com.flink;

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
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;


public class SessionWindowTime {

    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream < String > data = env.socketTextStream("localhost", 9090);

        DataStream < Tuple5 < String, String, String, Integer, Integer >> mapped = data.map(new Splitter()); // tuple  [June,Category5,Bat,12,1]
        DataStream < Tuple5 < String, String, String, Integer, Integer >> reduced = mapped
                .keyBy(t -> t.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
                .reduce(new Reducer());

        reduced.addSink(StreamingFileSink
                .forRowFormat(new Path("/Users/jonasgama/Documents/repos/apache-flink/window"),
                        new SimpleStringEncoder < Tuple5 < String, String, String, Integer, Integer >> ("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        // execute program
        env.execute("Session Window Month");

    }

    public static class Reducer implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                                                                       Tuple5<String, String, String, Integer, Integer> pre_result)
        {
            return new Tuple5<String, String, String, Integer, Integer>(current.f0,
                    current.f1, current.f2, (current.f3 + pre_result.f3), (current.f4 + pre_result.f4));
        }
    }
    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> map(String value)
        {
            String[] words = value.split(",");
            // ignore timestamp, we don't need it for any calculations
            return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1);
        }
    }


}
