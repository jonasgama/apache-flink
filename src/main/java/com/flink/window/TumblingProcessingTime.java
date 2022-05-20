package com.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class TumblingProcessingTime {

    public static void main(String[] args) throws Exception
    {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        // tuple  [June,Category5,Bat,12,1]
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());

        // [June,Category4,Perfume,10,1]
        // groupBy 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(t->t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new Reducer());

        reduced.print();

        /*
        *
        *   reduced.addSink(StreamingFileSink
              .forRowFormat(new Path("/home/jivesh/www"),
                new SimpleStringEncoder < Tuple5 < String, String, String, Integer, Integer >> ("UTF-8"))
              .withRollingPolicy(DefaultRollingPolicy.builder().build())
              .build());
        *
        *
        * */

        env.execute("tumbling window with processing time.");


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
