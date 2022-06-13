package com.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ListStateSample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream< String > data = env.socketTextStream("localhost", 9091);

        data.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[1], Math.random());
            }
        })
        .keyBy(t->t.f0)
        .flatMap(new RichFlatMapFunction<Tuple2<String, Double>, Object>() {

            private ListState<Double> sum;

            @Override
            public void open(Configuration parameters) throws Exception {
                sum = getRuntimeContext().getListState(new ListStateDescriptor<>("numbers", Double.class));
            }

            @Override
            public void flatMap(Tuple2<String, Double> input, Collector<Object> out) throws Exception {
                sum.add(input.f1);

                Double total =  0.0d;
                for (Double s : sum.get()) {
                    total += s;
                }


                if (total >= 10){
                    out.collect(total);
                    sum.clear();
                }
            }
        }).print();

        env.execute("list state sample");

    }
}
