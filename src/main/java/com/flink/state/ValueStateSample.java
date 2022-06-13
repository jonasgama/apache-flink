package com.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ValueStateSample {

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

            private ValueState<Double> sum;
            private ValueState<Long> count;

            @Override
            public void open(Configuration parameters) throws Exception {
                sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Double.class));
                count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
            }

            @Override
            public void flatMap(Tuple2<String, Double> input, Collector<Object> out) throws Exception {

                Double currentSum = sum.value();
                Long currentCount = count.value();

                if (currentCount == null){
                    currentCount = 0l;
                }
                if (currentSum == null){
                    currentSum = 0.0;
                }

                currentSum += input.f1;
                currentCount += 1;

                count.update(currentCount);
                sum.update(currentSum);

                if (currentCount >= 10){
                    out.collect(sum.value());

                    count.clear();
                    sum.clear();
                }
            }
        }).print();

        env.execute("value state sample");

    }
}
