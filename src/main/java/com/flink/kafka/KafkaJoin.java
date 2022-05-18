package com.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;


public class KafkaJoin {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ParameterTool params = ParameterTool.fromArgs(args);
        //env.getConfig().setGlobalJobParameters(params);

        KafkaSource<String> variablesConsumer = KafkaSource.<String>builder()
                                .setBootstrapServers("127.0.0.1:9092")
                                .setBounded(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema.valueOnly(
                                                StringDeserializer.class))
                                .setTopics("variables")
                                .build();

        KafkaSource<String> dependenciesConsumer = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setBounded(OffsetsInitializer.latest())
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(
                                StringDeserializer.class))
                .setTopics("dependencies")
                .build();

        DataStream<Tuple2<Integer, String>> variableInputs = env
                .fromSource(variablesConsumer,  WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(60)),
                        "kafka-all-variables")
                        .map(new MapFunction<String, Tuple2<Integer, String>>(){
                                public Tuple2<Integer, String> map(String s) throws Exception {
                                    String[] split = s.split(",");
                                    return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
                                }
                        });



        DataStream<Tuple2<Integer, String>> dependenciesData = env.fromSource(dependenciesConsumer,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60)),
                "kafka-dependencies-map")
                .map(new MapFunction<String, Tuple2<Integer, String>>(){
                    public Tuple2<Integer, String> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
                    }
                });

        DataStream<Tuple4<Integer, String, String, Integer>> joinedData = variableInputs
                .keyBy(t->t.f0)
                .join(dependenciesData)
                .where(t->t.f0)
                .equalTo(t->t.f0)
                //.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(6)))
                .window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(60)))
                .apply(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple4<Integer, String, String, Integer>>() {
                    @Override
                    public Tuple4<Integer, String, String, Integer> join(Tuple2<Integer, String> variables,
                                                                          Tuple2<Integer, String> dependencies) throws Exception {
                        //id, variable, bath size, count
                        return new Tuple4<Integer, String, String, Integer>(variables.f0, variables.f1, dependencies.f1, 0);
                    }
                })
                .keyBy(t->t.f0)
                .process(new KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, Tuple4<Integer, String, String, Integer>>() {

                    /** The state that is maintained by this process function */
                    private ValueState<CountWithTimestamp> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
                    }

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Integer> value, KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, Tuple4<Integer, String, String, Integer>>.Context ctx, Collector<Tuple4<Integer, String, String, Integer>> collector) throws Exception {
                        CountWithTimestamp current = state.value();

                        if (current == null) {
                            current = new CountWithTimestamp();
                            current.key = value.f0;
                        }
                        if (Integer.parseInt(value.f2) > current.count){
                            current.count++;
                        }else{
                            collector.collect(value);
                        }


                        state.update(current);
                    }
                });


        joinedData.print();
        env.execute("Kafka Example");

    }

    static class CountWithTimestamp {

        public Integer key;
        public long count;
        public long lastModified;
    }

}
