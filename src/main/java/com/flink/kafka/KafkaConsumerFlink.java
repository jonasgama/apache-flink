package com.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;



public class KafkaConsumerFlink {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> consumer = KafkaSource.<String>builder()
                                .setBootstrapServers("127.0.0.1:9092")
                                .setBounded(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema.valueOnly(
                                                StringDeserializer.class))
                                .setTopics("topic-test")
                                .build();

        DataStreamSource<String> kafkaData = env.fromSource(consumer, WatermarkStrategy.noWatermarks(), "kafka-source");

        kafkaData.flatMap(new FlatMapFunction< String, Tuple2< String, Integer >>() {
                public void flatMap(String value, Collector< Tuple2 < String, Integer >> out) {
                    out.collect(new Tuple2 < String, Integer > (value, 1));
                }
            })
            .print();

        env.execute("Kafka Example");

    }
}
