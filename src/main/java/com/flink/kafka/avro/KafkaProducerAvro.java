package com.flink.kafka.avro;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaProducerAvro {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String schemaRegistryUrl = "http://127.0.0.1:8081";

        KafkaSink<Dependency> kafkaSink = KafkaSink.<Dependency>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dependencies")
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema
                                .forSpecific(
                                        Dependency.class,
                                        "dependencies-value",
                                        schemaRegistryUrl))
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStreamSource<Dependency> source = env.fromElements(Dependency
                        .newBuilder()
                        .setId(1)
                        .setBatchSize(1)
                        .build(),
                Dependency
                        .newBuilder()
                        .setId(2)
                        .setBatchSize(5)
                        .build(),
                Dependency
                        .newBuilder()
                        .setId(3)
                        .setBatchSize(4)
                        .build());

        source.sinkTo(kafkaSink);

        env.execute("kafka producer avro example");



    }
}
