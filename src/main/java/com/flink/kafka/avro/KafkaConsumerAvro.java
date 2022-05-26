package com.flink.kafka.avro;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class KafkaConsumerAvro {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Dependency> variablesConsumer = KafkaSource.<Dependency>builder()
                                .setBootstrapServers("127.0.0.1:9092")
                                .setBounded(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema
                                                .valueOnly(AvroDeserializationSchema
                                                        .forSpecific(Dependency.class)))
                                .setTopics("variables")
                                .build();

        env.fromSource(variablesConsumer, WatermarkStrategy.noWatermarks(),
                        "kafka-all-variables")
                .print();


        env.execute("Kafka consumer avro example");

    }

   static class DataSource{
       private String id;
       private String variable;
   }

    static class MyMessage {
        private String id;
        private List<String> variables;

        public MyMessage() {
            this.variables = new ArrayList();
        }

        public MyMessage(String id) {
            this.id = id;
            this.variables = new ArrayList();
        }
        public MyMessage(String id, List<String> variables) {
            this.id = id;
            this.variables = variables;
        }

        public MyMessage initInstance(String id) {
            return new MyMessage(id);
        }

        public MyMessage merge(MyMessage other) {
            return new MyMessage(this.id, Stream.concat(this.variables.stream(), other.variables.stream())
                    .collect(Collectors.toList()));
        }

        @Override
        public String toString() {
            return "MyMessage{" +
                    "id=" + id +
                    ", variables=" + variables +
                    '}';
        }

        public boolean isDone() {
            return variables.size() == 3;
        }
    }

}
