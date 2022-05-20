package com.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class KafkaAggregator {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> variablesConsumer = KafkaSource.<String>builder()
                                .setBootstrapServers("127.0.0.1:9092")
                                .setBounded(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema.valueOnly(
                                                StringDeserializer.class))
                                .setTopics("variables")
                                .build();

        DataStream<DataSource> variableInputs = env
                .fromSource(variablesConsumer, WatermarkStrategy.noWatermarks(),
                        "kafka-all-variables")
                        .map(new MapFunction<String, DataSource>(){
                                public DataSource map(String s) throws Exception {
                                    String[] split = s.split(",");
                                    DataSource dataSource = new DataSource();
                                    dataSource.id = split[0];
                                    dataSource.variable = split[1];
                                    return dataSource;
                                }
                        });


        variableInputs
                .keyBy(m->m.id)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new AggregateFunction<DataSource, MyMessage, MyMessage>() {
                    @Override
                    public MyMessage createAccumulator() {
                        return new MyMessage();
                    }

                    @Override
                    public MyMessage add(DataSource dataSource, MyMessage myMessage) {
                        myMessage.id = dataSource.id;
                        myMessage.variables.add(dataSource.variable);
                        return myMessage;
                    }

                    @Override
                    public MyMessage getResult(MyMessage myMessage) {
                         return myMessage;
                    }

                    @Override
                    public MyMessage merge(MyMessage myMessage, MyMessage acc1) {
                        return myMessage.merge(acc1);
                    }
                })
                .print();


        env.execute("Kafka aggregation Example");

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
