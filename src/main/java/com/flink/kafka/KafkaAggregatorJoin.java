package com.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class KafkaAggregatorJoin {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        WatermarkStrategy<String> wm =
                WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

        KafkaSource<String> variablesConsumer = KafkaSource.<String>builder()
                                .setBootstrapServers("127.0.0.1:9092")
                                .setBounded(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema.valueOnly(
                                                StringDeserializer.class))
                                .setTopics("variables")
                                .build();

        KafkaSource<String> dependenciesMapConsumer = KafkaSource
                .<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setBounded(OffsetsInitializer.latest())
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(
                                StringDeserializer.class))
                .setTopics("dependencies")
                .build();

        DataStream<DataSource> variableInputs = env
                .fromSource(variablesConsumer, wm,
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

        DataStream<DependenciesMap> dependenciesInput = env
                .fromSource(dependenciesMapConsumer, wm,
                        "kafka-all-dependencies")
                .map(new MapFunction<String, DependenciesMap>(){
                    public DependenciesMap map(String s) throws Exception {
                        String[] split = s.split(",");
                        DependenciesMap dependenciesMap = new DependenciesMap();
                        dependenciesMap.id = split[0];
                        dependenciesMap.bathSize = Integer.parseInt(split[1]);
                        return dependenciesMap;
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
                .keyBy(m->m.id)
                .join(dependenciesInput)
                .where(m->m.id)
                .equalTo(d->d.id)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new JoinFunction<MyMessage, DependenciesMap, MyMessage>() {
                    @Override
                    public MyMessage join(MyMessage myMessage, DependenciesMap dependenciesMap) throws Exception {
                        myMessage.setBathSize(dependenciesMap.bathSize);
                        return myMessage;
                    }
                })
                .keyBy(m->m.id)
                .process(new KeyedProcessFunction<String, MyMessage, MyMessage>() {

                    /** The state that is maintained by this process function */
                    private ValueState<CountWithTimestamp> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
                    }

                    @Override
                    public void processElement(MyMessage myMessage, KeyedProcessFunction<String, MyMessage, MyMessage>.Context context, Collector<MyMessage> collector) throws Exception {
                        CountWithTimestamp current = state.value();
                        if (current == null) {
                            current = new CountWithTimestamp();
                            current.id = myMessage.id;
                            current.bathSize = myMessage.bathSize;
                            current.variables = myMessage.variables;
                        }

                        if (myMessage.isDone()){
                            collector.collect(myMessage);
                        }
                        else{
                            current.lastModified = context.timestamp();

                            state.update(current);
                            // schedule the next timer 10 seconds from the current event time
                            context.timerService().registerEventTimeTimer(current.lastModified + 10000);
                        }
                    }

                    @Override
                    public void onTimer(
                            long timestamp,
                            OnTimerContext ctx,
                            Collector<MyMessage> out) throws Exception {

                        // get the state for the key that scheduled the timer
                        CountWithTimestamp result = state.value();

                        // check if this is an outdated timer or the latest timer
                        if (timestamp == result.lastModified + 10000) {
                            // emit the state on timeout
                            MyMessage myMessage = new MyMessage();
                            myMessage.id = result.id;
                            myMessage.bathSize = result.bathSize;
                            myMessage.variables = result.variables;
                            out.collect(myMessage);
                        }
                    }
                })
                .print();


        env.execute("Kafka aggregation Example");

    }

    static class CountWithTimestamp {

        public String id;
        public long lastModified;
        public Integer bathSize;
        public List<String> variables;

    }

    static class DependenciesMap{
        private String id;
        private Integer bathSize;
    }

   static class DataSource{
       private String id;
       private String variable;
   }

    static class MyMessage {
        private String id;
        private Integer bathSize;
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
            return variables.size() == bathSize;
        }

        public Integer getBathSize() {
            return bathSize;
        }

        public void setBathSize(Integer bathSize) {
            this.bathSize = bathSize;
        }
    }

}
