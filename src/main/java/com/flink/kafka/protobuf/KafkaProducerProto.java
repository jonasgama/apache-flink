package com.flink.kafka.protobuf;

import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaProducerProto {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerTypeWithKryoSerializer(PersonOuterClass.Person.class, ProtobufSerializer.class);

        KafkaSink<PersonOuterClass.Person> kafkaSink = KafkaSink.<PersonOuterClass.Person>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("person")
                        .setValueSerializationSchema(new KafkaEventSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStreamSource<PersonOuterClass.Person> source = env.fromElements(PersonOuterClass.Person
                        .newBuilder()
                        .setId(1)
                        .setEmail("email@email.com")
                        .setName("foo")
                        .setSecondEmail("other@email.com")
                        .setLastName("doe")
                        .build());

        source.sinkTo(kafkaSink);

        env.execute("kafka producer protobuf example");

    }

    static class KafkaEventSchema implements SerializationSchema<PersonOuterClass.Person> {
        @Override
        public byte[] serialize(PersonOuterClass.Person person) {
            return person.toString().getBytes();
        }
    }
}
