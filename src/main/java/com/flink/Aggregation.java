package com.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.readTextFile("/Users/jonasgama/Documents/repos/apache-flink/avg1");

        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter());

        //iam keying by the month
        //suming all the profit by month
        //example the splitter cuts the first index, so we have July,Category1,Television
        //groupy by july
        //13-07-2018,July,Category1,Television,50, consider 50 as first input
        //17-07-2018,July,Category5,Steamer,27, consider 50+27 == 77
        //06-07-2018,July,Category1,Pendrive,12, consider 77+12 = 90
        mapped.keyBy(t->t.f0).sum(3).print("SUM");

        //the value is compared to the previous one available, and pick the minimum
        //but min operation save only the computed field, not the rest, this can cause some issues
        //flink dont get track of other fields, instead use minby
        mapped.keyBy(t->t.f0).min(3).print("MIN");

        //the value is compared to the previous one available, and pick the minimum
        mapped.keyBy(t->t.f0).minBy(3).print("MIN BY");

        mapped.keyBy(t->t.f0).max(3).print("MAX");

        mapped.keyBy(t->t.f0).maxBy(3).print("MAX BY");

        env.execute("Aggregation");

    }

    public static class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>>
    {
        public Tuple4<String, String, String, Integer> map(String value)         // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple4<String, String, String, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]));
        }                                                  //    June    Category5      Bat               12
    }
}
