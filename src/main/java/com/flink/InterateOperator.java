package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class InterateOperator {

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //as a graph all the edge cases must be covered.

        //generating data source
        DataStream<Tuple2<Long,Integer>> data = env.fromSequence(0, 4)
            .map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                public Tuple2<Long, Integer> map(Long value) {
                    //initialize as 0 iterations
                    return new Tuple2<Long, Integer>(value, 0);
                }
        });

        //creating a stream of data until x ms of time, it will call above function
        IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000);// ( 0,0   1,0  2,0  3,0   4,0  5,0 )

        // define iteration
        DataStream<Tuple2 <Long, Integer>> summing =
            iteration.map(new MapFunction <Tuple2<Long,Integer> , Tuple2<Long, Integer>>() {
                public Tuple2<Long,Integer> map(Tuple2<Long,Integer> value) {
                    if (value.f0 == 10)
                        return value;
                    else//if not 10, increment the number and the count.
                        return new Tuple2<Long,Integer> (value.f0 + 1, value.f1 + 1);
                }
        }); //   sum one - this will be the first iteration   1,1   2,1  3,1   4,1   5,1   6,1


        //two filters are required
        //one for feedback stream, other for output stream
        // feedback for next iteration (
        DataStream<Tuple2 <Long, Integer>> notEqualtoten =
                summing.filter(new FilterFunction < Tuple2 < Long, Integer >> () {
                    public boolean filter(Tuple2 < Long, Integer > value) {
                        if (value.f0 == 10) //if I found 10, dont need to loop again.
                            return false;
                        else
                            return true; //not 10, then loop again
                    }
                });
        //both filters are the same, the difference is that, this one as close with
        //this means that, this filter give feedback and not output.
        iteration.closeWith(notEqualtoten);

        //data for output
        DataStream < Tuple2 < Long, Integer >> equaltoten =
                summing.filter(new FilterFunction < Tuple2 < Long, Integer >> () {
                    public boolean filter(Tuple2 < Long, Integer > value) {
                        if (value.f0 == 10) return true;
                        else return false;
                    }
                });

        equaltoten.print("same as 10");

        // execute program
        env.execute("Iterate Operator");

    }
}
