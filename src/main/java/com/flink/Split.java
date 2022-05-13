package com.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Split {

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.readTextFile("/Users/jonasgama/Documents/repos/apache-flink/oddeven");

        // String type side output for Even values (par)
        final OutputTag<String> evenOutTag = new OutputTag<String>("even-string-output") {};
        // Integer type side output for Odd values (impar)
        final OutputTag<Integer> oddOutTag = new OutputTag<Integer>("odd-int-output") {};

        SingleOutputStreamOperator<Integer> mainStream = text.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(
                    String value,
                    Context ctx,
                    Collector<Integer> out) throws Exception {

                //convert string from the file and parse to number
                int intVal = Integer.parseInt(value);

                if (intVal % 2 == 0) {
                    // emit data to side output for even output
                    ctx.output(evenOutTag, String.valueOf(intVal));
                } else {
                    // emit data to side output for even output
                    ctx.output(oddOutTag, intVal);
                }
                //collect all data in regular output as well
                out.collect(intVal);
            }
        });

        DataStream<String> evenSideOutputStream = mainStream.getSideOutput(evenOutTag);
        DataStream<Integer> oddSideOutputStream = mainStream.getSideOutput(oddOutTag);
        evenSideOutputStream.print("even numbers");
        oddSideOutputStream.print("odd numbers");

        // execute program
        env.execute("SPLIT ODD EVEN");

    }
}
