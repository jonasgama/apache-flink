package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount
{
    public static void main(String[] args)
            throws Exception
    {
        //the environment is going to run in the existing jvm
        //also can be configured to run in a remote environment, like hadoop
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //read arguments from the java cli, .property files and etc.
        ParameterTool params = ParameterTool.fromArgs(args);
        //turning the params available to each node
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        //filter all names starting by N
        DataSet<String> filtered = text.filter(new FilterFunction<String>()

        {
            public boolean filter(String value)
            {
                return value.startsWith("N");
            }
        });


        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
        //output

        /*
        *
        *
        *   Noman 1
            Noman 1

        *
        *   Nanaho 1
            Noel 1
            Noman 1
            Nana 1
            Nicole 1

        * */

        //group by 0 means, the first element of the stream (string).
        // them sum up by 1 the second element, in this case a integer
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
        //output

        /*
           Noman 12
        *  Nicole 7
        *
        *
        * */

        if (params.has("output"))
        {
            //to write as csv have to be a tuple
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }
    //MapFunction generates mulitples outputs, flatmap generates one
    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }
}
