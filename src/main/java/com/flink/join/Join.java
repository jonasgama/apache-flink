package com.flink.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class Join
{
    public static void main(String[] args)
            throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple2<Integer, String>> persons = env
                .readTextFile(params.get("input"))
                .map(new MyMapping());

        DataSet<Tuple2<Integer, String>> locations = env
                .readTextFile(params.get("input1"))
                .map(new MyMapping());

        //inner join
        //where 0 is the same as first field of the person.txt
        //equal to 0 is the same as the first field of the location.txt
        DataSet<Tuple3<Integer,String, String>> joined = persons.join(locations)
        .where(0).equalTo(0)
                //overriding the output manually
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                        //id person.txt, name person.txt, location.txt name
                        return new Tuple3<>(person.f0, person.f1, location.f1);
                    }
                });

        joined.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Join Example");

    }

    public static final class MyMapping implements MapFunction<String, Tuple2<Integer, String>>{

        public Tuple2<Integer, String> map(String s) throws Exception {
            String[] split = s.split(",");
            return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
        }
    }

}
