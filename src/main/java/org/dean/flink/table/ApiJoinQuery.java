package org.dean.flink.table;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @description:
 * @author dean
 * @since 2019-07-16
 */
public class ApiJoinQuery {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Integer,String>> people = executionEnvironment.fromElements(
                new Tuple2<>(1,"eric"),
                new Tuple2<>(2,"duang"),
                new Tuple2<>(3,"dean"));
        DataSource<Tuple2<Integer,String>> address = executionEnvironment.fromElements(
                new Tuple2<>(1,"beijing"),
                new Tuple2<>(2,"shanghai"),
                new Tuple2<>(4,"hongkong")
        );

        // dataset1内关联dataset2
         people.leftOuterJoin(address)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (null == second){
                            return new Tuple3<>(first.f0,first.f1,null);
                        }else{
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();

    }
}
