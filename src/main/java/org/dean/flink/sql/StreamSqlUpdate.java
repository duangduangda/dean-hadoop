package org.dean.flink.sql;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.List;

public class StreamSqlUpdate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);

        List<Tuple2<String,Integer>> tuples = Lists.newArrayList();
        tuples.add(new Tuple2<>("Banana",1));
        tuples.add(new Tuple2<>("Bear",2));
        tuples.add(new Tuple2<>("Potato",1));

        DataStream<Tuple2<String,Integer>> sensorTable = environment.fromCollection(tuples);
        tableEnvironment.registerDataStream("Sensors",sensorTable,"product,amount");

        String[]filedNames = new String[]{"name","counter"};
        TypeInformation[] filedTypes = new TypeInformation[]{Types.STRING(),Types.INT()};
        CsvTableSink csvSink = new CsvTableSink("D:\\flink",",",1, FileSystem.WriteMode.OVERWRITE);
        tableEnvironment.registerTableSink("csv_output", filedNames,filedTypes,csvSink);
        String sql = "INSERT INTO csv_output SELECT product,amount FROM Sensors";
        tableEnvironment.sqlUpdate(sql);
        environment.execute();

    }
}
