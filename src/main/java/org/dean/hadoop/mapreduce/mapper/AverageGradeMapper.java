package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AverageGradeMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {
    public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException {
        String lines = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(lines, "\n");
        while (stringTokenizer.hasMoreTokens()){
            StringTokenizer tokenizer = new StringTokenizer(stringTokenizer.nextToken(),",");
            String name = tokenizer.nextToken();
            String grade = tokenizer.nextToken();
            context.write(new Text(name),new DoubleWritable(Double.parseDouble(grade)));

        }
    }
}
