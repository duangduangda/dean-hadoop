package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DedupMapper extends Mapper<LongWritable, Text,Text,Text> {

    public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException {
        context.write(value,new Text(""));
    }

}
