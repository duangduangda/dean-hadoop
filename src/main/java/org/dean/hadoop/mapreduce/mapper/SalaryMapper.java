package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class SalaryMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
    private static final Logger logger = LoggerFactory.getLogger(SalaryMapper.class);

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        String line = values.toString();
        StringTokenizer tokenizer = new StringTokenizer(line,"\n");
        while (tokenizer.hasMoreTokens()){
            StringTokenizer stringTokenizer = new StringTokenizer(tokenizer.nextToken(),",");
            String id = stringTokenizer.nextToken();
            String name = stringTokenizer.nextToken();
            String duty = stringTokenizer.nextToken();
            String salary = stringTokenizer.nextToken();
            String city = stringTokenizer.nextToken();
            logger.info("record:[" + id + "," + name + "," + duty + ","+ salary + ","+ city + ","+"]");
            Text employee = new Text(name);
            context.write(employee,new IntWritable(Integer.parseInt(salary)));
        }
    }
}
