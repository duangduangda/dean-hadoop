package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @description:
 * @author: dean
 * @create: 2019/03/05 17:36
 */
public class TopKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(TopKeyMapper.class);

    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()){
            String keyword = stringTokenizer.nextToken();
            String number = stringTokenizer.nextToken();
            logger.info("keyword:" + keyword + ",number:" + number);
            context.write(new Text(keyword),new IntWritable(Integer.parseInt(number)));
        }
    }
}
