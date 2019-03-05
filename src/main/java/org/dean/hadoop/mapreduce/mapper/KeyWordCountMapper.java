package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * @description:
 * @author: dean
 * @create: 2019/03/05 11:08
 */
public class KeyWordCountMapper extends Mapper<Object, Text,Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(KeyWordCountMapper.class);

    public void map(Object key,Text value,Context context) throws InterruptedException, IOException{
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()){
            String time = stringTokenizer.nextToken();
            String userId = stringTokenizer.nextToken();
            String keyword = stringTokenizer.nextToken();
            logger.info("time:" + time + ",userId:" + userId + ",keyword:" + keyword);
            context.write(new Text(keyword.trim()),new IntWritable(1));
        }
    }
}
