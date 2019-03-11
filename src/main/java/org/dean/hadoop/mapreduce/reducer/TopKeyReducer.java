package org.dean.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @description:
 * @author: dean
 * @create: 2019/03/05 17:41
 */
public class TopKeyReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
    private static final Logger logger = LoggerFactory.getLogger(TopKeyReducer.class);

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            context.write(key,iterator.next());
        }
    }
}
