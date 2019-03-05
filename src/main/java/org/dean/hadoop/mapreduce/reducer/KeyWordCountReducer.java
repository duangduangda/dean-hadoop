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
 * @create: 2019/03/05 11:09
 */
public class KeyWordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(KeyWordCountReducer.class);

    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
        Iterator<IntWritable> iterator = values.iterator();
        int sum = 0;
        while (iterator.hasNext()){
            sum += iterator.next().get();
        }
        context.write(key,new IntWritable(sum));
    }
}
