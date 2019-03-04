package org.dean.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @description:
 * @author: dean
 * @create: 2019/03/04 17:33
 */
public class CleanSameDataReducer extends Reducer<Text,Text,Text,Text> {
    private static final Logger logger = LoggerFactory.getLogger(CleanSameDataReducer.class);

    public void reduce(Text key, Iterable<Text> value, Context context) throws InterruptedException, IOException {
        context.write(key,new Text());
    }
}
