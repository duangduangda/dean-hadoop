package org.dean.hadoop.mapreduce.mapper;

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
 * @create: 2019/03/04 17:32
 */
public class CleanSameDataMapper extends Mapper<Object, Text,Text,Text> {
    private static final Logger logger = LoggerFactory.getLogger(CleanSameDataMapper.class);

    public void map(Object key, Text value, Context context) throws  InterruptedException, IOException {
        String line = value.toString();
        logger.info("line=======" + line);

       StringTokenizer tokenizer = new StringTokenizer(line,"\n");
        while (tokenizer.hasMoreTokens()){
            StringTokenizer stringTokenizer = new StringTokenizer(tokenizer.nextToken());
            String time = stringTokenizer.nextToken();
            String userId = stringTokenizer.nextToken();
            String keyword = stringTokenizer.nextToken();
            keyword = keyword.substring(1, keyword.length() - 1);
            context.write(new Text(time + "  " + userId + "  " +  keyword), new Text());
        }
    }
}
