package org.dean.hadoop.mapreduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordLineCounterMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    private static final Logger logger = LoggerFactory.getLogger(WordLineCounterMapper.class);

    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
        Counter counter = context.getCounter(WordLineCounterEnum.COUNTER_LINE);
        logger.info("line num increment");
        counter.increment(1L);

        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()){
            if (StringUtils.isNotBlank(stringTokenizer.nextToken())){
                context.getCounter(WordLineCounterEnum.COUNT_WORDS).increment(1L);
                logger.info("word num increment");
            }
        }
    }

    public enum WordLineCounterEnum{
        COUNT_WORDS,COUNTER_LINE
    }
}
