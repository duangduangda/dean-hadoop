package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.KeyWordCountMapper;
import org.dean.hadoop.mapreduce.reducer.KeyWordCountReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: 搜词热度排序
 * @author: dean
 * @create: 2019/03/05 10:45
 */
public class KeyWordCountJob {
    private static final Logger logger = LoggerFactory.getLogger(KeyWordCountJob.class);

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName(KeyWordCountJob.class.getName());
        job.setJarByClass(KeyWordCountJob.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(KeyWordCountMapper.class);
        job.setReducerClass(KeyWordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else{
            logger.info("failed");
        }

    }
}
