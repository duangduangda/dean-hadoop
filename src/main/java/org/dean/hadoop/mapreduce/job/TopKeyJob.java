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
import org.dean.hadoop.mapreduce.mapper.TopKeyMapper;
import org.dean.hadoop.mapreduce.reducer.TopKeyReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: 词搜排序
 * @author: dean
 * @create: 2019/03/05 17:31
 */
public class TopKeyJob {
    private static final Logger logger = LoggerFactory.getLogger(TopKeyJob.class);

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName(TopKeyJob.class.getName());
        job.setJarByClass(TopKeyJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TopKeyMapper.class);
        job.setReducerClass(TopKeyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else{
            logger.info("failed");
        }

    }
}
