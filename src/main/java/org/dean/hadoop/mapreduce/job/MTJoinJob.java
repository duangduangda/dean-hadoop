package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.MTJoinMapper;
import org.dean.hadoop.mapreduce.reducer.MTJoinReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MTJoinJob {
    private static final Logger logger = LoggerFactory.getLogger(MTJoinJob.class);

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName(MTJoinJob.class.getName());
        job.setJarByClass(MTJoinJob.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MTJoinMapper.class);
        job.setReducerClass(MTJoinReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else {
            logger.info("failed");
        }
    }
}
