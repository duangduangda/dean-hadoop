package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.SortMapper;
import org.dean.hadoop.mapreduce.reducer.SortReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSortJob {

    private final static Logger logger = LoggerFactory.getLogger(DataSortJob.class);

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("DataSortJob");
        job.setJarByClass(DataSortJob.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else{
            logger.info("failed");
        }
    }

}
