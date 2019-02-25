package org.dean.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.AverageGradeMapper;
import org.dean.hadoop.mapreduce.reducer.AverageGradeReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AverageGradeJob {
    private static final Logger logger = LoggerFactory.getLogger(AverageGradeJob.class);

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("AverageGradeJob");
        job.setJarByClass(AverageGradeJob.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(AverageGradeMapper.class);
        job.setReducerClass(AverageGradeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else {
            logger.info("failed");
        }
    }
}
