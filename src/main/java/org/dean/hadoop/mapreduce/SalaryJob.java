package org.dean.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.SalaryMapper;
import org.dean.hadoop.mapreduce.reducer.SalaryReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * 对hive中的emp表薪资进行统计
 */
public class SalaryJob {

    private static final Logger logger = LoggerFactory.getLogger(SalaryJob.class);

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SalaryJob.class);
        job.setJobName("salaryJob");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SalaryMapper.class);
        job.setCombinerClass(SalaryReducer.class);
        job.setReducerClass(SalaryReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("成功");
        }else{
            logger.info("失败");
        }
    }
}
