package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.WordLineCounterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordLineCounterJob {
    private static final Logger logger  = LoggerFactory.getLogger(WordLineCounterJob.class);

    public static void main(String[] args)throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName(WordLineCounterJob.class.getName());
        job.setJarByClass(WordLineCounterJob.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordLineCounterMapper.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)){
            logger.info("success");
        }else {
            logger.info("failed");
        }

    }
}
