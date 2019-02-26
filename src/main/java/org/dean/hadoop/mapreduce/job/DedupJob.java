package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.DedupMapper;
import org.dean.hadoop.mapreduce.reducer.DedupReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupJob {
    private static final Logger logger = LoggerFactory.getLogger(DedupJob.class);

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("deduplication job");
        job.setJarByClass(DedupJob.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(DedupMapper.class);
        job.setCombinerClass(DedupReducer.class);
        job.setReducerClass(DedupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else{
            logger.info("failed");
        }
    }
}
