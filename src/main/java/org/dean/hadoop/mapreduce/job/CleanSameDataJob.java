package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dean.hadoop.mapreduce.mapper.CleanSameDataMapper;
import org.dean.hadoop.mapreduce.reducer.CleanSameDataReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: 数据去重
 * @author: dean
 * @create: 2019/03/04 17:24
 */
public class CleanSameDataJob {

    private static final Logger logger = LoggerFactory.getLogger(CleanSameDataJob.class);

    public static void main(String[] args)  throws  Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName(CleanSameDataJob.class.getName());
        job.setJarByClass(CleanSameDataJob.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CleanSameDataMapper.class);
        job.setReducerClass(CleanSameDataReducer.class);

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
