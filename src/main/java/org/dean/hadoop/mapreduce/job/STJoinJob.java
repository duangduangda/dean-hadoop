package org.dean.hadoop.mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dean.hadoop.mapreduce.mapper.STJoinMapper;
import org.dean.hadoop.mapreduce.reducer.STJoinReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单表关联
 */
public class STJoinJob {
    private static final Logger logger = LoggerFactory.getLogger(STJoinJob.class);

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("STJoinJob");
        job.setJarByClass(STJoinJob.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(STJoinMapper.class);
        job.setReducerClass(STJoinReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (job.waitForCompletion(true)){
            logger.info("success");
        }else {
            logger.info("failed");
        }
    }

}
