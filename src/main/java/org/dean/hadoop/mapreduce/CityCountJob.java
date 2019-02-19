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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class CityCountJob {
    private static final Logger logger = LoggerFactory.getLogger(CityCountJob.class);

    public static class CityMap extends Mapper<LongWritable, Text,Text, IntWritable>{

        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line,"\n");
            while (stringTokenizer.hasMoreTokens()){
                StringTokenizer tokenizer = new StringTokenizer(stringTokenizer.nextToken(),",");
                String id = tokenizer.nextToken();
                String name = tokenizer.nextToken();
                String duty = tokenizer.nextToken();
                String salary = tokenizer.nextToken();
                String city = tokenizer.nextToken();
                context.write(new Text(city),new IntWritable(1));
            }
        }
    }

    public static class CityReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        private static int total = 0;

        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                total += iterator.next().get();
            }
            context.write(key,new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("CityCountJob");
        job.setJarByClass(CityCountJob.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CityMap.class);
        job.setReducerClass(CityReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setNumReduceTasks(3);

        boolean success = job.waitForCompletion(true);
        if (success){
            logger.info("success");
        }else {
            logger.info("failed");
        }
    }

}
