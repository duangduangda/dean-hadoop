package org.dean.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AverageGradeJob {
    private static final Logger logger = LoggerFactory.getLogger(AverageGradeJob.class);

    public static class Map extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException {
            String lines = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lines, "\n");
            while (stringTokenizer.hasMoreTokens()){
                StringTokenizer tokenizer = new StringTokenizer(stringTokenizer.nextToken(),",");
                String name = tokenizer.nextToken();
                String grade = tokenizer.nextToken();
                context.write(new Text(name),new DoubleWritable(Double.parseDouble(grade)));

            }
        }
    }

    public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            int count = 0;
            Iterator<DoubleWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                sum += iterator.next().get();
                count ++;
            }
            DoubleWritable average = new DoubleWritable(sum / count);
            context.write(key,average);
        }
    }

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("AverageGradeJob");
        job.setJarByClass(AverageGradeJob.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

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
