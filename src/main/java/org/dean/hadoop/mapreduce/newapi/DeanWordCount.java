package org.dean.hadoop.mapreduce.newapi;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * mapreduce new api
 */
public class DeanWordCount {
    public static class Map extends Mapper<LongWritable, Text, Text,LongWritable>{
        private static final LongWritable one = new LongWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value,Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while (stringTokenizer.hasMoreTokens()){
                word.set(stringTokenizer.nextToken());
                context.write(word, one);
            }


        }
    }


    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable result = new LongWritable();
        public void reduce(Text key, Iterable<LongWritable> value,Context context)
                throws IOException,InterruptedException {
            long sum = 0L;
            for (LongWritable v:value){
                sum += v.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("dean word count");
        job.setJarByClass(DeanWordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

