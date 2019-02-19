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

/**
 * 对hive中的emp表薪资进行统计
 */
public class SalaryJob {

    private static final Logger logger = LoggerFactory.getLogger(SalaryJob.class);
    public static class SalaryMap extends Mapper<LongWritable, Text,Text, IntWritable>{
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String line = values.toString();
            System.out.println(line);
            StringTokenizer tokenizer = new StringTokenizer(line,"\n");
            while (tokenizer.hasMoreTokens()){
                StringTokenizer stringTokenizer = new StringTokenizer(tokenizer.nextToken(),",");
                String id = stringTokenizer.nextToken();
                String employeeName = stringTokenizer.nextToken();
                String duty = stringTokenizer.nextToken();
                String salary = stringTokenizer.nextToken();
                String city = stringTokenizer.nextToken();
                Text employee = new Text(employeeName);
                context.write(employee,new IntWritable(Integer.parseInt(salary)));
            }
        }
    }

    public static class SalaryReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                sum += iterator.next().get();
            }
            context.write(key,new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SalaryJob.class);
        job.setJobName("salaryJob");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SalaryMap.class);
        job.setCombinerClass(SalaryReduce.class);
        job.setReducerClass(SalaryReduce.class);

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
