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

//    public static class SortPartitioner<K,V> extends Partitioner<K,V> {
//        public int getPartition(K key,V value, int numReduceTasks) {
//            int maxValue = 100;
//            int keySection = 0;
//            if (numReduceTasks > 1 && key.hashCode() < maxValue) {
//                int sectionValue = maxValue / (numReduceTasks - 1);
//                int count = 0;
//                while ((key.hashCode()) - sectionValue * count > sectionValue){
//                    count++;
//                }
//                keySection = numReduceTasks - 1 - count;
//            }
//            return keySection;
//        }
//    }

//    public static class SortComparator extends WritableComparator {
//        protected SortComparator(){
//            super(IntWritable.class, true);
//        }
//
//        public int compare(Writable a, Writable b){
//            return -super.compare(a, b);
//        }
//
//    }

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("dean word count");
        job.setJarByClass(DeanWordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
//        job.setPartitionerClass(SortPartitioner.class);
//        job.setSortComparatorClass(SortComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

