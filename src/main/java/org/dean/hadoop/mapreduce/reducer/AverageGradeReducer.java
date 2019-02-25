package org.dean.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AverageGradeReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
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
