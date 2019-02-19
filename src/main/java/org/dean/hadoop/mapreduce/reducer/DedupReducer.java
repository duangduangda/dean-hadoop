package org.dean.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DedupReducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
        context.write(key,new Text());
    }
}
