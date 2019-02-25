package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class STJoinMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()) {
            String child = stringTokenizer.nextToken();
            String parent = stringTokenizer.nextToken();
            if (child.contains("child") || parent.contains("parent")) {
                continue;
            }
            // 左表
            context.write(new Text(parent), new Text("1" + "+" + child + "+" + parent));
            // 右表
            context.write(new Text(child), new Text("2" + "+" + child + "+" + parent));

        }
    }
}
