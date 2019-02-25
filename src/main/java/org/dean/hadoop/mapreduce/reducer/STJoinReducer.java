package org.dean.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class STJoinReducer extends Reducer<Text,Text, Text,Text> {
    private static final Logger logger = LoggerFactory.getLogger(STJoinReducer.class);

    private static int time = 0;
    public void  reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
        // 输出表头
        if (time == 0){
            context.write(new Text("grandchild"),new Text("grandparent"));
            time++;
        }
        Iterator<Text> iterator = values.iterator();
        String record;
        List<String> children = new ArrayList<String>(10);
        List<String> parents = new ArrayList<String>(10);
        while (iterator.hasNext()){
            record = iterator.next().toString();
            if (record.length() == 0){
                continue;
            }
            StringTokenizer stringTokenizer = new StringTokenizer(record,"+");
            while (stringTokenizer.hasMoreTokens()){
                String type = stringTokenizer.nextToken();
                String first = stringTokenizer.nextToken();
                String second = stringTokenizer.nextToken();
                if ("1".equals(type)){
                    children.add(first);
                }else if("2".equals(type)){
                    parents.add(second);
                }else{
                    logger.info("illegal type!!");
                    continue;
                }
            }
        }

        if (children.size() != 0 && parents.size() != 0){
            for (int m = 0;m < children.size();m++){
                for (int n = 0;n < parents.size();n++){
                    context.write(new Text(children.get(m)),new Text(parents.get(n)));
                }
            }
        }
    }
}
