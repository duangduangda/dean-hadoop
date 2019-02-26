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

public class MTJoinReducer extends Reducer<Text,Text,Text,Text> {
    private static final Logger logger = LoggerFactory.getLogger(MTJoinReducer.class);

    private static int time = 0;

//    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
//        if (time == 0) {
//            context.write(new Text("factory"), new Text("address"));
//            time++;
//        }
//        int factorynum = 0;
//        String[]factory = new String[10];
//        int addressnum = 0;
//        String[]address = new String[10];
//        Iterator<Text> iterator = values.iterator();
//        while (iterator.hasNext()){
//            String record = iterator.next().toString();
//            int len = record.length();
//            char type = record.charAt(0);
//            if (type == '1'){
//                factory[factorynum] = record.substring(2);
//                factorynum++;
//            }else{
//                address[addressnum] = record.substring(2);
//                addressnum++;
//            }
//        }
//
//        if (factorynum != 0 && addressnum != 0){
//            for (int m = 0;m < factorynum;m++){
//                for (int n = 0;n < addressnum;n++){
//                    context.write(new Text(factory[m]), new Text(address[n]));
//                }
//            }
//        }
//
//    }



    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
        if (time == 0) {
            context.write(new Text("factory"), new Text("address"));
            time++;
        }
        Iterator<Text> iterator = values.iterator();

        List<String> factories = new ArrayList<String>(10);
        List<String> address = new ArrayList<String>(10);

        while (iterator.hasNext()) {
            String record = iterator.next().toString();
            StringTokenizer stringTokenizer = new StringTokenizer(record, "+");
            while (stringTokenizer.hasMoreTokens()) {
                String type = stringTokenizer.nextToken();
                String data = stringTokenizer.nextToken();
                if ("1".equals(type)) {
                    factories.add(data);
                } else {
                    address.add(data);
                }
            }
        }
        if (factories.size() != 0 && address.size() != 0) {
            logger.info("factories:{}",factories);
            logger.info("address:{}",address);
            for (String factory:factories){
                for (String addr:address){
                    context.write(new Text(factory),new Text(addr));
                }
            }
        }
    }
}
