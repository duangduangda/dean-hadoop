package org.dean.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MTJoinMapper extends Mapper<Object, Text,Text,Text> {
    private static final Logger logger = LoggerFactory.getLogger(MTJoinMapper.class);

    public void map(Object key, Text value,Context context) throws IOException,InterruptedException {
        String line = value.toString();
        // 首行及描述性文字不进行处理
        if (line.contains("factory") || line.contains("address") || line.length() == 0){
            return;
        }
        // 判断是factory表还是address表
        if (line.charAt(0) > '9' || line.charAt(0) < '0'){
            int i = 1;
            while (line.charAt(i) > '9' || line.charAt(i) < '0'){
                i++;
            }
            String company = line.substring(0,i);
            String addressId = line.substring(i);
            context.write(new Text(addressId),new Text(1 + "+" +  company));
        }else {
            int i = 1;
            while (line.charAt(i) <= '9' && line.charAt(i) >= '0'){
                i++;
            }
            String addressId = line.substring(0,i);
            String addressName = line.substring(i);
            context.write(new Text(addressId), new Text(2 + "+" + addressName));
        }
    }
}
