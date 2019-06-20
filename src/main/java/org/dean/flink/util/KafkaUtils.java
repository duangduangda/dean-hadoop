package org.dean.flink.util;

import java.util.Properties;

/**
 * @description: kafka工具类
 * @author: dean
 * @create: 2019/06/19 19:27
 */
public class KafkaUtils {

    /**
     * kafka参数配置
     * @return
     */
    public static Properties config() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");//broker地址
        properties.setProperty("zookeeper.connect", "localhost:2181");//zookeeper配置
        properties.setProperty("group.id", "flink_group");
        return properties;
    }
}
