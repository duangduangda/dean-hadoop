package org.dean.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @description: 使用Map算子进行数字加倍操作
 * @author: dean
 * @create: 2019/07/17 14:59
 */
public class MultiplyTwoNumber implements MapFunction<Integer,Integer> {

    @Override
    public Integer map(Integer value) throws Exception {
        return value * 2;
    }
}
