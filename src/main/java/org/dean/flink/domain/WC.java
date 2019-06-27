package org.dean.flink.domain;

import com.google.common.base.MoreObjects;

/**
 * @description: POJO
 *  1. 必须是public以及不含有非静态的内部类；
 *  2. 必须有一个public类型的无参构造方法；
 *  3. 所有的非静态，非transient字段必须是public,或者提供public的setter和getter方法
 * @author: dean
 * @create: 2019/06/15 23:04
 */
public class WC {

    public String word;

    public Integer counter;

    public WC(){

    }

    public WC(String word, Integer counter) {
        this.word = word;
        this.counter = counter;
    }

    public static WC build(){
        return new WC();
    }

    public String getWord() {
        return word;
    }

    public WC setWord(String word) {
        this.word = word;
        return this;
    }

    public Integer getCounter() {
        return counter;
    }

    public WC setCounter(Integer counter) {
        this.counter = counter;
        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("word", word)
                .add("counter", counter)
                .toString();
    }
}
