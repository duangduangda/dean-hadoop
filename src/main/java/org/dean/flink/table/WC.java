package org.dean.flink.table;

import com.google.common.base.MoreObjects;

/**
 * @description:
 * @author: dean
 * @create: 2019/06/15 23:04
 */
public class WC {

    private String word;

    private int count;

    public WC(){

    }

    public WC(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("word", word)
                .add("count", count)
                .toString();
    }
}
