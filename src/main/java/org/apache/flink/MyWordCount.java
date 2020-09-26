package org.apache.flink;

public class MyWordCount {
    private int count;
    private String word;
    private int frequency;

    public MyWordCount() {
    }

    private MyWordCount(int count, String word, int frequency) {
        this.count = count;
        this.word = word;
        this.frequency = frequency;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "MyWordCount{" +
                "count=" + count +
                ", word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}