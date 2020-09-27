package org.apache.flink.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.bean.MyWordCount;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: izgnod.
 * @date: 27/09/2020
 * @description:
 */
public class KeyedBy {
    private static MyWordCount[] data = new MyWordCount[]{
            new MyWordCount(1, "Hello", 1),
            new MyWordCount(2, "Hello", 2),
            new MyWordCount(3, "Hello", 3),
            new MyWordCount(1, "World", 3)
    };

    // 结论：KeyBy 在逻辑上是基于key对流进行分区，相同的Key会被分到一个分区（这里分区指的就是下游算子多个并行节点的其中一个）。
    // 在内部，它使用hash函数对流进行分区。它返回KeyedDataStream数据流。 使用方法：key(new KeySelector) 、key(position)、key(field)
    // key
    // 输出：
    //      1> MyWordCount{count=1, word='Hello', frequency=1}
    //      1> MyWordCount{count=2, word='Hello', frequency=2}
    //      1> MyWordCount{count=3, word='Hello', frequency=3}
    //      1> MyWordCount{count=1, word='World', frequency=3}
    // 解释：1. KeyBy基于word将流进行分区，不同的分区处理不同的key
    // 注意：由于keyBy会发生数据倾斜，并不是相同的key一定会在相同的slot上执行
    // 参考：https://medium.com/big-data-processing/apache-flink-specifying-keys-81b3b651469

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data).keyBy(new KeySelector<MyWordCount, Integer>() {
            @Override
            public Integer getKey(MyWordCount value) throws Exception {
                return value.getCount();
            }
        }).print("keyByWork");

//        env.fromElements(data).keyBy("word", "count").print("keyByWorkAndCount");
        env.execute();
    }
}
