package org.apache.flink.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.bean.MyWordCount;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: izgnod.
 * @date: 27/09/2020
 * @description:
 */
public class Reduce {
    private static MyWordCount[] data = new MyWordCount[]{
            new MyWordCount(1, "One", 1),
            new MyWordCount(1, "Two", 2),
            new MyWordCount(3, "Three", 3),
            new MyWordCount(4, "Four", 4)
    };

    // 结论：Reduce返回单个的结果值，并且reduce操作每处理一个元素总是创建一个新值。常用的方法有 average、sum、min、max、count，使用Reduce方法都可实现
    // 输出：
    //    reduce:1> MyWordCount{count=4, word='Four', frequency=4}
    //    reduce:4> MyWordCount{count=3, word='Three', frequency=3}
    //    reduce:3> MyWordCount{count=1, word='One', frequency=1}
    //    reduce:3> MyWordCount{count=1, word='One', frequency=3}
    // 解释：1. word = "One" 进入，此时没有亲一条数据，直接输出 MyWordCount{count=1, word='One', frequency=1}
    //      2. word = "Two" 进入，由于count=1，与之前一条同时分配到一个keyBy区域，两条数据frequency相加 1+2=3，输出前一个数据和更新后的frequency MyWordCount{count=1, word='One', frequency=3}
    //      3. word = "Three" 进入，由于count=3, 与之前的两条keyBy区域都不同，输出本身MyWordCount{count=3, word='Three', frequency=3}
    //      4. word = "Four" 进入， 由于count=4, 与之前的三条keyBy区域都不同，输出本身MyWordCount{count=4, word='Four', frequency=4}

    // 注意：Reduce只能在KeyedStream上进行，并且Reduce中在不同的KeyBy区域中单独进行计算。
    // 参考：https://medium.com/big-data-processing/apache-flink-specifying-keys-81b3b651469

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data).keyBy("count").reduce(new ReduceFunction<MyWordCount>() {
            @Override
            public MyWordCount reduce(MyWordCount value1, MyWordCount value2) throws Exception {
                value1.setFrequency(value1.getFrequency() + value2.getFrequency());
                return value1;
            }
        }).print("reduce");
        env.execute();
    }
}
