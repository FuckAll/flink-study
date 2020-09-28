package org.apache.flink.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: izgnod.
 * @date: 27/09/2020
 * @description:
 */
public class CountWindow {

    // 结论：CountWindow 分为Non-Keyed Window和Keyed Windows；Key Windows有keyBy，进行分区内计算； Non-Keyed Windows没有keyBy，只能在一个分区内计算。
    // CountWindow是按照个数触发，例如countWindow(5), 如果是Keyed Windows 需要在单个分区内满足5个元素才能输出结果。如果是Non-Keyed Window，只需要总共5个元素即可。

    // Keyed Windows: 调用keyBy就是keyedStream,keyedStream 能保证相同的key在同一个分区里，这时候进行窗口运算是有意义的，所以能够保证并发处理.
    // Non-Keyed Windows: 没有keyBy的支持，即使是拆分为多个并行，进行窗口运算，最终依旧要汇总成一个结果，所以Non-Keyed Windows支持一个并行。
    // 参考：http://www.wendaoxueyuan.com/thy-post/detail/ce9d4ce9-691a-4214-87d5-f2766b3e3d66
    // 启动：nc -l 9999 （nc命令在本地监听9999接口）
    // 输入：
    // one,1
    // one,2
    // two,3
    // two,2
    // two,1
    // two,0
    // two,4
    // 输出：
    //      non-keyed-countWindowAll:3> (two,3)
    //      keyed-countWindow:9> (two,4)
    //
    // 解释：
    //      1. Non-Keyed CountWindowAll 只需要总共5个数据，当输入到two,1的时候，满足条件，进行计算获取最大的元素(two,3)
    //      2. Keyed CountWindow 需要在每个分区满足5个元素的时候触发，one对应的分区由于没有达到5个，不进行触发计算。two对应的分区当输入到
    //         two,4的时候满足5个，触发计算找出two分区最大的元素(two,4)，进行输出。

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> singleOutputStreamOperator = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                if (split.length == 2) {
                    out.collect(new Tuple2<>(split[0], Integer.parseInt(split[1])));
                } else {
                    System.out.println("invalid input:" + value);
                }
            }
        });
        singleOutputStreamOperator.countWindowAll(5).maxBy(1).print("non-keyed-countWindowAll");
        singleOutputStreamOperator.keyBy(0).countWindow(5).maxBy(1).print("keyed-countWindow");
        env.execute();
    }
}
