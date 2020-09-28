package org.apache.flink.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: izgnod.
 * @date: 27/09/2020
 * @description:
 */
public class WindowAll {

    // 结论：WindowAll是Non-Keyed Window；窗口分为：Non-Keyed Windows 和 Key Windows；Key Windows有keyBy， Non-Keyed Windows没有。
    // Keyed Windows 使用window, Non-Keyed Windows使用windowAll;
    // TumblingWindow的运行方式：每隔指定时间,指定下一个窗口；例如：每隔5秒，执行5秒内窗口的数据，例如23:50:05 执行 23:50:00 - 23:50:05的数据。

    // Keyed Windows: 调用keyBy就是keyedStream,keyedStream 能保证相同的key在同一个分区里，这时候进行窗口运算是有意义的，所以能够保证并发处理.
    // Non-Keyed Windows: 没有keyBy的支持，即使是拆分为多个并行，进行窗口运算，最终依旧要汇总成一个结果，所以Non-Keyed Windows支持一个并行。
    // 参考：http://www.wendaoxueyuan.com/thy-post/detail/ce9d4ce9-691a-4214-87d5-f2766b3e3d66
    // 启动：nc -l 9999 （nc命令在本地监听9999接口）
    // 输入：(10秒内)
    // one,1
    // one,2
    // two,3
    // two,2
    // 输出：
    //   two,3
    //
    // 解释：1. one,1 进入，此时窗口内存在(one,1)，但是由于时间未到，不执行;
    //      2. one,2 进入，此时窗口内存在(one,2)，但是由于时间未到，不执行;
    //      3. two,3 进入，此时窗口内存在(two,3)，但是由于时间未到，不执行;
    //      4. two,2 进入，此时窗口内存在(two,2)，但是由于时间未到，不执行;
    //      5. 时间到达10s，keyBy无效，WindowAll是Non-Keyed Window, 找到最大的3，输出：（two，3）

    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("127.0.0.1", 9999);
        stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                if (split.length == 2) {
                    out.collect(new Tuple2<>(split[0], Integer.parseInt(split[1])));
                } else {
                    System.out.println("invalid input:" + value);
                }
            }
        }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))).maxBy(1).print();
        env.execute();
    }
}
