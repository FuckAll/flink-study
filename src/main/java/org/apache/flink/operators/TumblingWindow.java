package org.apache.flink.operators;

import io.flinkspector.datastream.DataStreamTestBase;
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
public class TumblingWindow extends DataStreamTestBase {

    // 结论：Window是一个时间窗口，分为：TumblingWindow(翻滚窗口), SlidingWindow(滚动窗口)，SessionWindow(会话窗口)
    // TumblingWindow的运行方式：每隔指定时间,指定下一个窗口；例如：每隔5秒，执行5秒内窗口的数据，例如23:50:05 执行 23:50:00 - 23:50:05的数据。
    // 启动：nc -l 9999 （nc命令在本地监听9999接口）
    // 输入：(10秒内)
    // one,1
    // one,2
    // two,3
    // two,2
    // 输出：
    //   one,2
    //   two,3
    //
    // 解释：1. one,1 进入，此时窗口内存在(one,1)，但是由于时间未到，不执行;
    //      2. one,2 进入，此时窗口内存在(one,2)，但是由于时间未到，不执行;
    //      3. two,3 进入，此时窗口内存在(two,3)，但是由于时间未到，不执行;
    //      4. two,2 进入，此时窗口内存在(two,2)，但是由于时间未到，不执行;
    //      5. 时间到达10s，由于keyBy, one和two在不同的分区，分别执行，one中最大的是(one,2)，two中最大的是(two,3)

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
        }).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).maxBy(1).print();
        env.execute();
    }
}
