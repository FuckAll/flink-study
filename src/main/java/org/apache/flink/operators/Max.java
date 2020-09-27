/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.operators;

import org.apache.flink.MyWordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class Max {
    private static MyWordCount[] data = new MyWordCount[]{
            new MyWordCount(1,"Hello", 1),
            new MyWordCount(2,"Hello", 2),
            new MyWordCount(3,"Hello", 3),
            new MyWordCount(1,"World", 3)
    };

    // 结论：如果是max，如果value1比value2大，返回value1；
    //      否则，将value2中用来作对比的field的值替换value1中该field的值，并返回value1。
    // 输出：
    //    MyWordCount{count=1, word='Hello', frequency=1}
    //
    //
    //    MyWordCount{count=1, word='Hello', frequency=2}
    //
    //
    //    MyWordCount{count=1, word='World', frequency=3}
    //
    //
    //    MyWordCount{count=1, word='Hello', frequency=3}
    // 解释：1. 按照keyBy将 word = "Hello" 和 word = "Word" 放入两个slot执行
    //      2. 例如 word = "Hello"，
    //                 第一步：{count=1, word='Hello', frequency=1} 此时frequency=1是最大的。返回{count=1, word='Hello', frequency=1}
    //                 第二步: {count=2, word='Hello', frequency=2} 此时frequency=2大于上一条，将frequency=2与上一条中frequency交换，不换其他的。返回：{count=1, word='Hello', frequency=2}
    //                 第三步: {count=3, word='Hello', frequency=3} 此时frequency=2大于上一条，将frequency=3与上一条中frequency交换，不换其他的。返回：{count=1, word='Hello', frequency=3}
    //       3. 同理 word = "World" ...
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data)
                .keyBy("word")
                .max("frequency")
                .addSink(new SinkFunction<MyWordCount>() {
                    @Override
                    public void invoke(MyWordCount value, Context context) {
                        System.err.println("\n" + value + "\n");
                    }
                });
        env.execute("testMax");
    }
}
