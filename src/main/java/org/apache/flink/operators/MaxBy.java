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

import org.apache.flink.bean.MyWordCount;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

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
public class MaxBy {
    private static MyWordCount[] data = new MyWordCount[]{
            new MyWordCount(1,"Hello", 1),
            new MyWordCount(2,"Hello", 2),
            new MyWordCount(3,"Hello", 3),
            new MyWordCount(1,"World", 3)
    };

    // 结论：如果是maxBy，如果相等，first为true返回第一个value；first为false返回第二个value；(与Max不同，Max只交换比较大小的内容，然后返回原elements)
    //
    // 输出：
    //    MyWordCount{count=1, word='Hello', frequency=1}
    //
    //
    //    MyWordCount{count=2, word='Hello', frequency=2}
    //
    //
    //    MyWordCount{count=1, word='World', frequency=3}
    //
    //
    //    MyWordCount{count=3, word='Hello', frequency=3}
    // 解释：1. 按照keyBy将 word = "Hello" 和 word = "Word" 放入两个slot执行
    //      2. 例如 word = "Hello"，
    //                 第一步：{count=1, word='Hello', frequency=1} 此时frequency=1是最大的。返回{count=1, word='Hello', frequency=1}
    //                 第二步: {count=2, word='Hello', frequency=2} 此时frequency=2大于上一条，返回：{count=2, word='Hello', frequency=2}
    //                 第三步: {count=3, word='Hello', frequency=3} 此时frequency=3大于上一条，返回：{count=3, word='Hello', frequency=3}
    //       3. 同理 word = "World" ...
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data)
                .keyBy("word")
                .maxBy("frequency")
                .addSink(new SinkFunction<MyWordCount>() {
                    @Override
                    public void invoke(MyWordCount value, Context context) {
                        System.err.println("\n" + value + "\n");
                    }
                });
        env.execute("testMax");
    }
}
