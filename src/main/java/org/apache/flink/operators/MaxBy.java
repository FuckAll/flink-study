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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.bean.MyWordCount;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
            new MyWordCount(1, "Hello", 1),
            new MyWordCount(2, "Hello", 2),
            new MyWordCount(3, "Hello", 1),
            new MyWordCount(1, "World", 1)
    };
    private static StreamTableEnvironment streamTableEnvironment;

    // 结论：如果是maxBy，current >= pre 返回第一个current,否则返回pre；
    // (与Max不同，Max只交换比较大小的内容，然后返回原element，max只操心field的最大值，maxBy操心field最大值的element)
    //
    // 输出：
    //    MyWordCount{count=1, word='Hello', frequency=1}
    //
    //
    //    MyWordCount{count=2, word='Hello', frequency=2}
    //
    //
    //    MyWordCount{count=2, word='Hello', frequency=2}
    //
    //
    //    MyWordCount{count=1, word='World', frequency=1}
    //
    // 解释：1. 按照keyBy将 word = "Hello" 和 word = "Word" 放入两个slot执行(分区)
    //      2. 例如 word = "Hello"，
    //                 第一步：{count=1, word='Hello', frequency=1} 此时frequency=1是最大的。返回{count=1, word='Hello', frequency=1}
    //                 第二步: {count=2, word='Hello', frequency=2} 此时frequency=2大于上一条，返回：{count=2, word='Hello', frequency=2}
    //                 第三步: {count=3, word='Hello', frequency=1} 此时frequency=1小于上一条，返回上一条：{count=2, word='Hello', frequency=2}
    //      3. 同理 word = "World"
    //                 由于与word = "Hello" 不用的keyBy分区
    //                 第一步：{count=1, word='World', frequency=1} 此时frequency=1是最大的。返回{count=1, word='World', frequency=1}
    // 注意：MaxBy只能在KeyedStream上进行，并且MaxBy中在不同的KeyBy区域中单独进行计算。

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        streamTableEnvironment = StreamTableEnvironment.create(env);

        Table tmpTable = streamTableEnvironment.fromDataStream(env.fromElements(data), "count,frequency,word,pc.proctime");
        tmpTable.printSchema();
        streamTableEnvironment.toAppendStream(tmpTable, Row.class).print();
//        scala.util.parsing.combinator.Parsers
//        env.fromElements(data)
//                .keyBy("word")
//                .maxBy("frequency")
//                .addSink(new SinkFunction<MyWordCount>() {
//                    @Override
//                    public void invoke(MyWordCount value, Context context) {
//                        System.err.println("\n" + value + "\n");
//                    }
//                });
        env.execute();
    }
}
