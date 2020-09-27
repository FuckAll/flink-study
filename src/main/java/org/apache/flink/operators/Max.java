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
            new MyWordCount(1, "Hello", 1),
            new MyWordCount(2, "Hello", 2),
            new MyWordCount(3, "Hello", 1),
            new MyWordCount(1, "World", 1)
    };

    // 结论：如果是max，current >= pre,current中用来作对比的field的值替换pre中field的值，并且返回pre, 如果current < pre, 直接返回pre;
    //      (与maxBy不同，maxBy是返回最大值对应的element, 而max返回的是数据流中的第一个值，只管field的最大值)
    // 注意：pre是之前的结果（之前的max结果）；MaxBy只能在KeyedStream上进行，并且MaxBy中在不同的KeyBy区域中单独进行计算。
    // 输出：
    //      1> MyWordCount{count=1, word='Hello', frequency=1}
    //      1> MyWordCount{count=1, word='Hello', frequency=2}
    //      1> MyWordCount{count=1, word='Hello', frequency=2}
    //      1> MyWordCount{count=1, word='World', frequency=1}
    //
    // 解释：1. 按照keyBy将 word = "Hello" 和 word = "Word" 放入两个slot执行
    //      2. 例如 word = "Hello"，
    //                 第一步：{count=1, word='Hello', frequency=1} 此时frequency=1是最大的，结果：{count=1, word='Hello', frequency=1}
    //                 第二步: {count=2, word='Hello', frequency=2} 此时frequency=2大于上一次结果，将frequency=2与上一条中frequency交换，不换其他的,结果：{count=1, word='Hello', frequency=2}
    //                 第三步: {count=3, word='Hello', frequency=1} 此时frequency=2小于一次结果，将frequency=3与上一条中frequency交换，不换其他的。返回：{count=1, word='Hello', frequency=3}
    //       3. 同理 word = "World"
    //                 work = "World" 与 work = "Hello" 在不同的keyBy区域中，单独进行计算

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data)
                .keyBy("word")
                .max("frequency")
                .print();
        env.execute();
    }
}
