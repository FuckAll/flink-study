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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
public class FlatMap {
    private static MyWordCount[] data = new MyWordCount[]{
            new MyWordCount(1, "Hello", 1),
            new MyWordCount(2, "Hello", 2),
            new MyWordCount(3, "Hello", 3),
            new MyWordCount(1, "World", 3)
    };

    // 结论：FlatMap 算子的输入流是 DataStream，经过 FlatMap 算子后返回的数据格式是 SingleOutputStreamOperator 类型，获取一个元素并生成零个、一个或多个元素。
    // 输出：
    //    flatMap operator:9> (3,Hello,3)
    //    flatMap operator:8> (2,Hello,2)
    //    flatMap operator:7> (1,Hello,1)
    //    flatMap operator:10> (1,World,3)
    //    flatMap operator:7> (1,Hello,1)
    //    flatMap operator:8> (2,Hello,2)
    //    flatMap operator:9> (3,Hello,3)
    //    flatMap operator:10> (1,World,3)
    // 解释：1. Map会处理每一个进入DataStream的element,并且将element进行目标类型转换

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data).flatMap(new FlatMapFunction<MyWordCount, Tuple3<Integer, String, Integer>>() {
            @Override
            public void flatMap(MyWordCount value, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
                out.collect(new Tuple3<>(value.getCount(),value.getWord(),value.getFrequency()));
                out.collect(new Tuple3<>(value.getCount(),value.getWord(),value.getFrequency()));
            }
        }).print("flatMap operator");
        env.execute();
    }
}
