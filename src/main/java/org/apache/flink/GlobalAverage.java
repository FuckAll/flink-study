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

package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

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
public class GlobalAverage {
// 方案一：三元组(流计算)，流数据不适合计算全局的平均值，一旦没有之前
//    public static void main(String[] args) throws Exception {
//        // set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.fromElements(employments).flatMap(new FlatMapFunction<List<Employment>, Tuple3<Integer, Integer, Integer>>() {
//            @Override
//            public void flatMap(List<Employment> employments, Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {
//                for (Employment employment : employments) {
//                    collector.collect(new Tuple3<>(employment.shopId, employment.salary, 1));
//                }
//            }
//        }).keyBy(0).reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
//            @Override
//            public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> integerIntegerTuple2, Tuple3<Integer, Integer, Integer> t1) throws Exception {
//                return new Tuple3<>(integerIntegerTuple2.f0, integerIntegerTuple2.f1 + t1.f1, integerIntegerTuple2.f2 + t1.f2);
//            }
//        }).map(new MapFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Integer> integerIntegerIntegerTuple3) throws Exception {
//                return new Tuple2<>(integerIntegerIntegerTuple3.f0, integerIntegerIntegerTuple3.f1 / integerIntegerIntegerTuple3.f2);
//            }
//        }).print();
//        // execute program
//        env.execute("Flink Streaming Java API Skeleton");
//    }

    // 方案二：批处理 数据源是一次性的输入
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        UnsortedGrouping<Tuple3<Integer, Integer, Integer>> unsortedGrouping = env.fromElements(employments).flatMap(new FlatMapFunction<List<Employment>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void flatMap(List<Employment> employments, Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                for (Employment employment : employments) {
                    collector.collect(new Tuple3<>(employment.shopId, employment.salary, 1));
                }
            }
        }).groupBy(0);
        unsortedGrouping.sum(1).andSum(2).map(new MapFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Integer> integerIntegerIntegerTuple3) throws Exception {
                return new Tuple2<>(integerIntegerIntegerTuple3.f0, integerIntegerIntegerTuple3.f1 / integerIntegerIntegerTuple3.f2);
            }
        }).print();
    }


    static class Employment {
        Integer shopId;
        Integer salary;

        public Employment(Integer shopId, Integer salary) {
            this.shopId = shopId;
            this.salary = salary;
        }

        @Override
        public String toString() {
            return "Employment{" +
                    "shopId=" + shopId +
                    ", salary=" + salary +
                    '}';
        }
    }

    static List<Employment> employments = Arrays.asList(new Employment(1, 1000), new Employment(1
            , 100), new Employment(2, 200), new Employment(1, 300));
}
