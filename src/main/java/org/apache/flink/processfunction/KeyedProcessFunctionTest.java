package org.apache.flink.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.bean.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author izgnod
 */
public class KeyedProcessFunctionTest {
    private static SensorReading[] data = new SensorReading[]{
            new SensorReading("2", 10L, 1639554687000L),
            new SensorReading("2", 11L, 1639554681000L),
            new SensorReading("2", 12L, 1639554682000L),
            new SensorReading("2", 13L, 1639554683000L),
            new SensorReading("2", 14L, 1639554684000L),
            new SensorReading("2", 15L, 1639554685000L),
            new SensorReading("2", 16L, 1639554686000L),
            new SensorReading("2", 17L, 1639554687000L),
            new SensorReading("2", 18L, 1639554688000L),
            new SensorReading("2", 19L, 1639554689000L),
            new SensorReading("2", 20L, 1639554690000L),
            new SensorReading("2", 21L, 1639554691000L),
            new SensorReading("2", 22L, 1639554682000L),
            new SensorReading("2", 23L, 1639554681000L),
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1);
        env.fromElements(data).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getEventTime();
            }
        }).keyBy((KeySelector<SensorReading, String>) SensorReading::getId).
                process(new TemperatureKeyedProcessFunction()).print();
        env.execute();
    }
}

/**
 * 如果某个传感器的温度在10秒（处理时间）内持续增加，则发出警告；
 */
class TemperatureKeyedProcessFunction extends KeyedProcessFunction<String, SensorReading, String> {

    private ValueState<Long> lastTemp;
    private ValueState<Long> currentTimer;

    @Override
    public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {

        Long preTemp = lastTemp.value();
        lastTemp.update(sensorReading.getTemperature());

        System.out.println("context.timerService().currentWatermark() = " + context.timerService().currentWatermark());

        Long currentTimerTemp = currentTimer.value();
        if (preTemp != null && preTemp > sensorReading.getTemperature()) {
            context.timerService().deleteEventTimeTimer(currentTimer.value());
            currentTimer.clear();
        } else if (currentTimerTemp == null && preTemp != null && preTemp < sensorReading.getTemperature()) {
            long l = context.timerService().currentWatermark() + 10 * 1000;
            context.timerService().registerEventTimeTimer(l);
            currentTimer.update(l);
        }
        TimeUnit.SECONDS.sleep(1);

        collector.collect("id : " + sensorReading.getId() + " temperature: " + sensorReading.getTemperature());
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("fire timestamp = " + timestamp);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Long.class));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
    }
}
