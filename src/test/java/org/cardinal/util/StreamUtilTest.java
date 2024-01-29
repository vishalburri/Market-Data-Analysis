package org.cardinal.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

class StreamUtilTest {

    @Test
    void getProcessedDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test data stream
        SingleOutputStreamOperator<Double> testStream = env.fromElements(1.0, 2.0, 3.0);

        DataStream<Tuple3<Double, Double, String>> processedStream = StreamUtil.getProcessedDataStream(testStream);

        List<Tuple3<Double, Double, String>> result = processedStream.executeAndCollect(3);

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(new Tuple3<>(1.0, 0.0, "constantKey"), result.get(0));
        Assertions.assertEquals(new Tuple3<>(2.0, 1.0, "constantKey"), result.get(1));
        Assertions.assertEquals(new Tuple3<>(3.0, 2.0, "constantKey"), result.get(2));
    }

    @Test
    public void testGetBuySellSignals() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<Tuple3<Double, Double, String>> watermarkStrategy = WatermarkStrategy
                .<Tuple3<Double, Double, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

        // Create test data streams for target and non-target symbols
        DataStream<Tuple3<Double, Double, String>> processedTargetStream = env.fromElements(
                new Tuple3<>(2.0, 1.5, StreamUtil.CONSTANT_KEY),
                new Tuple3<>(1.8, 2.0, StreamUtil.CONSTANT_KEY)
        ).assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Tuple3<Double, Double, String>> processedNonTargetsStream = env.fromElements(
                new Tuple3<>(3.0, 2.5, StreamUtil.CONSTANT_KEY),
                new Tuple3<>(2.2, 3.0, StreamUtil.CONSTANT_KEY)
        ).assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<String> buySellSignals = StreamUtil.getBuySellSignals(processedTargetStream, processedNonTargetsStream);

        List<String> result = buySellSignals.executeAndCollect(2);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("BUY Signal triggered with currentTargetAvgPrice: 2.0 lastTargetAvgPrice: 1.5" +
                " currentOtherStockAvgPrice: 3.0 lastOtherStockAvgPrice: 2.5", result.get(0));
        Assertions.assertEquals("SELL Signal triggered with currentTargetAvgPrice: 2.0 lastTargetAvgPrice: 1.5 " +
                "currentOtherStockAvgPrice: 2.2 lastOtherStockAvgPrice: 3.0", result.get(1));
    }

}