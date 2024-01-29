package org.cardinal.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.cardinal.functions.MaintainLastAverage;

import java.util.Arrays;
import java.util.List;

/**
 * The StreamUtil class provides utility methods for creating and processing data streams
 * in the Flink application. It includes methods for reading data, calculating moving averages,
 * and joining streams for analysis.
 */
public class StreamUtil {
    public static final String TARGET_SYMBOL = "AAPL";
    public static final List<String> SUBSCRIBED_SYMBOL_LIST = Arrays.asList("AAPL", "GOOG", "MSFT", "TSLA");
    public static final String CONSTANT_KEY = "constantKey";
    public static final int SLIDING_WINDOW_RANGE = 60;
    public static final int SLIDE_INTERVAL = 30;
    public static final String TRADES_FILE_PATH = "/home/vishal/trades.csv";
    public static final String FLINK_CONFIG_PATH = "flink-config.properties";
    public static final String CHECKPOINT_INTERVAL_KEY = "checkpoint.interval";
    public static final String CHECKPOINT_MODE_KEY = "checkpoint.mode";
    public static final String CHECKPOINT_PATH_KEY = "checkpoint.path";
    public static final String CHECKPOINT_CONCURRENT_KEY = "checkpoint.max.concurrent";

    /**
     * Processes a given data stream to calculate moving averages and maintain the last average state.
     *
     * @param movingAverageStream The data stream of Double values representing moving averages.
     * @return A processed data stream containing the current and last averages, along with a constant key.
     */
    public static DataStream<Tuple3<Double, Double, String>> getProcessedDataStream(SingleOutputStreamOperator<Double> movingAverageStream) {
        return movingAverageStream.map(new MapFunction<Double, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Double value) {
                return new Tuple2<>("constantKey", value);
            }
        }).keyBy(value -> value.f0).map(new MaintainLastAverage());
    }

    /**
     * Joins two processed data streams to generate buy or sell signals based on the comparison of current and last averages.
     *
     * @param processedTargetStream     A processed data stream for the target symbol.
     * @param processedNonTargetsStream A processed data stream for other symbols.
     * @return A data stream of Strings representing buy or sell signals.
     */
    public static DataStream<String> getBuySellSignals(
            DataStream<Tuple3<Double, Double, String>> processedTargetStream,
            DataStream<Tuple3<Double, Double, String>> processedNonTargetsStream) {
        return processedTargetStream
                .join(processedNonTargetsStream)
                .where(value -> value.f2)
                .equalTo(value -> value.f2)
                .window(SlidingEventTimeWindows.of(Time.seconds(SLIDE_INTERVAL), Time.seconds(SLIDE_INTERVAL)))
                .apply(new JoinFunction<Tuple3<Double, Double, String>, Tuple3<Double, Double, String>, String>() {
                    @Override
                    public String join(Tuple3<Double, Double, String> eth, Tuple3<Double, Double, String> other) {
                        Double currentTargetAvg = eth.f0;
                        Double lastTargetAvg = eth.f1;
                        Double currentOtherAvg = other.f0;
                        Double lastOtherAvg = other.f1;

                        if (currentTargetAvg != null && currentOtherAvg != null && lastTargetAvg != null && lastOtherAvg != null) {
                            if (currentTargetAvg > lastTargetAvg && currentOtherAvg > lastOtherAvg) {
                                String message = "BUY Signal triggered with currentTargetAvgPrice: " + currentTargetAvg
                                        + " lastTargetAvgPrice: " + lastTargetAvg + " currentOtherStockAvgPrice: "
                                        + currentOtherAvg + " lastOtherStockAvgPrice: " + lastOtherAvg;
                                System.out.println(message);
                                return message;
                            } else {
                                String message = "SELL Signal triggered with currentTargetAvgPrice: " + currentTargetAvg
                                        + " lastTargetAvgPrice: " + lastTargetAvg + " currentOtherStockAvgPrice: "
                                        + currentOtherAvg + " lastOtherStockAvgPrice: " + lastOtherAvg;
                                System.out.println(message);
                                return message;
                            }
                        }
                        return null;
                    }
                });
    }
}
