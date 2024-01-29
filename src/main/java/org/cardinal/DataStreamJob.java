package org.cardinal;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.cardinal.functions.AverageAggregate;
import org.cardinal.functions.ConditionalAverageAggregate;
import org.cardinal.model.Trade;
import org.cardinal.source.CSVSourceFunction;
import org.cardinal.util.StreamUtil;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Objects;

import static org.cardinal.util.StreamUtil.CHECK_POINTS_INTERVAL;
import static org.cardinal.util.StreamUtil.CHECK_POINTS_PATH;
import static org.cardinal.util.StreamUtil.CONSTANT_KEY;
import static org.cardinal.util.StreamUtil.MAX_CONCURRENT_CHECK_POINTS;
import static org.cardinal.util.StreamUtil.SLIDE_INTERVAL;
import static org.cardinal.util.StreamUtil.SLIDING_WINDOW_RANGE;
import static org.cardinal.util.StreamUtil.TARGET_SYMBOL;

/**
 * The DataStreamJob class is responsible for setting up and executing the Apache Flink data stream processing job.
 * It reads trade data from a CSV file, computes moving averages for a target symbol and other symbols,
 * and then performs a join to generate buy or sell signals based on these averages.
 */
public class DataStreamJob {

    /**
     * The main method sets up the streaming environment, defines the data processing pipeline,
     * and starts the execution of the stream processing job.
     *
     * @param args Command line arguments (not used).
     * @throws Exception if there is an issue in setting up or executing the Flink job.
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECK_POINTS_INTERVAL);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECK_POINTS);
        env.getCheckpointConfig().setCheckpointStorage(CHECK_POINTS_PATH);

        SingleOutputStreamOperator<Trade> quoteStream = env
                .addSource(new CSVSourceFunction(StreamUtil.TRADES_FILE_PATH))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp().
                                atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                );

        // Generate moving average stream data based on sliding window for target symbol
        SingleOutputStreamOperator<Double> targetMovingAverageStream = quoteStream
                .filter(quote -> TARGET_SYMBOL.equals(quote.getSymbol()))
                .map(Trade::getPrice)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(SLIDING_WINDOW_RANGE), Time.seconds(SLIDE_INTERVAL)))
                .aggregate(new AverageAggregate());


        // Generate moving average stream data based on sliding window for non target symbols
        KeyedStream<Trade, String> keyedNonTargetStream = quoteStream
                .filter(quote -> !TARGET_SYMBOL.equals(quote.getSymbol()))
                .keyBy(quote -> CONSTANT_KEY);

        SingleOutputStreamOperator<Double> nonTargetMovingAverageStream = keyedNonTargetStream
                .window(SlidingEventTimeWindows.of(Time.seconds(SLIDING_WINDOW_RANGE), Time.seconds(SLIDE_INTERVAL)))
                .aggregate(new ConditionalAverageAggregate());


        DataStream<Tuple3<Double, Double, String>> processedTargetStream = StreamUtil.getProcessedDataStream(targetMovingAverageStream);

        DataStream<Tuple3<Double, Double, String>> processedNonTargetsStream = StreamUtil.getProcessedDataStream(nonTargetMovingAverageStream);

        DataStream<String> buySellSignals = StreamUtil.getBuySellSignals(processedTargetStream, processedNonTargetsStream);

        env.execute("Market Data Analysis");
    }
}
