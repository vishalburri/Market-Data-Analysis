package org.cardinal.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * A Flink RichMapFunction that maintains the last average value of a stream.
 * It updates its state with each new value and emits a Tuple3 containing the current value,
 * the last average, and a constant key.
 */
public class MaintainLastAverage extends RichMapFunction<Tuple2<String, Double>, Tuple3<Double, Double, String>> {
    private transient ValueState<Double> lastAverageState;

    /**
     * Initializes the state for maintaining the last average value.
     *
     * @param parameters Configuration parameters for the function.
     * @throws Exception If initialization fails.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        lastAverageState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAverage", Double.class));
    }

    /**
     * Processes each incoming Tuple2 element, updates the state with the current average,
     * and returns a new Tuple3 containing the current and last average values along with the key.
     *
     * @param current The current Tuple2 element being processed.
     * @return A new Tuple3 with the current value, last average, and key.
     * @throws Exception If processing fails.
     */
    @Override
    public Tuple3<Double, Double, String> map(Tuple2<String, Double> current) throws Exception {
        Double lastAverage = lastAverageState.value();
        // Check if lastAverage is null and handle accordingly
        lastAverage = (lastAverage == null) ? 0.0 : lastAverage;

        lastAverageState.update(current.f1);
        return new Tuple3<>(current.f1, lastAverage, current.f0); // current.f1 is the current average, lastAverage is the last average, current.f0 is the key
    }

}
