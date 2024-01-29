package org.cardinal.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * An AggregateFunction that calculates the average of Double values.
 * It uses a Tuple2<Double, Integer> as an accumulator, where the first element is the sum of values,
 * and the second element is the count of values.
 */
public class AverageAggregate implements AggregateFunction<Double, Tuple2<Double, Integer>, Double> {

    /**
     * Creates a new accumulator to hold the sum and count of Double values.
     *
     * @return A Tuple2<Double, Integer> initialized to (0.0, 0).
     */
    @Override
    public Tuple2<Double, Integer> createAccumulator() {
        return new Tuple2<>(0.0, 0);
    }

    /**
     * Adds a new Double value to the accumulator.
     *
     * @param value The value to add.
     * @param accumulator The current accumulator state.
     * @return The updated accumulator with the new sum and count.
     */
    @Override
    public Tuple2<Double, Integer> add(Double value, Tuple2<Double, Integer> accumulator) {
        return new Tuple2<>(accumulator.f0 + value, accumulator.f1 + 1);
    }

    /**
     * Computes the average from the accumulator.
     *
     * @param accumulator The accumulator containing the sum and count of values.
     * @return The computed average, or 0.0 if no values have been added.
     */
    @Override
    public Double getResult(Tuple2<Double, Integer> accumulator) {
        if (accumulator.f1 == 0) {
            return 0.0;
        } else {
            return accumulator.f0 / accumulator.f1;
        }
    }

    /**
     * Merges two accumulators in distributed computations.
     *
     * @param a The first accumulator.
     * @param b The second accumulator.
     * @return The merged accumulator with combined sum and count.
     */
    @Override
    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
