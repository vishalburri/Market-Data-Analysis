package org.cardinal.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.cardinal.model.Trade;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An AggregateFunction for computing the average trade price conditionally across multiple trades.
 * It aggregates trade data by symbol and computes the average trade price for each symbol.
 */
public class ConditionalAverageAggregate implements AggregateFunction<Trade, Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer>, Double> {

    /**
     * Creates an accumulator for the aggregate function.
     * @return A new accumulator tuple.
     */
    @Override
    public Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> createAccumulator() {
        return new Tuple3<>(new HashMap<>(), new HashMap<>(), 0);
    }

    /**
     * Adds a trade to the accumulator.
     * @param value The trade to add.
     * @param accumulator The current accumulator state.
     * @return The updated accumulator.
     */
    @Override
    public Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> add(Trade value, Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> accumulator) {
        String symbol = value.getSymbol();
        double avgPrice = value.getPrice();

        if (!accumulator.f0.containsKey(symbol)) {
            accumulator.f2++;
        }
        accumulator.f0.put(symbol, avgPrice);  // Store latest average price
        accumulator.f1.computeIfAbsent(symbol, k -> new ArrayList<>()).add(avgPrice);  // Accumulate all averages


        return accumulator;
    }

    /**
     * Retrieves the result of the aggregation from the accumulator.
     * @param accumulator The accumulator of the aggregate function.
     * @return The computed average value.
     */
    @Override
    public Double getResult(Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> accumulator) {
        double sumIndividualAverages = 0.0;
        int numCryptos = accumulator.f0.size();

        for (Map.Entry<String, List<Double>> entry : accumulator.f1.entrySet()) {
            double individualAverage = entry.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            sumIndividualAverages += individualAverage;
        }

        return sumIndividualAverages / numCryptos;  // Calculate average of averages
    }

    /**
     * Merges two accumulators in the case of distributed computations.
     * @param a The first accumulator.
     * @param b The second accumulator.
     * @return The merged accumulator.
     */
    @Override
    public Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> merge(Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> a, Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> b) {
        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> mergedAccumulator = new Tuple3<>();
        mergedAccumulator.f0 = new HashMap<>();
        mergedAccumulator.f0.putAll(a.f0);
        mergedAccumulator.f0.putAll(b.f0);
        mergedAccumulator.f1 = new HashMap<>();
        mergedAccumulator.f1.putAll(a.f1);
        mergedAccumulator.f1.putAll(b.f1);
        mergedAccumulator.f2 = a.f2 + b.f2;

        return mergedAccumulator;
    }
}
