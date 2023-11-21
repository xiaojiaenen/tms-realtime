package tms.realtime.app.func;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author xiaojia
 * @date 2023/11/14 21:21
 * @desc 聚合逻辑实现
 */
public abstract class MyAggregationFunction<T> implements AggregateFunction<T, T, T> {
    @Override
    public T createAccumulator() {
        return null;
    }

    @Override
    public T getResult(T accumulator) {
        return accumulator;
    }

    @Override
    public T merge(T a, T b) {
        return null;
    }
}
