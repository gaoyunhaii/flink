package org.apache.flink.streaming.tests.udf;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class RandomNumberSourceFunction extends RichParallelSourceFunction<Integer> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RandomNumberSourceFunction.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private transient ListState<Tuple2<Integer, Long>> checkpointedState;

    private final RandomNumberSourceIterator numberSourceIterator;
    private final ThrottledIterator<Integer> throttledIterator;

    private long maxCount = -1;

    //--- initialized state ---//
    private transient int lastNumber = -1;
    private transient long currentCount = 0;

    public RandomNumberSourceFunction(int largest, int seed, long rate, long maxCount) {
        this.maxCount = maxCount;

        this.numberSourceIterator = new RandomNumberSourceIterator(largest, seed);
        this.throttledIterator = new ThrottledIterator<>(this.numberSourceIterator, rate);
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (isRunning && (maxCount < 0 || currentCount < maxCount)) {
            Integer number = throttledIterator.next();

            synchronized (sourceContext.getCheckpointLock()) {
                if (number == null) {
                    break;
                }

                lastNumber = number;
                ++currentCount;

                // Reset the seed to a value related to states
                this.numberSourceIterator.resetSeed(lastNumber + (int) currentCount);
                sourceContext.collect(number);
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Preconditions.checkState(this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState = context.getOperatorStateStore().getOperatorState(
                new ListStateDescriptor<>(
                        "number-source-state",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {
                        }).createSerializer(getRuntimeContext().getExecutionConfig())
                )
        );

        if (context.isRestored()) {
            Tuple2 state = this.checkpointedState.get().iterator().next();
            this.lastNumber = (int) state.f0;
            this.currentCount = (long) state.f1;
        } else {
            this.lastNumber = -1;
            this.currentCount = 0;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(new Tuple2<>(lastNumber, currentCount));
    }

    class RandomNumberSourceIterator implements Iterator<Integer>, Serializable {
        private int largest = 10;
        private Random rnd;

        public RandomNumberSourceIterator(int largest, int seed) {
            this.largest = largest;
            this.rnd = new Random(seed);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Integer next() {
            Integer value = rnd.nextInt(largest + 1);
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public void resetSeed(int newSeed) {
            this.rnd = new Random(newSeed);
        }
    }
}
