/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.udf;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import static java.util.Objects.requireNonNull;

/**
 * A variant of the collection source (emits a sequence of elements as a stream)
 * that supports throttling the emission rate.
 *
 * @param <T>
 */
public class ThrottledIterator<T> implements Iterator<T>, Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final Iterator<T> source;

    private final long sleepBatchSize;
    private final long sleepBatchTime;

    private long thisSpeedSleepBatchTime;
    private long thisSpeedRemain = 0;

    private long lastBatchCheckTime;
    private long num;

    private Random random = new Random(1000);

    public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
        this.source = requireNonNull(source);

        if (!(source instanceof Serializable)) {
            throw new IllegalArgumentException("source must be java.io.Serializable");
        }

        if (elementsPerSecond >= 1e8) {
            sleepBatchSize = 0;
            sleepBatchTime = 0;
        } else if (elementsPerSecond >= 100) {
            // how many elements would we emit per 50ms
            this.sleepBatchSize = elementsPerSecond / 20;
            this.sleepBatchTime = 50;
        } else if (elementsPerSecond >= 1) {
            // how long does element take
            this.sleepBatchSize = 1;
            this.sleepBatchTime = 1000 / elementsPerSecond;
        } else {
            throw new IllegalArgumentException("'elements per second' must be positive and not zero");
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        // delay if necessary
        if (lastBatchCheckTime > 0) {
            if (++num >= sleepBatchSize) {
                num = 0;

                final long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchCheckTime;

                // check if we need to change the time
                if (thisSpeedRemain <= 0) {
                    double gg = random.nextGaussian();
                    thisSpeedSleepBatchTime = (long) (sleepBatchTime + gg * (sleepBatchTime * 0.2));
                    if (thisSpeedSleepBatchTime <= 0) {
                        thisSpeedSleepBatchTime = 1;
                    }

                    thisSpeedRemain = Math.max(5 * 60 * 1000L / sleepBatchTime, 1);
                }

                thisSpeedRemain -= 1;

                if (elapsed < thisSpeedSleepBatchTime) {
                    try {
                        Thread.sleep(thisSpeedSleepBatchTime - elapsed);
                    } catch (InterruptedException e) {
                        // restore interrupt flag and proceed
                        Thread.currentThread().interrupt();
                    }
                }
                lastBatchCheckTime = System.currentTimeMillis();
            }
        } else {
            lastBatchCheckTime = System.currentTimeMillis();
        }

        return source.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
