// WindowState.java
package com.iot.pipeline.pipeline;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable sliding-window state for a single device node.
 * NOT thread-safe on its own — callers must synchronize on the instance
 * (done via ConcurrentHashMap.compute() in MultiNodeWindowProcessor).
 */
class WindowState {

    private static final int WINDOW_SIZE = 5;

    // Ring-buffer via bounded deque — avoids index arithmetic.
    private final Deque<Float> window = new ArrayDeque<>(WINDOW_SIZE);

    // AtomicLong so the TTL cleanup job can read lastUpdated without
    // contending on the object monitor held during compute().
    final AtomicLong lastUpdated = new AtomicLong(System.currentTimeMillis());

    /**
     * Push a new temperature sample and return the current window average.
     */
    float addAndAverage(float temperature) {
        if (window.size() == WINDOW_SIZE) {
            window.pollFirst(); // evict oldest
        }
        window.addLast(temperature);
        lastUpdated.set(System.currentTimeMillis());

        float sum = 0f;
        for (float t : window) sum += t;
        return sum / window.size();
    }
}