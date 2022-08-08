package org.apache.iotdb.utils.core.pipeline;

/**
 *
 * @param <T>
 */

public interface Component<T> {
    T execute();
}
