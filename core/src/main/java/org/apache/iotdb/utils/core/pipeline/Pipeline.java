package org.apache.iotdb.utils.core.pipeline;

import reactor.core.Disposable;

import java.io.IOException;

/**
 *
 */
public interface Pipeline {

    Disposable start() throws IOException;

    void shutDown();

}
