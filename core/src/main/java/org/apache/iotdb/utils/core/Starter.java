package org.apache.iotdb.utils.core;

import org.apache.iotdb.utils.core.pipeline.context.model.PipelineModel;
import reactor.core.Disposable;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/26 12:37
 */
public interface Starter<T extends PipelineModel> {

    Disposable start(T model);

    void shutDown();

    Double[] rateOfProcess();

    Long finishedRowNum();
}
