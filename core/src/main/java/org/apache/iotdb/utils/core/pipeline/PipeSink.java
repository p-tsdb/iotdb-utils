package org.apache.iotdb.utils.core.pipeline;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

@Slf4j
public abstract class PipeSink<T,R> implements Component <Function<ParallelFlux<T>,ParallelFlux<R>>> {

    @Override
    public Function<ParallelFlux<T>,ParallelFlux<R>> execute() {
        return doExecute()
                .andThen(f-> f.doOnError(e-> log.error("异常信息:",e)));
    }

    public abstract Function<ParallelFlux<T>, ParallelFlux<R>> doExecute();

    public abstract Double[] rateOfProcess();

    public abstract Long finishedRowNum();

}
