package org.apache.iotdb.utils.core.pipeline;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

@Data
@Slf4j
public abstract class PipeChannel<T,R,V> extends PipeComponent<Function<ParallelFlux<T>,ParallelFlux<R>>,V> {

    @Override
    public Function<ParallelFlux<T>,ParallelFlux<R>> execute() {
        return this.doExecute()
                .andThen(f-> f.doOnError(e-> log.error("异常信息:",e)));
    }

    public abstract Function<ParallelFlux<T>,ParallelFlux<R>> doExecute();

}
