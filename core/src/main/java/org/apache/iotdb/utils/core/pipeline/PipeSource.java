package org.apache.iotdb.utils.core.pipeline;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.util.function.Function;

@Slf4j
public abstract class PipeSource <T,R,V> extends PipeComponent<Function<Flux<T>,Flux<R>>,V> {


    @Override
    public Function<Flux<T>, Flux<R>> execute() {
        return this.doExecute()
                .andThen(f-> f.doOnError(e-> log.error("异常信息:",e)));
    }

    public abstract Function<Flux<T>,Flux<R>> doExecute();

}
