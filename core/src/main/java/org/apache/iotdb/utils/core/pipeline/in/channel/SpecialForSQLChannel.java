package org.apache.iotdb.utils.core.pipeline.in.channel;

import org.apache.iotdb.utils.core.pipeline.PipeChannel;
import lombok.Data;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

/**
 * 转化数据，主要处理字符串
 */
@Data
public class SpecialForSQLChannel extends PipeChannel<String, String, Function<ParallelFlux<String>, ParallelFlux<String>>> {

    private String name;

    public SpecialForSQLChannel(String name) {
        this.name = name;
    }

    @Override
    public Function<ParallelFlux<String>, ParallelFlux<String>> doExecute() {
        return flux -> flux
                .transform(doNext());
    }
}
