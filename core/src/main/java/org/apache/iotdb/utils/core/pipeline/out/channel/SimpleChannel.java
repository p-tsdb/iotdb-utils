package org.apache.iotdb.utils.core.pipeline.out.channel;

import org.apache.iotdb.utils.core.model.IField;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.PipeChannel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/19 11:20
 */
public class SimpleChannel extends PipeChannel<TimeSeriesRowModel, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

    private String name;

    public SimpleChannel(String name) {
        this.name = name;
    }

    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return flux -> flux
                .flatMap(s -> {
//                    s.getIFieldList()
//                            .forEach(this::sqlFileTransformer);
                    return ParallelFlux.from(Flux.just(s));
                })
                .transform(doNext());
    }


    private void sqlFileTransformer(IField iField){
        if (iField.getField() != null && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
        } else {
            iField.setField(null);
        }
    }
}
