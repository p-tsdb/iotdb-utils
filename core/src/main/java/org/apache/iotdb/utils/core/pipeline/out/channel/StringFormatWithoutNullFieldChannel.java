package org.apache.iotdb.utils.core.pipeline.out.channel;

import org.apache.iotdb.utils.core.model.IField;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.PipeChannel;
import lombok.Data;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

/**
 * 转化数据，主要处理字符串
 */
@Data
public class StringFormatWithoutNullFieldChannel extends PipeChannel<TimeSeriesRowModel, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

    private String name;

    public StringFormatWithoutNullFieldChannel(String name) {
        this.name = name;
    }

    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return flux -> flux
                .flatMap(s -> {
                    s.getIFieldList()
                            .forEach(this::sqlFileTransformer);
                    return ParallelFlux.from(Flux.just(s));
                })
                .transform(doNext());
    }


    private void sqlFileTransformer(IField iField){
        if (iField.getField() != null && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
            if (iField.getField().getDataType() == TSDataType.TEXT) {
                String value = iField.getField().getStringValue();
                if(value.indexOf('\"') >=0){
                    value = value.replaceAll("\"","\\\"");
                }
                StringBuilder builder = new StringBuilder();
                builder.append("\"")
                        .append(value)
                        .append("\"");
                iField.getField().setBinaryV(Binary.valueOf(builder.toString()));
            }
        } else {
            iField.setField(null);
        }
    }
}
