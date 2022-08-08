package org.apache.iotdb.utils.core.pipeline.in.channel;

import org.apache.iotdb.utils.core.model.IField;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.PipeChannel;
import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ImportModel;
import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.List;
import java.util.function.Function;

/**
 *
 */
@Data
public class FieldColumnFormatChannel extends PipeChannel<TimeSeriesRowModel, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

    private String name;

    public FieldColumnFormatChannel(String name) {
        this.name = name;
    }

    /**
     * 把 root.test.test.cli.ali.dd 格式的timeseries，转化为 dd，即不带前缀的格式
     * @return
     */
    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return flux -> flux
                .flatMap(s -> {
                    return Flux.deferContextual(contextView -> {
                        PipelineContext<ImportModel> pcontext = contextView.get("pipelineContext");
                        ImportModel importModel = pcontext.getModel();
                        List<IField> list = s.getIFieldList();
                        list.stream().forEach(ifield->{
                            if(ifield.getField() !=null){
                                StringBuilder builder = new StringBuilder();
                                builder.append(s.getDeviceModel().getDeviceName())
                                        .append(".");
                                ifield.setColumnName(ifield.getColumnName().replace(builder.toString(),""));
                            }
                        });
                        return ParallelFlux.from(Flux.just(s));
                    });
                })
                .transform(doNext());
    }
}
