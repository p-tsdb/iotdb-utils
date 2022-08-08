package org.apache.iotdb.utils.core.pipeline.out.channel;

import org.apache.iotdb.utils.core.model.IField;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.PipeChannel;
import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.utils.core.service.ExportPipelineService;
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
public class StringFormatIncludeNullFieldChannel extends PipeChannel<TimeSeriesRowModel, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

    private String name;

    private ExportPipelineService exportPipelineService;

    public StringFormatIncludeNullFieldChannel(String name) {
        this.name = name;
        if(this.exportPipelineService == null){
            exportPipelineService = ExportPipelineService.exportPipelineService();
        }
    }

    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return flux -> flux
                .flatMap(s -> {
                    return Flux.deferContextual(contextView -> {
                        PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
                        ExportModel exportModel = pcontext.getModel();
                        s.getIFieldList()
                                .stream()
                                .forEach(iField -> {
                                    otherFileTransformer(iField);
                                });
                        return ParallelFlux.from(Flux.just(s));
                    });
                })
                .transform(doNext());
    }

    private void otherFileTransformer(IField iField){
        if (iField.getField() != null && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
            if (iField.getField().getDataType() == TSDataType.TEXT) {
                StringBuilder value = new StringBuilder();
                value.append("\"")
                        .append(iField.getField().getStringValue())
                        .append("\"");
                iField.getField().setBinaryV(Binary.valueOf(value.toString()));
            }
        }
    }
}
