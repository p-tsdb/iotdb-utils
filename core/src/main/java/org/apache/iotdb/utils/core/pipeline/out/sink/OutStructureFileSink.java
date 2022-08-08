package org.apache.iotdb.utils.core.pipeline.out.sink;

import org.apache.iotdb.utils.core.model.IField;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.PipeSink;
import org.apache.iotdb.utils.core.service.ExportPipelineService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Slf4j
public class OutStructureFileSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

    private String name;

    private ExportPipelineService exportPipelineService;

    private AtomicInteger finishedFileNum = new AtomicInteger();

    private AtomicLong finishedRowNum = new AtomicLong();

    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return sink -> sink
                .transformGroups((Function<GroupedFlux<Integer, TimeSeriesRowModel>, Publisher<? extends TimeSeriesRowModel>>) integerTimeSeriesRowModelGroupedFlux ->
                        integerTimeSeriesRowModelGroupedFlux
                                .buffer(1000, 1000)
                                .flatMap(allList -> {
                                    return Flux.deferContextual(contextView -> {
                                        CSVPrinter[] csvPrinters = contextView.get("csvPrinters");
                                        CSVPrinter printer = csvPrinters[0];
                                        List<List<String>> list = allList
                                                .stream()
                                                .map(s -> generateCsvString(s))
                                                .collect(Collectors.toList());
                                        try {
                                            exportPipelineService.syncPrintRecoreds(printer, list);
                                            printer.flush();
                                        } catch (IOException e) {
                                            log.error("异常信息:", e);
                                        }
                                        finishedRowNum.addAndGet(allList.size());
                                        return Flux.fromIterable(allList);
                                    });
                                })
                )
                .doOnTerminate(() -> {
                    finishedFileNum.incrementAndGet();
                });

    }

    @Override
    public Double[] rateOfProcess() {
        log.info("已经导出文件：{}", finishedFileNum);
        log.info("总文件数：{}", 1);
        Double[] rateDouble = new Double[2];
        rateDouble[0] = finishedFileNum.doubleValue();
        rateDouble[1] = 1d;
        return rateDouble;
    }

    @Override
    public Long finishedRowNum() {
        return finishedRowNum.get();
    }

    public List<String> generateCsvString(TimeSeriesRowModel timeSeriesRowModel) {
        List<String> value = new ArrayList<>();
        for (int i = 0; i < timeSeriesRowModel.getIFieldList().size(); i++) {
            IField iField = timeSeriesRowModel.getIFieldList().get(i);
            if (iField.getField() != null && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
                String valueStr = String.valueOf(iField.getField().getObjectValue(iField.getField().getDataType()));
                value.add(valueStr);
            } else {
                value.add("");
            }
        }

        value.add(new StringBuilder().append("\"").append(timeSeriesRowModel.getDeviceModel().isAligned()).append("\"").toString());
        return value;
    }

    public OutStructureFileSink(String name) {
        this.name = name;
        if (exportPipelineService == null) {
            exportPipelineService = ExportPipelineService.exportPipelineService();
        }
    }
}
