package org.apache.iotdb.utils.core.pipeline.out.sink;

import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.utils.core.pipeline.PipeSink;
import org.apache.iotdb.utils.core.service.ExportPipelineService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Slf4j
public class OutCompressFileSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

    private String name;

    private ExportPipelineService exportPipelineService;

    private AtomicInteger finishedFileNum = new AtomicInteger();

    private int totalFileNum;

    private AtomicLong finishedRowNum = new AtomicLong();

    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return sink -> sink
                .transformGroups((Function<GroupedFlux<Integer, TimeSeriesRowModel>, Publisher<? extends TimeSeriesRowModel>>) integerTimeSeriesRowModelGroupedFlux ->
                        integerTimeSeriesRowModelGroupedFlux
                                .buffer(15000, 15000)
                                .flatMap(allList -> {
                                    return Flux.deferContextual(contextView -> {
                                        ConcurrentHashMap<String, OutputStream> outputStreamMap = contextView.get("outputStreamMap");
                                        Integer[] totalSize = contextView.get("totalSize");
                                        totalFileNum = totalSize[0];
                                        PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
                                        ExportModel exportModel = pcontext.getModel();
                                        Map<String, List<TimeSeriesRowModel>> groupMap = allList.stream()
                                                .collect(Collectors.toMap(k -> k.getDeviceModel().getDeviceName(), p -> {
                                                    List<TimeSeriesRowModel> result = new ArrayList();
                                                    result.add(p);
                                                    return result;
                                                }, (o, n) -> {
                                                    o.addAll(n);
                                                    return o;
                                                }));
                                        for (String groupKey : groupMap.keySet()) {
                                            if (groupKey.startsWith("finish")) {
                                                finishedFileNum.incrementAndGet();
                                                continue;
                                            }
                                            //TODO: ???????????????outstream??????  exportModel.getcompressEnum
                                            List<TimeSeriesRowModel> groupList = groupMap.get(groupKey);
                                            OutputStream sout = outputStreamMap.get(groupKey);
                                            exportPipelineService.compressBlock(groupList,sout,exportModel);
                                            finishedRowNum.addAndGet(groupList.size());
                                        }
                                        return Flux.fromIterable(allList);
                                    });
                                })
                ).flatMap(timeSeriesRowModel -> {
                    return Flux.deferContextual(contextView -> {
                        String deviceName = timeSeriesRowModel.getDeviceModel().getDeviceName();
                        if (deviceName.startsWith("finish")) {
                            ConcurrentHashMap<String, OutputStream> outputStreamMap = contextView.get("outputStreamMap");
                            deviceName = deviceName.substring(deviceName.indexOf(",")+1,deviceName.length());
                            OutputStream outputStream = outputStreamMap.get(deviceName);
                            outputStreamMap.remove(deviceName);
                            try {
                                outputStream.flush();
                                outputStream.close();
                            } catch (IOException e) {
                                log.error("outputStream ???????????????",e);
                            }
                        }
                        return Flux.just(timeSeriesRowModel);
                    });
                });
    }

    @Override
    public Double[] rateOfProcess() {
        log.info("?????????????????????{}",finishedFileNum);
        log.info("???????????????{}",totalFileNum);
        Double[] rateDouble = new Double[2];
        rateDouble[0] = finishedFileNum.doubleValue();
        rateDouble[1] = Double.parseDouble(String.valueOf(totalFileNum));
        return rateDouble;
    }

    @Override
    public Long finishedRowNum(){
        return finishedRowNum.get();
    }

    public OutCompressFileSink(String name) {
        this.name = name;
        if (this.exportPipelineService == null) {
            //TODO??? ??????????????????
            this.exportPipelineService = ExportPipelineService.exportPipelineService();
        }
    }
}
