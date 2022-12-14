package org.apache.iotdb.utils.core.pipeline.out.source;

import org.apache.iotdb.utils.core.model.DeviceModel;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.utils.core.pipeline.context.model.FileSinkStrategyEnum;
import org.apache.iotdb.utils.core.pipeline.PipeSource;
import org.apache.iotdb.utils.core.service.ExportPipelineService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Data
@Slf4j
public class OutSqlDataSource extends PipeSource<String, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

    private String name;

    private Scheduler scheduler;

    private ConcurrentHashMap<String, OutputStream> OUTPUT_STREAM_MAP = new ConcurrentHashMap<>();

    private static final String CATALOG_SQL = "CATALOG_SQL.CATALOG";

    private AtomicLong fileNo = new AtomicLong();

    private ExportPipelineService exportPipelineService;

    private Integer[] totalSize = new Integer[1];

    private int parallelism;

    @Override
    public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
        return flux -> flux
                .flatMap(s-> exportPipelineService.parseFileSinkStrategy(OUTPUT_STREAM_MAP,CATALOG_SQL))
                .flatMap(s-> exportPipelineService.countDeviceNum(s,totalSize))
                .flatMap(s -> exportPipelineService.parseToDeviceModel())
                .parallel(parallelism)
                .runOn(scheduler)
                .flatMap(exportPipelineService::parseTimeseries)
                .flatMap(s-> this.initOutputStream(s,fileNo,OUTPUT_STREAM_MAP))
                .flatMap(exportPipelineService::parseToRowModel)
                .transform(doNext())
                .sequential()
                .doFinally(signalType -> {
                    try {
                        for (String key : OUTPUT_STREAM_MAP.keySet()) {
                            OUTPUT_STREAM_MAP.get(key).flush();
                            OUTPUT_STREAM_MAP.get(key).close();
                        }
                        scheduler.dispose();
                    } catch (IOException e) {
                        log.error("????????????:",e);
                    }
                })
                .contextWrite(context -> {
                    context = context.put("totalSize",totalSize);
                    context = context.put("outputStreamMap", OUTPUT_STREAM_MAP);
                    return context;
                });
    }

    /**
     * ??????????????????????????????outputStream
     * @param pair
     * @return
     */
    public Flux<Pair<DeviceModel, List<String>>> initOutputStream(Pair<DeviceModel,List<String>> pair, AtomicLong fileNo, ConcurrentHashMap<String, OutputStream> outputStreamMap){
        return Flux.deferContextual(contextView -> {
            PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
            ExportModel exportModel = pcontext.getModel();
            FileOutputStream outputStream = null;
            try {
                File file = new File(exportModel.getFileFolder());
                String fileName;
                long no = fileNo.incrementAndGet();
                if(exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG){
                    fileName = exportModel.getFileFolder() + no;
                }else{
                    fileName = exportModel.getFileFolder() + pair.getLeft().getDeviceName();
                }
                fileName = fileName + ".sql";
                file = new File(fileName);
                outputStream = new FileOutputStream(file);
                outputStreamMap.put(String.valueOf(pair.getLeft().getDeviceName()), outputStream);
                if(exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG){
                    StringBuilder catalogRecord = new StringBuilder();
                    catalogRecord.append(no)
                            .append(",")
                            .append(pair.getLeft().getDeviceName())
                            .append("\r\n");
                    outputStreamMap.get("CATALOG").write(catalogRecord.toString().getBytes());
                }
            } catch (IOException e) {
                log.error("????????????:",e);
            }
            return Flux.just(pair);
        });
    }

    public OutSqlDataSource(String name) {
        this(name,Schedulers.DEFAULT_POOL_SIZE);
    }


    public OutSqlDataSource(String name,int parallelism) {
        this.name = name;
        this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
        scheduler = Schedulers.newParallel("pipeline-thread",this.parallelism);
        if(this.exportPipelineService == null){
            this.exportPipelineService = ExportPipelineService.exportPipelineService();
        }
    }
}
