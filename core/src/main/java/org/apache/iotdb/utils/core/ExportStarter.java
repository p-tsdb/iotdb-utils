package org.apache.iotdb.utils.core;

import com.alibaba.fastjson.JSON;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.*;
import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.utils.core.pipeline.out.channel.StringFormatIncludeNullFieldChannel;
import org.apache.iotdb.utils.core.pipeline.out.channel.StringFormatWithoutNullFieldChannel;
import org.apache.iotdb.utils.core.pipeline.out.sink.OutCompressFileSink;
import org.apache.iotdb.utils.core.pipeline.out.sink.OutCsvFileSink;
import org.apache.iotdb.utils.core.pipeline.out.sink.OutSqlFileSink;
import org.apache.iotdb.utils.core.pipeline.out.sink.OutStructureFileSink;
import org.apache.iotdb.utils.core.pipeline.out.source.OutCompressDataSource;
import org.apache.iotdb.utils.core.pipeline.out.source.OutCsvDataSource;
import org.apache.iotdb.utils.core.pipeline.out.source.OutSqlDataSource;
import org.apache.iotdb.utils.core.pipeline.out.source.OutStructureSource;
import reactor.core.Disposable;
import reactor.core.publisher.ParallelFlux;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ExportStarter implements Starter<ExportModel>{

    private List<PipeSink<TimeSeriesRowModel, TimeSeriesRowModel>> pipeSinkList = new ArrayList<>();
    private List<PipeSource> pipeSourceList = new ArrayList<>();

    private CommonPipeline pipeline;

    @Override
    public Disposable start(ExportModel exportModel){

        pipeSinkList.clear();
        pipeSourceList.clear();

        PipelineBuilder builder = new PipelineBuilder();
        String fileFloder = exportModel.getFileFolder();
        if (!fileFloder.endsWith("/") && !fileFloder.endsWith("\\")) {
            fileFloder += File.separator;
        }
        exportModel.setFileFolder(fileFloder);
        File file = new File(fileFloder);
        if(!file.exists()){
            file.mkdirs();
        }
        StringBuilder requestFilePath = new StringBuilder();
        requestFilePath.append(exportModel.getFileFolder())
                .append("REQUEST.json");
        String json = JSON.toJSONString(exportModel);
        try(OutputStreamWriter out = new FileWriter(requestFilePath.toString())){
            out.write(json);
        }catch (IOException e) {
            e.printStackTrace();
        }

        if (exportModel.getNeedTimeseriesStructure()) {
            PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> outStructureFileSink = new OutStructureFileSink("structure sink");
            //pipeSinkList.add(outStructureFileSink);
            builder
                    .source(new OutStructureSource("structure source"))
                    .channel(new StringFormatIncludeNullFieldChannel("structure channel"))
                    .sink(outStructureFileSink);
        }
        PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> pipeSink = generateSink(exportModel);
        pipeSinkList.add(pipeSink);
        PipeSource pipeSource = generateSource(exportModel);
        pipeSourceList.add(pipeSource);
        pipeline = builder
                .source(pipeSource)
                .channel(() -> generateChannel(exportModel))
                .sink(pipeSink)
                .build()
                .withContext(() -> {
                    PipelineContext<ExportModel> context = new PipelineContext<>();
                    context.setModel(exportModel);
                    return context;
                });
        return pipeline.start();
    }

    @Override
    public void shutDown(){
        this.pipeline.shutDown();
    }

    /**
     * ????????????????????????
     * @return
     */
    @Override
    public Double[] rateOfProcess(){
        Double[] rateArray = new Double[2];
        pipeSinkList.forEach(pipesink->{
            Double[] d = pipesink.rateOfProcess();
            rateArray[0] += d[0];
            rateArray[1] += d[1];
        });
        return rateArray;
    }

    /**
     * ????????????????????????
     * @return
     */
    @Override
    public Long finishedRowNum(){
        AtomicLong result = new AtomicLong();
        pipeSinkList.forEach(pipesink->{
            result.addAndGet(pipesink.finishedRowNum());
        });
        return result.get();
    }

    public PipeSource<String, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> generateSource(ExportModel exportModel) {
        PipeSource<String, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> pipeSource;
        switch (exportModel.getCompressEnum()) {
            case SQL:
                pipeSource = new OutSqlDataSource("sql source",exportModel.getParallelism());
                break;
            case CSV:
                pipeSource = new OutCsvDataSource("csv source",exportModel.getParallelism());
                break;
            case SNAPPY:
            case GZIP:
            case LZ4:
                pipeSource = new OutCompressDataSource("compress source",exportModel.getParallelism());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + exportModel.getCompressEnum());
        }
        return pipeSource;
    }

    public PipeChannel<TimeSeriesRowModel, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> generateChannel(ExportModel exportModel) {
        PipeChannel<TimeSeriesRowModel, TimeSeriesRowModel, Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> pipeChannel;
        switch (exportModel.getCompressEnum()) {
            case SQL:
                pipeChannel = new StringFormatWithoutNullFieldChannel("string format without null channel");
                break;
            case CSV:
            case SNAPPY:
            case GZIP:
            case LZ4:
                pipeChannel = new StringFormatIncludeNullFieldChannel("string format channel");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + exportModel.getCompressEnum());
        }
        return pipeChannel;
    }

    public PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> generateSink(ExportModel exportModel) {
        PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> pipeSink;
        switch (exportModel.getCompressEnum()) {
            case SQL:
                pipeSink = new OutSqlFileSink("sql sink");
                break;
            case CSV:
                pipeSink = new OutCsvFileSink("csv sink");
                break;
            case SNAPPY:
            case GZIP:
            case LZ4:
                pipeSink = new OutCompressFileSink("compress sink");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + exportModel.getCompressEnum());
        }
        return pipeSink;
    }
}
