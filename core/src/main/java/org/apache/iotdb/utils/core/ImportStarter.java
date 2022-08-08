package org.apache.iotdb.utils.core;

import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.CommonPipeline;
import org.apache.iotdb.utils.core.pipeline.PipelineBuilder;
import org.apache.iotdb.utils.core.pipeline.PipeChannel;
import org.apache.iotdb.utils.core.pipeline.in.channel.FieldColumnFormatChannel;
import org.apache.iotdb.utils.core.pipeline.in.channel.SpecialForSQLChannel;
import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.utils.core.pipeline.PipeSink;
import org.apache.iotdb.utils.core.pipeline.in.sink.InRowModelFileSink;
import org.apache.iotdb.utils.core.pipeline.in.sink.InSqlFileSink;
import org.apache.iotdb.utils.core.pipeline.in.sink.InStructureFileSink;
import org.apache.iotdb.utils.core.pipeline.PipeSource;
import org.apache.iotdb.utils.core.pipeline.in.source.InCompressDataSource;
import org.apache.iotdb.utils.core.pipeline.in.source.InCsvDataSource;
import org.apache.iotdb.utils.core.pipeline.in.source.InSqlDataSource;
import org.apache.iotdb.utils.core.pipeline.in.source.InStructureSource;
import reactor.core.Disposable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ImportStarter implements Starter<ImportModel>{

    private List<PipeSink<TimeSeriesRowModel, TimeSeriesRowModel>> pipeSinkList = new ArrayList<>();

    private List<PipeSource> pipeSourceList = new ArrayList<>();

    private CommonPipeline pipeline;

    @Override
    public Disposable start(ImportModel importModel){

        pipeSinkList.clear();
        pipeSourceList.clear();

        PipelineBuilder builder = new PipelineBuilder();
        String fileFloder = importModel.getFileFolder();
        if (!fileFloder.endsWith("/") && !fileFloder.endsWith("\\")) {
            fileFloder += File.separator;
        }
        importModel.setFileFolder(fileFloder);
        if(importModel.getNeedTimeseriesStructure()){
            PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> iStructureFileSink = new InStructureFileSink("structure sink");
            //pipeSinkList.add(iStructureFileSink);
            builder
                    .source(new InStructureSource("structure source"))
                    .channel(new FieldColumnFormatChannel("structure channel"))
                    .sink(iStructureFileSink);
        }
        PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> pipeSink = generateSink(importModel);
        pipeSinkList.add(pipeSink);

        PipeSource pipeSource = generateSource(importModel);
        pipeSourceList.add(pipeSource);
        pipeline = builder
                .source(() -> generateSource(importModel))
                .channel(() -> generateChannel(importModel))
                .sink(pipeSink)
                .build()
                .withContext(() -> {
                    PipelineContext context = new PipelineContext();
                    context.setModel(importModel);
                    return context;
                });
        return pipeline.start();
    }

    @Override
    public void shutDown(){
        this.pipeline.shutDown();
    }

    /**
     * 查看导出文件进度
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
     * 查看文件导出行数
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

    public PipeSource generateSource(ImportModel importModel) {
        PipeSource pipeSource;
        switch (importModel.getCompressEnum()) {
            case SQL:
                pipeSource = new InSqlDataSource("sql source",importModel.getParallelism());
                break;
            case CSV:
                pipeSource = new InCsvDataSource("csv source",importModel.getParallelism());
                break;
            case SNAPPY:
            case GZIP:
            case LZ4:
                pipeSource = new InCompressDataSource("compress source",importModel.getParallelism());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + importModel.getCompressEnum());
        }
        return pipeSource;
    }

    public PipeChannel generateChannel(ImportModel importModel) {
        PipeChannel pipeChannel;
        switch (importModel.getCompressEnum()) {
            case SQL:
                pipeChannel = new SpecialForSQLChannel("sql channel");
                break;
            case CSV:
            case SNAPPY:
            case GZIP:
            case LZ4:
                pipeChannel = new FieldColumnFormatChannel("csv channel");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + importModel.getCompressEnum());
        }
        return pipeChannel;
    }

    public PipeSink generateSink(ImportModel importModel) {
        PipeSink pipeSink;
        switch (importModel.getCompressEnum()) {
            case SQL:
                pipeSink = new InSqlFileSink("sql sink");
                break;
            case CSV:
            case SNAPPY:
            case GZIP:
            case LZ4:
                pipeSink = new InRowModelFileSink("compress sink");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + importModel.getCompressEnum());
        }
        return pipeSink;
    }
}
