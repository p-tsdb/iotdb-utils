package org.apache.iotdb.utils.core.pipeline.in.sink;

import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.utils.core.pipeline.PipeSink;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Data
@Slf4j
public class InSqlFileSink extends PipeSink<String, String> {

    private String name;

    private AtomicInteger finishedFileNum = new AtomicInteger();

    private Integer[] totalFileNum = new Integer[1];

    private AtomicLong finishedRowNum = new AtomicLong();

    @Override
    public Function<ParallelFlux<String>, ParallelFlux<String>> doExecute() {
        return sink -> sink
                .flatMap(s -> {
                    return Flux.deferContextual(contextView -> {
                        PipelineContext<ImportModel> pcontext = contextView.get("pipelineContext");
                        totalFileNum = contextView.get("totalSize");
                        ImportModel importModel = pcontext.getModel();
                        Session session = importModel.getSession();
                        try {
                            if(s.startsWith("finish")){
                                finishedFileNum.incrementAndGet();
                            }else{
                                session.executeNonQueryStatement(s);
                                finishedRowNum.incrementAndGet();
                            }
                        } catch (StatementExecutionException | IoTDBConnectionException e) {
                            log.error("异常信息:",e);
                        }
                        return ParallelFlux.from(Flux.just(s));
                    });
                });
    }

    @Override
    public Double[] rateOfProcess() {
        log.info("已经导出文件：{}",finishedFileNum);
        log.info("总文件数：{}",totalFileNum[0]);
        Double[] rateDouble = new Double[2];
        rateDouble[0] = finishedFileNum.doubleValue();
        rateDouble[1] = Double.parseDouble(String.valueOf(totalFileNum[0]));
        return rateDouble;
    }

    @Override
    public Long finishedRowNum(){
        return finishedRowNum.get();
    }

    public InSqlFileSink(String name){
        this.name = name;
    }
}
