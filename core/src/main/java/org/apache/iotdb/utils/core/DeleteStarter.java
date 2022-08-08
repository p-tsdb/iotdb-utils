package org.apache.iotdb.utils.core;

import org.apache.iotdb.utils.core.pipeline.context.model.DeleteModel;
import org.apache.iotdb.utils.core.service.ExportPipelineService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/26 13:51
 */
@Slf4j
public class DeleteStarter implements Starter<DeleteModel> {

    @Override
    public Disposable start(DeleteModel model) {
        Scheduler scheduler = Schedulers.single();
        Disposable disposable = Flux.just("")
                .subscribeOn(scheduler)
                .flatMap(s -> {
                    deleteTimeseries(model);
                    return Flux.just(s);
                })
                .doFinally(s->{
                    scheduler.dispose();
                })
                .subscribe();
        return disposable;
    }

    public void deleteTimeseries(DeleteModel model) {
        StringBuilder sql = new StringBuilder();
        sql.append("delete from ")
                .append(ExportPipelineService.formatPath(model.getIotdbPath()))
                .append(" where ")
                .append(model.getWhereClause());

        Session session = model.getSession();
        try {
            session.executeNonQueryStatement(sql.toString());
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            log.error("异常SQL:{}\n异常信息:", sql.toString(), e);
        }
    }


    @Override
    public void shutDown() {

    }

    @Override
    public Double[] rateOfProcess() {
        return new Double[0];
    }

    @Override
    public Long finishedRowNum() {
        return null;
    }

//    public static void main(String[] args) throws IoTDBConnectionException, InterruptedException {
//        DeleteModel deleteModel = new DeleteModel();
//        deleteModel.setIotdbPath("root._monitor.\"115.28.134.232\".\"quantity\".\"name=device\".**");
//        deleteModel.setWhereClause("time < 1657072800000");
//
//        Session session = new Session("127.0.0.1",6667,"root","root");
//        session.open();
//        deleteModel.setSession(session);
//
//
//        Starter starter = new DeleteStarter();
//        Disposable disposable = starter.start(deleteModel);
//        while (!disposable.isDisposed()){
//            Thread.sleep(1000);
//        }
//        session.close();
//    }
}
