package org.apache.iotdb.utils.core.pipeline;

import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.IECommonModel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Data
@Slf4j
public class CommonPipeline implements Pipeline {

    private List<Component> componentChain;

    private PipelineContext<IECommonModel> pipelineContext = new PipelineContext<>();

    private List<Disposable> disposableList = new ArrayList<>();

    /**
     * 某些情况，需要等到任务执行结束，参考代码
     * while (!disposable.isDisposed()){
     * Thread.sleep(1000);
     * }
     *
     * @return
     */
    @Override
    public Disposable start() {
        Iterator<Component> it = componentChain.iterator();
        if (it.hasNext()) {
            return syncRun(it, null);
        }
        return null;
    }

    private Disposable syncRun(Iterator<Component> it, Disposable disposable) {
        Component component = it.next();
        Flux pipelineFlux = Flux.just("------------------pipeline start------------- \r\n");
        if (disposable != null) {
            Disposable finalDisposable = disposable;
            pipelineFlux = pipelineFlux
                    .publishOn(Schedulers.newSingle("single"))
                    .flatMap(s -> {
                        while (!finalDisposable.isDisposed()) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                log.error("异常信息:", e);
                            }
                        }
                        return Flux.just(s);
                    });
        }
        pipelineFlux = pipelineFlux
                .transform((Function<? super Flux, Flux>) component.execute())
                .contextWrite(ctx -> ((Context) ctx).put("pipelineContext", pipelineContext));

        if (it.hasNext()) {
            disposable = pipelineFlux
                    .subscribe();
            disposableList.add(disposable);
            return syncRun(it, disposable);
        } else {
            disposable = pipelineFlux
                    .doFinally(type->{
                        SignalType signalType = (SignalType) type;
                        //提供回调
                        IECommonModel ieCommonModel = pipelineContext.getModel();
                        if(ieCommonModel.getConsumer() != null){
                            ieCommonModel.getConsumer().accept(signalType);
                        }
                    })
                    .subscribe();
            disposableList.add(disposable);
            return disposable;
        }
    }


    /**
     * 暂时没用到
     * 某些情况，需要等到任务执行结果，参考代码
     * while(true){
     * List flags = disposableList.stream().map(s->{
     * return s.isDisposed();
     * }).collect(Collectors.toList());
     * if(!flags.contains(false)){
     * return;
     * }
     * try{
     * Thread.sleep(1000);
     * } catch (InterruptedException e) {
     * }
     * }
     *
     * @return
     */
    private List<Disposable> startParallel() {
        List<Disposable> disposableList = componentChain.parallelStream().map(wrapper -> {
            Flux pipelineFlux = Flux.just("------------------pipeline start------------- \r\n");
            pipelineFlux = pipelineFlux
                    .transform((Function<? super Flux, Flux>) wrapper.execute());
            Disposable disposable = pipelineFlux
                    .contextWrite(ctx -> ((Context) ctx).put("pipelineContext", pipelineContext))
                    .doOnTerminate(() -> log.info("pipeline complate!"))
                    .subscribe();
            return disposable;
        })
                .collect(Collectors.toList());
        return disposableList;
    }

    @Override
    public void shutDown() {
        disposableList.forEach(disposable -> {
            disposable.dispose();
        });
    }


    public CommonPipeline withContext(PipelineContext context) {
        this.pipelineContext = pipelineContext;
        return this;
    }

    public CommonPipeline withContext(Supplier<PipelineContext> supplier) {
        this.pipelineContext = supplier.get();
        return this;
    }
}
