package org.apache.iotdb.utils.core.pipeline.out.sink;

import org.apache.iotdb.utils.core.model.IField;
import org.apache.iotdb.utils.core.model.TimeSeriesRowModel;
import org.apache.iotdb.utils.core.pipeline.PipeSink;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Slf4j
public class OutCsvFileSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

    private String name;

    private AtomicInteger finishedFileNum = new AtomicInteger();

    private int totalFileNum;

    private AtomicLong finishedRowNum = new AtomicLong();

    public OutCsvFileSink(String name){
        this.name = name;
    }

    @Override
    public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
        return sink -> sink
                .transformGroups((Function<GroupedFlux<Integer, TimeSeriesRowModel>, Publisher<? extends TimeSeriesRowModel>>) integerTimeSeriesRowModelGroupedFlux ->
                        integerTimeSeriesRowModelGroupedFlux
                                .buffer(15000, 15000)
                                .flatMap(allList -> {
                                    return Flux.deferContextual(contextView -> {
                                        ConcurrentHashMap<String, CSVPrinter> outputStreamMap = contextView.get("outputStreamMap");
                                        Integer[] totalSize = contextView.get("totalSize");
                                        totalFileNum = totalSize[0];
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
                                                String deviceName = groupKey.substring(groupKey.indexOf(",")+1,groupKey.length());
                                                finishedFileNum.incrementAndGet();
                                                continue;
                                            }
                                            List<TimeSeriesRowModel> groupList = groupMap.get(groupKey);
                                            CSVPrinter printer = outputStreamMap.get(groupKey);
                                            groupList.forEach(s -> {
                                                List<String> list = generateCsvString(s);
                                                try {
                                                    printer.printRecord(list);
                                                    printer.flush();
                                                } catch (IOException e) {
                                                    log.error("????????????:",e);
                                                }
                                            });
                                            finishedRowNum.addAndGet(groupList.size());
                                        }
                                        return Flux.fromIterable(allList);
                                    });
                                })
                ).flatMap(timeSeriesRowModel -> {
                    return Flux.deferContextual(contextView -> {
                        String deviceName = timeSeriesRowModel.getDeviceModel().getDeviceName();
                        if (deviceName.startsWith("finish")) {
                            ConcurrentHashMap<String, CSVPrinter> outputStreamMap = contextView.get("outputStreamMap");
                            deviceName = deviceName.substring(deviceName.indexOf(",")+1,deviceName.length());
                            CSVPrinter csvPrinter = outputStreamMap.get(deviceName);
                            outputStreamMap.remove(deviceName);
                            try {
                                csvPrinter.flush();
                                csvPrinter.close();
                            } catch (IOException e) {
                                log.error("csvPrinter ???????????????",e);
                            }
                        }
                        return Flux.just(timeSeriesRowModel);
                    });
                });
    }

    @Override
    public Double[] rateOfProcess(){
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

    public List<String> generateCsvString(TimeSeriesRowModel timeSeriesRowModel) {
        List<String> value = new ArrayList<>();
        value.add(String.valueOf(timeSeriesRowModel.getTimestamp()));
        for (int i = 0; i < timeSeriesRowModel.getIFieldList().size(); i++) {
            IField iField = timeSeriesRowModel.getIFieldList().get(i);
            if (iField.getField() != null && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
                value.add(String.valueOf(iField.getField().getObjectValue(iField.getField().getDataType())));
            } else {
                value.add("");
            }
        }
        return value;
    }
}
