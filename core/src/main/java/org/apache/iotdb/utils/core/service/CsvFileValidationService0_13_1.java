package org.apache.iotdb.utils.core.service;

import org.apache.iotdb.utils.core.exception.FileTransFormationException;
import org.apache.iotdb.utils.core.model.ValidationType;
import org.apache.iotdb.utils.core.parse.CsvFileTransParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/8/3 9:53
 */
public class CsvFileValidationService0_13_1 extends CsvFileValidationService{

    @Override
    public void dataValidateWithServer(String path, Session session, String charset, ValidationType type) throws Exception {
        File fi = validateFilePath(path);
        long size = fi.length();
        if (size >= 200 * 1024 * 1024) {
            throw new FileTransFormationException("the max supported file size is 200MB");
        }
        if (!fi.getName().endsWith(".csv")) {
            throw new FileTransFormationException("given file is not a csv file");
        }

        CsvFileTransParser parser = new CsvFileTransParser(fi,charset);
        List<CSVRecord> allList = new ArrayList<>();
        List<CSVRecord> batchList = new ArrayList();
        try {
            while ((batchList = parser.nextBatchRecords(10000)).size() != 0) {
                allList.addAll(batchList);
            }
        } catch (Exception e) {
        } finally {
            parser.close();
        }
        if(allList.size() == 0){
            return;
        }

        List<String> parserHeader = parser.csvParser.getHeaderNames();
        List<String> timeseries = new ArrayList<>();
        timeseries.addAll(parserHeader);
        timeseries.remove("Time");
        String deviceName = timeseries.get(0).substring(0,timeseries.get(0).lastIndexOf("."));
        Map<String,String> headMap = this.getFiledTypeMap(timeseries);
        doDataValidation(allList, timeseries,headMap, session,type);
    }

    public Map<String, String> getFiledTypeMap(List<String> timeseries) throws StatementExecutionException, IoTDBConnectionException {
        Map<String, String> typeMap = new HashMap<>();
        for (String t : timeseries) {
            typeMap.put(t,t);
        }
        return typeMap;
    }
}
