package org.apache.iotdb.utils.core.service;

import org.apache.iotdb.utils.core.exception.ParamCheckException;
import org.apache.iotdb.utils.core.model.CompressType;
import org.apache.iotdb.utils.core.model.ExportModel;
import org.apache.iotdb.utils.core.utils.CompressUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.thrift.TException;

import java.io.*;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class ExportCsvService {

    /**
     * This method will be called, if the query commands are written in a sql file.
     * @param filePath
     * @throws IOException
     */
    public void dumpFromSqlFile(String filePath, Session session, ExportModel exportModel) throws IOException, StatementExecutionException, TException, IoTDBConnectionException, ParamCheckException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String sql;
            int index = 0;
            while ((sql = reader.readLine()) != null) {
                dumpResult(sql, index, session,exportModel);
                index++;
            }
        }
    }

    /**
     * Dump files from database to CSV file.
     * @param sql export the result of executing the sql
     * @param index used to create dump file name
     */
    public void dumpResult(String sql, int index, Session session, ExportModel exportModel) throws TException, IOException, StatementExecutionException, IoTDBConnectionException, ParamCheckException {
        String path =
                new StringBuilder(exportModel.getTargetDirectory())
                        .append(exportModel.getTargetFile())
                        .append(index)
                        .append(".csv")
                        .toString();
        if(exportModel.isAppend()){
            if (exportModel.getTargetFile() == null || "".equals(exportModel.getTargetFile())){
                throw new ParamCheckException("the param append is true,a target file is needed");
            }
        }
        exportModel.setTimestampPrecision(session.getTimestampPrecision());
        SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
        if (!exportModel.getCompressType().equals(CompressType.NONE)) {
            if(exportModel.getCompressType().equals(CompressType.SNAPPY)){
                path = new StringBuilder(path).append(".snappy").toString();
            }
            if(exportModel.getCompressType().equals(CompressType.GZIP)){
                path = new StringBuilder(path).append(".gz").toString();
            }
            PipedInputStream input = new PipedInputStream();
            final PipedOutputStream out = new PipedOutputStream((PipedInputStream) input);
            if(exportModel.isAppend()){
                getCsvCloumnNameList(path,exportModel);
            }
            new Thread(
                    new Runnable() {
                        public void run() {
                            try {
                                writeCsvFile(sessionDataSet, out, exportModel);
                            } catch (Exception e) {
                            }
                        }
                    })
                    .start();
            OutputStream out2 = new FileOutputStream(new File(path),exportModel.isAppend());
            CompressUtil.compress(input, out2,exportModel.getCompressType());
            input.close();
            out2.close();
        } else {
            if(exportModel.isAppend()){
                getCsvCloumnNameList(path,exportModel);
            }
            writeCsvFile(sessionDataSet, new FileOutputStream(new File(path),exportModel.isAppend()),exportModel);
        }
        System.out.println("Export completely!");
    }

    private Boolean writeCsvFile(SessionDataSet sessionDataSet, OutputStream out,
            ExportModel exportModel) throws IOException, StatementExecutionException, IoTDBConnectionException {
        if(exportModel.getCharSet() == null || "".equals(exportModel.getCharSet())){
            exportModel.setCharSet("utf8");
        }
        CSVPrinter printer =
                CSVFormat.Builder.create(CSVFormat.DEFAULT)
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setEscape('\\')
                        .setQuoteMode(QuoteMode.NONE)
                        .build()
                        .print(new OutputStreamWriter(out, exportModel.getCharSet()));
        try{
            if(exportModel.isAppend()){
                printAppendData(sessionDataSet,printer,exportModel);
            }else {
                printData(sessionDataSet,printer,exportModel);
            }
        }finally {
            printer.flush();
            printer.close();
        }
        return true;
    }

    private void printAppendData(SessionDataSet sessionDataSet,CSVPrinter printer,ExportModel exportModel) throws StatementExecutionException, IoTDBConnectionException, IOException {
        List<String> columnNameList = sessionDataSet.getColumnNames();
        if(exportModel.isAppend()){
            exportModel.getCsvCloumnNameList().stream().forEach(s->{
                if(!columnNameList.contains(s)){
                    throw new RuntimeException("the append dataset not fit the dataset in the csv; cloumn name:" + s);
                }
            });
        }
        List<String> columnNameListWithoutTime = new ArrayList<>(columnNameList);
        if ("Time".equals(columnNameList.get(0))) {
            columnNameListWithoutTime.remove(0);
            exportModel.getCsvCloumnNameList().remove(0);
        }
        while (sessionDataSet.hasNext()) {
            RowRecord rowRecord = sessionDataSet.next();
            ArrayList<String> record = new ArrayList<>();
            if ("Time".equals(columnNameList.get(0))) {
                record.add(timeTrans(rowRecord.getTimestamp(),exportModel));
            }

            exportModel.getCsvCloumnNameList().stream()
                    .forEach(s->{
                        int i = columnNameListWithoutTime.indexOf(s);
                        Field field = rowRecord.getFields().get(i);
                        String fieldStringValue = field.getStringValue();

                        if (field.getObjectValue(field.getDataType()) != null) {
                            if (field.getDataType() == TSDataType.TEXT
                                    && !"Device".equals(s)) {
                                fieldStringValue = "\"" + fieldStringValue + "\"";
                            }
                            record.add(fieldStringValue);
                        } else {
                            record.add("");
                        }
                    });
            printer.printRecord(record);
        }
    }

    private void printData(SessionDataSet sessionDataSet,CSVPrinter printer,ExportModel exportModel) throws IOException, StatementExecutionException, IoTDBConnectionException {
        List<String> columnNameList = sessionDataSet.getColumnNames();
        List<String> columnTypeList = sessionDataSet.getColumnTypes();
        for (int i = 0; i < columnNameList.size(); i++) {
            String name = columnNameList.get(i);
            if ("Time".equals(name)) {
                printer.print(name);
            } else {
                String type = columnTypeList.get(i);
                if (exportModel.isNeedDataTypePrinted() && !"Device".equals(name)) {
                    printer.print(new StringBuilder(name).append("(").append(type).append(")").toString());
                } else {
                    printer.print(name);
                }
            }
        }
        printer.println();
        List<String> columnNameListWithoutTime = new ArrayList<>(columnNameList);
        if ("Time".equals(columnNameList.get(0))) {
            columnNameListWithoutTime.remove(0);
        }
        while (sessionDataSet.hasNext()) {
            RowRecord rowRecord = sessionDataSet.next();
            ArrayList<String> record = new ArrayList<>();
            if ("Time".equals(columnNameList.get(0))) {
                record.add(timeTrans(rowRecord.getTimestamp(),exportModel));
            }
            columnNameListWithoutTime.stream()
                    .forEach(s->{
                        int i = columnNameListWithoutTime.indexOf(s);
                        Field field = rowRecord.getFields().get(i);
                        String fieldStringValue = field.getStringValue();

                        if (field.getObjectValue(field.getDataType()) != null) {
                            if (field.getDataType() == TSDataType.TEXT
                                    && !"Device".equals(s)) {
                                fieldStringValue = "\"" + fieldStringValue + "\"";
                            }
                            record.add(fieldStringValue);
                        } else {
                            record.add("");
                        }
                    });
            printer.printRecord(record);
        }
    }

    public String timeTrans(Long time,ExportModel exportModel) {
        switch (exportModel.getTimeFormat()) {
            case "default":
                return RpcUtils.parseLongToDateWithPrecision(
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME, time, exportModel.getZoneId(), exportModel.getTimestampPrecision());
            case "timestamp":
            case "long":
            case "number":
                return String.valueOf(time);
            default:
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), exportModel.getZoneId())
                        .format(DateTimeFormatter.ofPattern(exportModel.getTimeFormat()));
        }
    }

    public void getCsvCloumnNameList(String path, ExportModel exportModel) throws IOException {
        CSVParser parser = this.readCsvFile(path,exportModel);
        exportModel.setCsvCloumnNameList(parseHeaders(parser.getHeaderNames()));
    }

    /**
     * read data from the CSV file
     *
     * @param path
     * @return
     * @throws IOException
     */
    private CSVParser readCsvFile(String path, ExportModel exportModel) throws IOException {
        InputStream input = null;
        if(exportModel.getCharSet() == null || "".equals(exportModel.getCharSet())){
            exportModel.setCharSet("utf8");
        }

        if (!exportModel.getCompressType().equals(CompressType.NONE)) {
            input = new PipedInputStream();
            final PipedOutputStream out = new PipedOutputStream((PipedInputStream) input);
            new Thread(
                    () -> {
                        try {
                            InputStream in = new FileInputStream(path);
                            CompressUtil.uncompress(in, out,exportModel.getCompressType());
                            in.close();
                            out.close();
                        } catch (IOException e) {
                            log.error("异常信息:",e);
                        }
                    })
                    .start();

            return CSVFormat.EXCEL
                    .withFirstRecordAsHeader()
                    .withQuote('\'')
                    .withEscape('\\')
                    .withIgnoreEmptyLines()
                    .parse(new InputStreamReader(input, exportModel.getCharSet()));
        } else {
            input = new FileInputStream(path);
            return CSVFormat.EXCEL
                    .withFirstRecordAsHeader()
                    .withQuote('\'')
                    .withEscape('\\')
                    .withIgnoreEmptyLines()
                    .parse(new InputStreamReader(input, exportModel.getCharSet()));
        }
    }


    private List<String> parseHeaders(List<String> headerNames) {
        String regex = "(?<=\\()\\S+(?=\\))";
        Pattern pattern = Pattern.compile(regex);
        return headerNames.stream().map(s->{
            if (s.equals("Time") || s.equals("Device"))
                return s;
            Matcher matcher = pattern.matcher(s);
            String type;
            if (matcher.find()) {
                type = matcher.group();
                String headerNameWithoutType =
                        s.replace(new StringBuilder().append("(").append(type).append(")").toString(), "").replaceAll("\\s+", "");
                return headerNameWithoutType;
            }
            return s;
        }).collect(Collectors.toList());
    }
}
