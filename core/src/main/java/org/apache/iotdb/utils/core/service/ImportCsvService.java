package org.apache.iotdb.utils.core.service;

import org.apache.iotdb.utils.core.exception.ParamCheckException;
import org.apache.iotdb.utils.core.model.CompressType;
import org.apache.iotdb.utils.core.model.CsvModel;
import org.apache.iotdb.utils.core.model.ImportModel;
import org.apache.iotdb.utils.core.utils.CompressUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.thrift.annotation.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.*;

@Slf4j
public class ImportCsvService {

    private static List<DateTimeFormatter> fmts = new LinkedList<>();
    private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    static {
        fmts.add(fmt);
    }

    /**
     * Specifying a CSV file or a directory including CSV files that you want to import. This method
     * can be offered to console cli to implement importing CSV file by command.
     *
     * @param session
     * @param importModel
     */
    public void importFromTargetPath(Session session, ImportModel importModel) throws Exception {
        File file = new File(importModel.getTargetPath());
        if (file.isFile()) {
            importFromSingleFileNew(session, file, importModel);
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null) {
                return;
            }

            for (File subFile : files) {
                if (subFile.isFile()) {
                    importFromSingleFileNew(session, subFile, importModel);
                }
            }
        } else {
            System.out.println("File not found!");
        }
    }


    /**
     * import the CSV file and load headers and records.
     *
     * @param file the File object of the CSV file that you want to import.
     */
    private void importFromSingleFileNew(Session session, File file, ImportModel importModel) throws Exception {
        if (importModel.getCompressType().equals(CompressType.NONE)
                && !file.getName().endsWith(ImportModel.CSV_SUFFIXS)) {
            throw new ParamCheckException("The file name must end with \".csv\" or \".txt\"!");
        } else if (importModel.getCompressType().equals(CompressType.SNAPPY) && !file.getName().endsWith(ImportModel.SNAPPY_SUFFIXS)) {
            throw new ParamCheckException("The file name must end with \".snappy\"!");
        } else if (importModel.getCompressType().equals(CompressType.GZIP) && !file.getName().endsWith(ImportModel.GZIP_SUFFIXS)) {
            throw new ParamCheckException("The file name must end with \".gzip\"!");
        }
        Stream<CSVRecord> csvRecords = readCsvFile(file.getAbsolutePath(), importModel).stream();
        writeDataAlignedByTime(session, csvRecords, importModel.getBatchPointSize(), importModel.isAligned());
    }


    /**
     * read data from the CSV file
     *
     * @param path
     * @return
     * @throws IOException
     */
    private CSVParser readCsvFile(String path, ImportModel importModel) throws IOException {
        InputStream input = null;
        if(importModel.getCharSet() == null || "".equals(importModel.getCharSet())){
            importModel.setCharSet("utf8");
        }

        if (!importModel.getCompressType().equals(CompressType.NONE)) {
            input = new PipedInputStream();
            final PipedOutputStream out = new PipedOutputStream((PipedInputStream) input);
            new Thread(
                    new Runnable() {
                        public void run() {
                            try {
                                InputStream in = new FileInputStream(path);
                                CompressUtil.uncompress(in, out,importModel.getCompressType());
                                in.close();
                                out.close();
                            } catch (IOException e) {
                                log.error("异常信息:",e);
                            }
                        }
                    })
                    .start();

            return CSVFormat.EXCEL
                    .withFirstRecordAsHeader()
                    .withQuote('\'')
                    .withEscape('\\')
                    .withIgnoreEmptyLines()
                    .parse(new InputStreamReader(input, importModel.getCharSet()));
        } else {
            input = new FileInputStream(path);
            return CSVFormat.EXCEL
                    .withFirstRecordAsHeader()
                    .withQuote('\'')
                    .withEscape('\\')
                    .withIgnoreEmptyLines()
                    .parse(new InputStreamReader(input, importModel.getCharSet()));
        }
    }


    private void writeDataAlignedByTime(Session session, Stream<CSVRecord> records,
                                               int batchPointSize, boolean aligned) throws Exception {
        @SuppressWarnings("unchecked")
        List<String>[] headerNames = new LinkedList[1];

        HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
        HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
        HashMap<String, String> headerNameMap = new HashMap<>();

        Map<String, CsvModel> m = new HashMap<>();

        boolean[] booleanArray = {
                true, false
        }; // [0]: first or not; [1]: header contains "Device" or not;
        try {
            records.forEach(
                    record -> {
                        if (booleanArray[0]) {
                            booleanArray[0] = false;
                            headerNames[0] = new LinkedList<>();
                            Map<String, String> map = record.toMap();
                            headerNames[0].addAll(map.keySet());

                            if (headerNames[0].contains("Device")) {
                                booleanArray[1] = true;
                            }

                            parseHeaders(headerNames[0], deviceAndMeasurementNames, headerTypeMap, headerNameMap,session);
                        }
                        String d = null;
                        if (booleanArray[1]) {
                            d = String.valueOf(typeTrans(record.get("Device"), TEXT));
                        }
                        String timeStr = record.get("Time");
                        for (Map.Entry<String, List<String>> e : deviceAndMeasurementNames.entrySet()) {
                            String measurement = e.getKey();
                            String measurementKey =
                                    booleanArray[1]
                                            ? new StringBuilder(d).append(".").append(measurement).toString()
                                            : measurement;
                            if (!m.containsKey(measurementKey)) {
                                m.put(measurementKey, new CsvModel());
                            }
                            CsvModel cm = m.get(measurementKey);
                            List<String> l = e.getValue();
                            List<String> devices = new LinkedList<String>();
                            List<TSDataType> types = new LinkedList<TSDataType>();
                            List<Object> values = new LinkedList<Object>();
                            Long time = 0L;
                            if (timeStr.indexOf('+') >= 0) {
                                timeStr = timeStr.substring(0, timeStr.indexOf('+'));
                            }
                            timeStr = timeStr.replaceAll("T", " ");
                            if (timeStr.indexOf(' ') == -1) {
                                try {
                                    time = Long.parseLong(timeStr);
                                } catch (Exception e1) {
                                }
                            } else {
                                try {
                                    time = fmt.parseDateTime(timeStr).toDate().getTime();
                                } catch (Exception e2) {
                                }
                            }
                            for (int i = 0; i < l.size(); i++) {
                                if (!measurement.equals(e.getKey())) {
                                    continue;
                                }
                                String ee = l.get(i);
                                String header = ee;
                                if (!booleanArray[1]) {
                                    header = new StringBuilder(e.getKey()).append(".").append(ee).toString();
                                }
                                String raw = record.get(header);
                                if (raw != null && !"".equals(raw)) {
                                    TSDataType type = headerTypeMap.get(headerNameMap.get(header));
                                    Object value = typeTrans(raw, type);
                                    if (value != null) {
                                        String device = ee;
                                        if (ee.indexOf('(') > -1) {
                                            device = ee.substring(0, ee.indexOf('('));
                                        }
                                        devices.add(device);
                                        types.add(type);
                                        values.add(value);
                                    }
                                } else {
                                }
                            }
                            if (!devices.isEmpty()) {
                                if (booleanArray[1]) {
                                    cm.setDeviceId(d);
                                } else {
                                    cm.setDeviceId(e.getKey());
                                }
                                addLine(
                                        cm.getTimes(),
                                        cm.getMeasurementsList(),
                                        cm.getTypesList(),
                                        cm.getValuesList(),
                                        time,
                                        devices,
                                        types,
                                        values);
                            }
                            // 批量导入
                            if (cm.getTimes().size() % batchPointSize == 0
                                    && cm.getTimes().size() / batchPointSize > 0) {
                                try {
                                    if (!aligned) {
                                        session.insertRecordsOfOneDevice(
                                                cm.getDeviceId(),
                                                cm.getTimes(),
                                                cm.getMeasurementsList(),
                                                cm.getTypesList(),
                                                cm.getValuesList());
                                    } else {
                                        session.insertAlignedRecordsOfOneDevice(
                                                cm.getDeviceId(),
                                                cm.getTimes(),
                                                cm.getMeasurementsList(),
                                                cm.getTypesList(),
                                                cm.getValuesList());
                                    }
                                    cm.setCount(cm.getCount() + batchPointSize);
                                    cm.getTimes().clear();
                                    cm.getMeasurementsList().clear();
                                    cm.getTypesList().clear();
                                    cm.getValuesList().clear();
                                } catch (StatementExecutionException | IoTDBConnectionException ex) {
                                    throw new RuntimeException("insert failed,cause: " + ex.getMessage());
                                }
                            }
                        }
                    });
        } catch (Exception e) {
            throw new Exception("method writeDataAlignedByTime error:" + e.getMessage());
        }
        for (Map.Entry<String, CsvModel> e2 : m.entrySet()) {
            CsvModel cm = e2.getValue();
            if (cm.getTimes().size() != 0) {
                System.out.println(
                        new StringBuilder(e2.getKey())
                                .append(",")
                                .append(cm.getCount() + cm.getTimes().size())
                                .toString());
                try {
                    if (!aligned) {
                        session.insertRecordsOfOneDevice(
                                cm.getDeviceId(),
                                cm.getTimes(),
                                cm.getMeasurementsList(),
                                cm.getTypesList(),
                                cm.getValuesList());
                    } else {
                        session.insertAlignedRecordsOfOneDevice(
                                cm.getDeviceId(),
                                cm.getTimes(),
                                cm.getMeasurementsList(),
                                cm.getTypesList(),
                                cm.getValuesList());
                    }
                } catch (IoTDBConnectionException | StatementExecutionException e1) {
                    throw new RuntimeException("insert failed,cause: " + e1.getMessage());
                }
            }
        }
    }

    private void addLine(
            List<Long> times,
            List<List<String>> measurements,
            List<List<TSDataType>> datatypes,
            List<List<Object>> values,
            long time,
            List<String> s1,
            List<TSDataType> s1type,
            List<Object> value2) {

        List<String> tmpMeasurements = new ArrayList<>();
        List<TSDataType> tmpDataTypes = new ArrayList<>();
        List<Object> tmpValues = new ArrayList<>();
        for (int i = 0; i < s1.size(); i++) {
            tmpMeasurements.add(s1.get(i));
            tmpDataTypes.add(s1type.get(i));
            tmpValues.add(value2.get(i));
        }
        times.add(time);
        measurements.add(tmpMeasurements);
        datatypes.add(tmpDataTypes);
        values.add(tmpValues);
    }

    /**
     * parse deviceNames, measurementNames(aligned by time), headerType from headers
     *
     * @param headerNames
     * @param deviceAndMeasurementNames
     * @param headerTypeMap
     * @param headerNameMap
     */
    private void parseHeaders(
            List<String> headerNames,
            @Nullable HashMap<String, List<String>> deviceAndMeasurementNames,
            HashMap<String, TSDataType> headerTypeMap,
            HashMap<String, String> headerNameMap,
            Session session) {
        String regex = "(?<=\\()\\S+(?=\\))";
        Pattern pattern = Pattern.compile(regex);
        Map<String, String> columnTypeMap = null;
        for (String headerName : headerNames) {
            if (headerName.equals("Time") || headerName.equals("Device")) continue;
            Matcher matcher = pattern.matcher(headerName);
            String type;
            if (matcher.find()) {
                type = matcher.group();
                String headerNameWithoutType =
                        headerName.replace(new StringBuilder().append("(").append(type).append(")").toString(), "").replaceAll("\\s+", "");
                headerNameMap.put(headerName, headerNameWithoutType);
                headerTypeMap.put(headerNameWithoutType, getType(type));
            } else {
                if (columnTypeMap == null) {
                    columnTypeMap = buildColumnTypeMap(session);
                }
                headerNameMap.put(headerName, headerName);
                headerTypeMap.put(headerName, getType(columnTypeMap.get(headerName)));
            }
            String[] split = headerName.split("\\.");
            String measurementName = split[split.length - 1];
            String deviceName = headerName.replace("." + measurementName, "");
            if (deviceAndMeasurementNames != null) {
                if (!deviceAndMeasurementNames.containsKey(deviceName)) {
                    deviceAndMeasurementNames.put(deviceName, new ArrayList<>());
                }
                deviceAndMeasurementNames.get(deviceName).add(measurementName);
            }
        }
    }

    private Map<String, String> buildColumnTypeMap(Session session) {
        Map<String, String> columnTypeMap = new HashMap<>();
        try {
            SessionDataSet sessionDataSet = session.executeQueryStatement("show timeseries");
            SessionDataSet.DataIterator it = sessionDataSet.iterator();
            while (it.next()) {
                columnTypeMap.put(it.getString("timeseries"), it.getString("dataType"));
            }
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            log.error("异常信息:",e);
        }
        return columnTypeMap;
    }

    /**
     * return the TSDataType
     *
     * @param typeStr
     * @return
     */
    private TSDataType getType(String typeStr) {
        switch (typeStr) {
            case "TEXT":
                return TEXT;
            case "BOOLEAN":
                return BOOLEAN;
            case "INT32":
                return INT32;
            case "INT64":
                return INT64;
            case "FLOAT":
                return FLOAT;
            case "DOUBLE":
                return DOUBLE;
            default:
                return null;
        }
    }

    /**
     * @param value
     * @param type
     * @return
     */
    private Object typeTrans(String value, TSDataType type) {
        try {
            switch (type) {
                case TEXT:
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    return value;
                case BOOLEAN:
                    if (!"true".equals(value) && !"false".equals(value)) {
                        return null;
                    }
                    return Boolean.valueOf(value);
                case INT32:
                    return Integer.valueOf(value);
                case INT64:
                    return Long.valueOf(value);
                case FLOAT:
                    return Float.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                default:
                    return null;
            }
        } catch (Exception e) {
            System.out.println("value format exception: check your values" + e.getMessage());
            throw e;
        }
    }
}
