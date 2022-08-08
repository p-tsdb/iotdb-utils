package org.apache.iotdb.utils.core.integration;

import org.apache.iotdb.utils.core.CsvFileValidation;
import org.apache.iotdb.utils.core.model.CompressType;
import org.apache.iotdb.utils.core.model.ExportModel;
import org.apache.iotdb.utils.core.model.ImportModel;
import org.apache.iotdb.utils.core.script.CommonScript;
import org.apache.iotdb.utils.core.script.SessionProperties;
import org.apache.iotdb.utils.core.service.ExportCsvService;
import org.apache.iotdb.utils.core.service.ImportCsvService;
import org.apache.iotdb.utils.core.model.ValidationType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

import static org.junit.Assert.fail;

public class ImExIT extends CommonScript {

    static final String delSqlAli = "delete from root.test.test.cli.ali.*";
    static final String delSqlImport = "delete from root.test.yonyou.cli.monitor.*";
    static final String delSqlImportAlignByDevice = "delete from root.test.devic.monitor.*";

    static final String selectSqlAli = "select * from root.test.test.cli.ali";
    static final String selectSqlImport = "select * from root.test.yonyou.cli.monitor";
    static final String selectSqlImportAlignByDevice = "select * from root.test.devic.monitor";

    public static final Logger logger = LoggerFactory.getLogger(ImExIT.class);
    static Session session;
    static {
        try{
            SessionProperties sessionProperties = new SessionProperties();
            session = new Session(sessionProperties.getHost(), sessionProperties.getPort(), sessionProperties.getUsername(), sessionProperties.getPassword());
            session.open(false);
            session.setTimeZone("+8");
            logger.info("session is prepared");
        }catch (Exception e){
            fail();
        }
    }
    @Before
    public void setUp() {
        try {
            session.executeNonQueryStatement(delSqlAli);
            session.executeNonQueryStatement(delSqlImport);
            session.executeNonQueryStatement(delSqlImportAlignByDevice);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void tearDown() {
        try {
            session.executeNonQueryStatement(delSqlAli);
            session.executeNonQueryStatement(delSqlImport);
            session.executeNonQueryStatement(delSqlImportAlignByDevice);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {
                session.close();
            } catch (IoTDBConnectionException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testImExAliCsv(){
        ImportModel importModel = initImportModel("ali.csv");
        ExportModel exportModel = initExportModel("aliout");
        ImportCsvService importCsvService = new ImportCsvService();
        ExportCsvService exportCsvService = new ExportCsvService();
        try{
            importCsvService.importFromTargetPath(session,importModel);
            CsvFileValidation.dataValidation(importModel.getTargetPath(),session,importModel.getCharSet(), ValidationType.EQUAL);
            exportCsvService.dumpResult(selectSqlAli,0,session,exportModel);
            CsvFileValidation.dataValidation(exportModel.getTargetDirectory()+exportModel.getTargetFile()+"0.csv",session,importModel.getCharSet(), ValidationType.EQUAL);
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }


    @Test
    public void testImExImportCsv(){
        ImportModel importModel = initImportModel("import.csv");
        ExportModel exportModel = initExportModel("importout");
        ImportCsvService importCsvService = new ImportCsvService();
        ExportCsvService exportCsvService = new ExportCsvService();
        try{
            importCsvService.importFromTargetPath(session,importModel);
            CsvFileValidation.dataValidation(importModel.getTargetPath(),session,importModel.getCharSet(), ValidationType.EQUAL);
            exportCsvService.dumpResult(selectSqlImport,0,session,exportModel);
            CsvFileValidation.dataValidation(exportModel.getTargetDirectory()+exportModel.getTargetFile()+"0.csv",session,importModel.getCharSet(), ValidationType.EQUAL);
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testImExImportAlignByDeviceCsv(){
        ImportModel importModel = initImportModel("importAlignByDevice.csv");
        ExportModel exportModel = initExportModel("importAlignByDeviceout");
        ImportCsvService importCsvService = new ImportCsvService();
        ExportCsvService exportCsvService = new ExportCsvService();
        try{
            importCsvService.importFromTargetPath(session,importModel);
            CsvFileValidation.dataValidation(importModel.getTargetPath(),session,importModel.getCharSet(), ValidationType.EQUAL);
            exportCsvService.dumpResult(selectSqlImportAlignByDevice,0,session,exportModel);
            CsvFileValidation.dataValidation(exportModel.getTargetDirectory()+exportModel.getTargetFile()+"0.csv",session,importModel.getCharSet(), ValidationType.EQUAL);
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testImExAliGbkCsv(){
        ImportModel importModel = initImportModel("ali-gbk.csv");
        ImportModel importModel1 = initImportModel("ali.csv");
        importModel.setCharSet("GBK");
        ExportModel exportModel = initExportModel("ali-gbkout");
        ExportModel exportModel1 = initExportModel("aliout");
        exportModel.setCharSet("GBK");
        ImportCsvService importCsvService = new ImportCsvService();
        ExportCsvService exportCsvService = new ExportCsvService();
        try{
            importCsvService.importFromTargetPath(session,importModel);
            CsvFileValidation.dataValidation(importModel.getTargetPath(),session,importModel.getCharSet(), ValidationType.EQUAL);
            CsvFileValidation.dataValidation(importModel1.getTargetPath(),session,importModel1.getCharSet(), ValidationType.EQUAL);
            exportCsvService.dumpResult(selectSqlAli,0,session,exportModel);
            exportCsvService.dumpResult(selectSqlAli,0,session,exportModel1);
            CsvFileValidation.dataValidation(exportModel.getTargetDirectory()+exportModel.getTargetFile()+"0.csv",session,importModel.getCharSet(), ValidationType.EQUAL);
            CsvFileValidation.dataValidation(exportModel1.getTargetDirectory()+exportModel1.getTargetFile()+"0.csv",session,importModel1.getCharSet(), ValidationType.EQUAL);
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }


    private ExportModel initExportModel(String csvFileName) {
        ExportModel exportModel = new ExportModel();
        try {
            exportModel.setTimestampPrecision(session.getTimestampPrecision());
        } catch (TException e) {
            fail();
        }
        exportModel.setZoneId(ZoneId.of(session.getTimeZone()));
        exportModel.setCompressType(CompressType.NONE);
        exportModel.setTimeFormat("default");
        exportModel.setTargetFile(csvFileName);
        exportModel.setTargetDirectory(getResourceFilePath()+"\\");
        exportModel.setNeedDataTypePrinted(true);
        exportModel.setCharSet("utf8");
        exportModel.setAppend(false);
        return exportModel;
    }


    private ImportModel initImportModel(String csvFileName) {
        String resourceFilePath = getResourceFilePath();
        ImportModel importModel = new ImportModel();
        importModel.setTargetPath(resourceFilePath + "\\" + csvFileName);
        importModel.setBatchPointSize(1000);
        importModel.setAligned(true);
        importModel.setCompressType(CompressType.NONE);
        importModel.setCharSet("utf8");
        return importModel;
    }
}
