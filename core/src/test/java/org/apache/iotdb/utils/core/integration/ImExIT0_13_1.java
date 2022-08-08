package org.apache.iotdb.utils.core.integration;

import org.apache.iotdb.utils.core.CsvFileValidation;
import org.apache.iotdb.utils.core.ExportStarter;
import org.apache.iotdb.utils.core.ImportStarter;
import org.apache.iotdb.utils.core.model.ValidationType;
import org.apache.iotdb.utils.core.pipeline.context.model.CompressEnum;
import org.apache.iotdb.utils.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.utils.core.pipeline.context.model.FileSinkStrategyEnum;
import org.apache.iotdb.utils.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.utils.core.script.CommonScript;
import org.apache.iotdb.utils.core.script.SessionProperties;
import org.apache.iotdb.utils.core.service.ExportPipelineService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import java.io.File;

import static org.junit.Assert.fail;

public class ImExIT0_13_1 extends CommonScript {

    public static final String DEVICENAME = "root.0131.**";

    public static final String delSqlAli = "delete timeseries "+ ExportPipelineService.formatPath(DEVICENAME);

    public static final Logger logger = LoggerFactory.getLogger(ImExIT0_13_1.class);
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

    @After
    public void tearDown() {
        try {
            session.executeNonQueryStatement(delSqlAli);
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
    public void imExUnit() throws InterruptedException, StatementExecutionException, IoTDBConnectionException {

        CompressEnum csv = CompressEnum.CSV;
        ExportModel exportModel = initExportModel(csv);

        ImportStarter importStarter = new ImportStarter();
        ImportModel importModel = initImportModel(csv);
        String resourceFilePath = getResourceFilePath();
        String resourceSource = resourceFilePath + "\\" + "file0_13_1";
        importModel.setFileFolder(resourceSource);
        Disposable disposable = importStarter.start(importModel);
        while (!disposable.isDisposed()){
            Thread.sleep(1000);
        }
        checkCSV(exportModel);

        ExportStarter starter = new ExportStarter();
        disposable = starter.start(exportModel);
        while (!disposable.isDisposed()){
            Thread.sleep(1000);
        }
        CompressEnum gzip = CompressEnum.GZIP;
        exportModel = initExportModel(gzip);
        disposable = starter.start(exportModel);
        while (!disposable.isDisposed()){
            Thread.sleep(1000);
        }
        checkCSV(exportModel);

        session.executeNonQueryStatement(delSqlAli);
        Thread.sleep(2000);
        importStarter = new ImportStarter();
        importModel = initImportModel(gzip);
        disposable= importStarter.start(importModel);
        while (!disposable.isDisposed()){
            Thread.sleep(1000);
        }
        checkCSV(exportModel);
    }


    /**
     * 校验工具  校验导出的文件是否和库中的一致
     * 使用方式，导出某个路径下的所有数据，导出格式csv，如果要测试压缩格式，请同时导出对应的压缩格式
     * 如果要比对csv文件，直接制定csv文件所在的文件位置，制定session链接的iotdb库，运行本方法
     * 如果要比对其他格式的，把其他格式文件导入到本地库中（导入之前请删除对应measurement数据），然后对比csv文件与库中文件
     */
    public void checkCSV(ExportModel exportModel){
        try{
            File dic = new File(exportModel.getFileFolder());
            if(dic.isDirectory()){
                String[] filepath = dic.list((f,n)->{
                    if(n.endsWith(".csv")){
                        return true;
                    }
                    return false;
                });
                for (String s : filepath) {
                    CsvFileValidation.dataValidation0_13_1(exportModel.getFileFolder()+"\\"+s,session,exportModel.getCharSet(), ValidationType.EQUAL);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    private ExportModel initExportModel(CompressEnum compressEnum){
        ExportModel exportModel = new ExportModel();
        exportModel.setSession(session);
        exportModel.setNeedTimeseriesStructure(true);
        exportModel.setFileSinkStrategyEnum(FileSinkStrategyEnum.EXTRA_CATALOG);
        exportModel.setCompressEnum(compressEnum);
        exportModel.setIotdbPath(DEVICENAME);
        exportModel.setCharSet("utf8");
        exportModel.setFileFolder("d:\\validate_test\\83");
        return exportModel;
    }

    private ImportModel initImportModel(CompressEnum compressEnum){
        ImportModel importModel = new ImportModel();
        importModel.setSession(session);
        importModel.setNeedTimeseriesStructure(true);
        importModel.setFileSinkStrategyEnum(FileSinkStrategyEnum.EXTRA_CATALOG);
        importModel.setCompressEnum(compressEnum);
        importModel.setCharSet("utf8");
        importModel.setFileFolder("d:\\validate_test\\83");
        return importModel;
    }
}
