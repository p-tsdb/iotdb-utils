package org.apache.iotdb.utils.core;

import org.apache.iotdb.utils.core.service.CsvFileValidationService;
import org.apache.iotdb.utils.core.model.ValidationType;
import org.apache.iotdb.utils.core.service.CsvFileValidationService0_13_1;
import org.apache.iotdb.session.Session;

public class CsvFileValidation {

    /**
     * this is for the csv file exported by v0.13.0
     * compare the content in the path file  with the data in iotdb
     * this method need session,and it do not close the session,you need to manage the session by yourself
     * @param path
     * @param session
     * @throws Exception
     */
    @Deprecated
    public static void dataValidation(String path, Session session, String charset, ValidationType type) throws Exception {
        CsvFileValidationService validationService = new CsvFileValidationService();
        validationService.dataValidateWithServer(path,session,charset,type);
    }


    /**
     * this is for the csv file exported by v0.13.1
     * @param path
     * @param session
     * @param charset
     * @param type
     * @throws Exception
     */
    public static void dataValidation0_13_1(String path, Session session, String charset, ValidationType type) throws Exception {
        CsvFileValidationService0_13_1 service0_13_1 = new CsvFileValidationService0_13_1();
        service0_13_1.dataValidateWithServer(path,session,charset,type);
    }
}
