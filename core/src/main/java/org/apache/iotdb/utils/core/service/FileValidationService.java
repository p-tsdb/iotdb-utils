package org.apache.iotdb.utils.core.service;

import org.apache.iotdb.utils.core.exception.FileTransFormationException;
import org.apache.iotdb.utils.core.model.ValidationType;
import org.apache.iotdb.session.Session;

import java.io.File;

public interface FileValidationService {

    void dataValidateWithServer(String path, Session session,String charset, ValidationType type) throws Exception;

    default File validateFilePath(String filePath) throws FileTransFormationException {
        File fi = new File(filePath);
        if (fi.isFile()) {

        } else if (fi.isDirectory()) {
            throw new FileTransFormationException("given path is not a file,it is a directory");
        } else {
            throw new FileTransFormationException("file can not find");
        }
        return fi;
    }
}
