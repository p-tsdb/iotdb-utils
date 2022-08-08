package org.apache.iotdb.utils.core.service;

import org.apache.iotdb.utils.core.exception.FileTransFormationException;
import org.apache.iotdb.utils.core.parse.CsvFileTransParser;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

@Slf4j
public class CsvFileTransFormationService implements FileTransformationService {

    @Override
    public CsvFileTransParser getParser(String path, String charset) throws Exception {
        File fi = validateFilePath(path);

        if (!fi.getName().endsWith(".csv")) {
            throw new FileTransFormationException("given file is not a csv file");
        }
        CsvFileTransParser parser = new CsvFileTransParser(fi,charset);
        return parser;
    }

    @Override
    public void transToInsertFile(String csvPath, String outPutPath) {
    }

    @Override
    public InputStream transToInsertStream(String path, boolean aligned,String charset) throws IOException {
        PipedInputStream inputStream = new PipedInputStream();
        PipedOutputStream outputStream = new PipedOutputStream(inputStream);
        new Thread(
                () -> {
                    try {
                        CsvFileTransParser parser = getParser(path,charset);
                        while (true) {
                            List<String> lst = parser.loadInsertStrList(1, aligned);
                            if (lst.size() == 0) {
                                break;
                            }
                            String sql = lst.get(0);
                            outputStream.write(sql.getBytes());
                        }
                        outputStream.close();
                    } catch (IOException | FileTransFormationException e) {
                        log.error("异常信息:",e);
                    } catch (Exception e) {
                        log.error("异常信息:",e);
                    }
                })
                .start();
        return inputStream;
    }
}
