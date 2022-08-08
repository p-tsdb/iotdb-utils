package org.apache.iotdb.utils.core;

import org.apache.iotdb.utils.core.exception.FileTransFormationException;
import org.apache.iotdb.utils.core.parse.CsvFileTransParser;
import org.apache.iotdb.utils.core.service.CsvFileTransFormationService;

import java.io.IOException;
import java.io.InputStream;

public class CsvFileTransformation {

    /**
     * @param path full file path
     * @return CsvFileTransParser ,use 'CsvFileTransParser.loadInsertStrList' to get the 'insert sql' strings
     *     then if you have finished, please close the parse
     *     CsvFileTransParser，CsvFileTransParser.close()
     * @throws IOException
     * @throws FileTransFormationException
     */
    public static CsvFileTransParser getCsvFileTransParser(String path, String charset) throws Exception {
        CsvFileTransFormationService util = new CsvFileTransFormationService();
        return util.getParser(path,charset);
    }

    /**
     * @param path full file path
     * @param aligned
     * @return
     * @throws IOException
     */
    public static InputStream transCsvToInsertStream(String path, boolean aligned, String charset)
            throws IOException {
        CsvFileTransFormationService util = new CsvFileTransFormationService();
        return util.transToInsertStream(path, aligned,charset);
    }

}
