package org.apache.iotdb.utils.core.utils;

import java.io.*;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/8/1 14:55
 */
public class NoHeaderObjectInputStream extends ObjectInputStream {
    public NoHeaderObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    protected void readStreamHeader() throws IOException, StreamCorruptedException {
    }

    protected NoHeaderObjectInputStream() throws IOException, SecurityException {
    }
}
