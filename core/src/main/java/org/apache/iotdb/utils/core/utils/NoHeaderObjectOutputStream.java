package org.apache.iotdb.utils.core.utils;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/19 15:14
 */
public class NoHeaderObjectOutputStream extends ObjectOutputStream {
    public NoHeaderObjectOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    // do not wirte the header
    protected void writeStreamHeader() throws IOException {

    }
}
