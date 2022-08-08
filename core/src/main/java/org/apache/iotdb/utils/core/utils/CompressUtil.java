package org.apache.iotdb.utils.core.utils;

import org.apache.iotdb.utils.core.model.CompressType;
import org.apache.iotdb.utils.core.pipeline.context.model.CompressEnum;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

@Slf4j
public class CompressUtil {

    public static void compress(InputStream fi, OutputStream fo, CompressType type) {

        byte[] buffer = new byte[1024 * 1024 * 8];
        try (OutputStream sout = CompressType.SNAPPY.equals(type) ? new SnappyOutputStream(fo) : CompressType.GZIP.equals(type) ? new GZIPOutputStream(fo) : null) {
            while (true) {
                int count = fi.read(buffer, 0, buffer.length);
                if (count == -1) {
                    break;
                }
                sout.write(buffer, 0, count);
            }
            sout.flush();
        } catch (Throwable ex) {
            log.error("异常信息:",ex);
        }
    }

    public static void uncompress(InputStream fi, OutputStream fo, CompressType type){
        byte[] buffer = new byte[1024 * 1024 * 8];
        try(InputStream sin = CompressEnum.SNAPPY.equals(type) ? new SnappyInputStream(fi) : CompressEnum.GZIP.equals(type)? new GZIPInputStream(fi) : null ){
            while (true) {
                int count = sin.read(buffer, 0, buffer.length);
                if (count == -1) {
                    break;
                }
                fo.write(buffer, 0, count);
            }
            fo.flush();
        } catch (Throwable ex) {
            log.error("异常信息:",ex);
        }
    }
}
