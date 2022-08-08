package org.apache.iotdb.utils.core.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.*;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/19 14:05
 */
@Slf4j
public class ToByteArrayUtils {
    /**
     * 把对象转变成二进制
     *
     * @param obj 待转换的对象
     * @return 返回二进制数组
     */
    public static byte[] getByteArray(Object obj) {
        ByteArrayOutputStream bos = null;
        NoHeaderObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new NoHeaderObjectOutputStream(bos);
            //读取对象并转换成二进制数据
            oos.writeObject(obj);
            return bos.toByteArray();
        } catch (IOException e) {
            log.warn("对象转换成二级制数据失败, {}", e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                    log.error("异常信息:",e);
                }
            }
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    log.error("异常信息:",e);
                }
            }
        }
        return null;
    }

    /**
     * 把二进制数组的数据转回对象
     *
     * @param b
     * @return
     */
    public static Object convertToObject(byte[] b) {
        ByteArrayInputStream bis = null;
        NoHeaderObjectInputStream ois = null;
        try {
            //读取二进制数据并转换成对象
            bis = new ByteArrayInputStream(b);
            ois = new NoHeaderObjectInputStream(bis);
            return ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            log.warn("二进制数据转回对象失败, {}", e);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    log.error("异常信息:",e);
                }
            }
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    log.error("异常信息:",e);
                }
            }
        }
        return null;
    }


    public static byte[] intToBytes(int n) {
        byte[] b = new byte[4];
        for(int i = 0;i< b.length ; i++){
            int shift = (3-i) * 8;
            b[i] = (byte)(n >> shift & 0xff);
        }
        return b;
    }


    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (bytes[i] & 0xFF) << shift;
        }
        return value;
    }

    public static byte[] longToBytes(long n) {
        byte[] b = new byte[8];
        for(int i = 0;i< b.length ; i++){
            int shift = (7-i) * 8;
            b[i] = (byte)(n >> shift & 0xff);
        }
        return b;
    }


    public static long byteArrayToLong(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < 8; i++) {
            int shift = (7 - i) * 8;
            value += (bytes[i] & 0xFF) << shift;
        }
        return value;
    }

    public static byte[] shortToBytes(short n) {
        byte[] b = new byte[2];
        for(int i = 0;i< b.length ; i++){
            int shift = (1-i) * 8;
            b[i] = (byte)(n >> shift & 0xff);
        }
        return b;
    }


    public static short byteArrayToShort(byte[] bytes) {
        short value = 0;
        for (int i = 0; i < 2; i++) {
            int shift = (1 - i) * 8;
            value += (bytes[i] & 0xFF) << shift;
        }
        return value;
    }
}
