package org.apache.iotdb.utils.core.model;

/**
 * model for import params
 */
public class ImportModel {

    public static final String CSV_SUFFIXS = "csv";
    public static final String GZIP_SUFFIXS = "gzip";
    public static final String SNAPPY_SUFFIXS = "snappy";

    //使用的压缩方式
    private CompressType compressType;
    //文件路径
    private String targetPath;
    //批量值
    private int batchPointSize;
    //是否时间对齐
    private boolean aligned;
    //文件编码
    private String charSet;

    public CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public int getBatchPointSize() {
        return batchPointSize;
    }

    public void setBatchPointSize(int batchPointSize) {
        this.batchPointSize = batchPointSize;
    }

    public boolean isAligned() {
        return aligned;
    }

    public void setAligned(boolean aligned) {
        this.aligned = aligned;
    }

    public String getCharSet() {
        return charSet;
    }

    public void setCharSet(String charSet) {
        this.charSet = charSet;
    }
}
