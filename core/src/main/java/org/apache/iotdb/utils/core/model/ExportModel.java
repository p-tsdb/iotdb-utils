package org.apache.iotdb.utils.core.model;

import java.time.ZoneId;
import java.util.List;

/**
 * model for export params
 */
public class ExportModel {

    //使用的压缩方式
    private CompressType compressType;
    //为导出的CSV文件指定输出路径
    private String targetDirectory;
    //导出的文件名
    private String targetFile;
    //在CSV文件的header中时间序列的后面打印出对应的数据类型，例如：Time, root.sg1.d1.s1(INT32), root.sg1.d1.s2(INT64)
    private boolean needDataTypePrinted;
    //不需要传
    private String timestampPrecision;
    //时区
    private ZoneId zoneId;
    //时间格式  如果想要时间戳的话timestamp，其他格式yyyy-MM-dd\ HH:mm:ss
    private String timeFormat;
    //文件编码格式
    private String charSet;
    //是否增量
    private boolean isAppend;
    //不需要传
    List<String> csvCloumnNameList;

    public CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
    }

    public String getTargetDirectory() {
        return targetDirectory;
    }

    public void setTargetDirectory(String targetDirectory) {
        this.targetDirectory = targetDirectory;
    }

    public String getTargetFile() {
        return targetFile;
    }

    public void setTargetFile(String targetFile) {
        this.targetFile = targetFile;
    }

    public boolean isNeedDataTypePrinted() {
        return needDataTypePrinted;
    }

    public void setNeedDataTypePrinted(boolean needDataTypePrinted) {
        this.needDataTypePrinted = needDataTypePrinted;
    }

    public String getTimestampPrecision() {
        return timestampPrecision;
    }

    public void setTimestampPrecision(String timestampPrecision) {
        this.timestampPrecision = timestampPrecision;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public void setZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getCharSet() {
        return charSet;
    }

    public void setCharSet(String charSet) {
        this.charSet = charSet;
    }

    public boolean isAppend() {
        return isAppend;
    }

    public void setAppend(boolean append) {
        isAppend = append;
    }

    public List getCsvCloumnNameList() {
        return csvCloumnNameList;
    }

    public void setCsvCloumnNameList(List csvCloumnNameList) {
        this.csvCloumnNameList = csvCloumnNameList;
    }
}
