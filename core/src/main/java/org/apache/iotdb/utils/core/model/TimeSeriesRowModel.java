package org.apache.iotdb.utils.core.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class TimeSeriesRowModel implements Serializable {
    public static final long serialVersionUID = 1L;
    //时间戳
    private String timestamp;
    //设备信息
    private DeviceModel deviceModel;
    //行数据
    private List<IField> iFieldList;

}
