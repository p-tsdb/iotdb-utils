package org.apache.iotdb.utils.core.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/6/24 10:40
 */
@Data
public class DeviceModel implements Serializable {
    public static final long serialVersionUID = 1L;
    //设备实体路径，也是timeseries前缀路径
    private String deviceName;

    //是否是时间对齐的
    private boolean aligned;

}
