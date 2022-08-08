package org.apache.iotdb.utils.core.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/6/24 10:06
 */
@Data
public class IField implements Serializable {

    public static final long serialVersionUID = 1L;

    private String columnName;

    private FieldCopy field;

}
