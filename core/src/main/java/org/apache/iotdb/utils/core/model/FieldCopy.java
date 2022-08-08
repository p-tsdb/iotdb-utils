package org.apache.iotdb.utils.core.model;

import org.apache.iotdb.tsfile.exception.NullFieldException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.Serializable;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/19 10:33
 */
public class FieldCopy implements Serializable {
    public static final long serialVersionUID = 1L;
    private TSDataType dataType;
    private boolean boolV;
    private int intV;
    private long longV;
    private float floatV;
    private double doubleV;
    private Binary binaryV;

    public FieldCopy(TSDataType dataType) {
        this.dataType = dataType;
    }

    public static FieldCopy copy(Field field) {
        if(field == null){
            return null;
        }
        FieldCopy out = new FieldCopy(field.getDataType());
        if (out.dataType != null) {
            switch(out.dataType) {
                case DOUBLE:
                    out.setDoubleV(field.getDoubleV());
                    break;
                case FLOAT:
                    out.setFloatV(field.getFloatV());
                    break;
                case INT64:
                    out.setLongV(field.getLongV());
                    break;
                case INT32:
                    out.setIntV(field.getIntV());
                    break;
                case BOOLEAN:
                    out.setBoolV(field.getBoolV());
                    break;
                case TEXT:
                    out.setBinaryV(field.getBinaryV());
                    break;
                default:
                    throw new UnSupportedDataTypeException(out.dataType.toString());
            }
        }

        return out;
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public boolean getBoolV() {
        if (this.dataType == null) {
            throw new NullFieldException();
        } else {
            return this.boolV;
        }
    }

    public void setBoolV(boolean boolV) {
        this.boolV = boolV;
    }

    public int getIntV() {
        if (this.dataType == null) {
            throw new NullFieldException();
        } else {
            return this.intV;
        }
    }

    public void setIntV(int intV) {
        this.intV = intV;
    }

    public long getLongV() {
        if (this.dataType == null) {
            throw new NullFieldException();
        } else {
            return this.longV;
        }
    }

    public void setLongV(long longV) {
        this.longV = longV;
    }

    public float getFloatV() {
        if (this.dataType == null) {
            throw new NullFieldException();
        } else {
            return this.floatV;
        }
    }

    public void setFloatV(float floatV) {
        this.floatV = floatV;
    }

    public double getDoubleV() {
        if (this.dataType == null) {
            throw new NullFieldException();
        } else {
            return this.doubleV;
        }
    }

    public void setDoubleV(double doubleV) {
        this.doubleV = doubleV;
    }

    public Binary getBinaryV() {
        if (this.dataType == null) {
            throw new NullFieldException();
        } else {
            return this.binaryV;
        }
    }

    public void setBinaryV(Binary binaryV) {
        this.binaryV = binaryV;
    }

    public String getStringValue() {
        if (this.dataType == null) {
            return "null";
        } else {
            switch(this.dataType) {
                case DOUBLE:
                    return String.valueOf(this.doubleV);
                case FLOAT:
                    return String.valueOf(this.floatV);
                case INT64:
                    return String.valueOf(this.longV);
                case INT32:
                    return String.valueOf(this.intV);
                case BOOLEAN:
                    return String.valueOf(this.boolV);
                case TEXT:
                    return this.binaryV.toString();
                default:
                    throw new UnSupportedDataTypeException(this.dataType.toString());
            }
        }
    }

    public String toString() {
        return this.getStringValue();
    }

    public Object getObjectValue(TSDataType dataType) {
        if (this.dataType == null) {
            return null;
        } else {
            switch(dataType) {
                case DOUBLE:
                    return this.getDoubleV();
                case FLOAT:
                    return this.getFloatV();
                case INT64:
                    return this.getLongV();
                case INT32:
                    return this.getIntV();
                case BOOLEAN:
                    return this.getBoolV();
                case TEXT:
                    return this.getBinaryV();
                default:
                    throw new UnSupportedDataTypeException(dataType.toString());
            }
        }
    }
}
