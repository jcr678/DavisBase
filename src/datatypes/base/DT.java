package datatypes.base;

import Model.Literal;
import datatypes.*;

/**
 * Created by dakle on 13/4/17.
 */
public abstract class DT<T> {

    protected T value;

    protected boolean isNull;

    protected final byte valueSerialCode;

    protected final byte nullSerialCode;

    public static DT CreateDT(Literal value) {
        switch(value.type) {
            case TINYINT:
                return new DT_TinyInt(Byte.valueOf(value.value));
            case SMALLINT:
                return new DT_SmallInt(Short.valueOf(value.value));
            case BIGINT:
                return new DT_BigInt(Long.valueOf(value.value));
            case INT:
                return new DT_Int(Integer.valueOf(value.value));
            case REAL:
                return new DT_Real(Float.valueOf(value.value));
            case DOUBLE:
                return new DT_Double(Double.valueOf(value.value));
            case DATETIME:
                // TODO : Create DateTime
                return new DT_DateTime();
            case DATE:
                // TODO : Create Date
                return new DT_Date();
            case TEXT:
                return new DT_Text(value.value);
        }

        return null;
    }

    protected DT(int valueSerialCode, int nullSerialCode) {
        this.valueSerialCode = (byte) valueSerialCode;
        this.nullSerialCode = (byte) nullSerialCode;
    }

    public T getValue() {
        return value;
    }

    public String getStringValue() {
        if(value == null) {
            return "NULL";
        }
        return value.toString();
    }

    public void setValue(T value) {
        this.value = value;
         if (value != null) {
             this.isNull = false;
         }
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public byte getValueSerialCode() {
        return valueSerialCode;
    }

    public byte getNullSerialCode() {
        return nullSerialCode;
    }
}
