package datatypes;

import java.util.Date;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_DateTime {

    private long value;

    public static final byte valueSerialCode = 0x0A;

    public static final byte nullSerialCode = 0x03;

    private boolean isNull;

    public static final short BYTES = Long.BYTES;

    public DT_DateTime() {
        value = 0;
        isNull = true;
    }

    public DT_DateTime(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_DateTime(long value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public String getStringValue() {
        Date date = new Date(this.value);
        return date.toString();
    }

    public byte getSerialCode() {
        if(isNull)
            return nullSerialCode;
        else
            return valueSerialCode;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }
}
