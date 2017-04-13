package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_BigInt {

    private long value;

    public static final byte valueSerialCode = 0x07;

    public static final byte nullSerialCode = 0x03;

    private boolean isNull;

    public static final byte BYTES = Long.BYTES;

    public DT_BigInt() {
        value = 0;
        isNull = true;
    }

    public DT_BigInt(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_BigInt(long value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
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
