package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_SmallInt {

    private short value;

    public static final byte valueSerialCode = 0x05;

    public static final byte nullSerialCode = 0x01;

    private boolean isNull;

    public static final byte BYTES = Short.BYTES;

    public DT_SmallInt() {
        value = 0;
        isNull = true;
    }

    public DT_SmallInt(Short value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_SmallInt(short value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public short getValue() {
        return value;
    }

    public void setValue(short value) {
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
