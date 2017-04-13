package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_TinyInt {

    private byte value;

    public static final byte valueSerialCode = 0x04;

    public static final byte nullSerialCode = 0x00;

    private boolean isNull;

    public static final byte BYTES = Byte.BYTES;

    public DT_TinyInt() {
        value = 0;
        isNull = true;
    }

    public DT_TinyInt(Byte value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_TinyInt(byte value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public byte getValue() {
        return value;
    }

    public void setValue(byte value) {
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
