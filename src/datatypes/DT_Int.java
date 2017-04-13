package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_Int {

    private int value;

    public static final byte valueSerialCode = 0x06;

    public static final byte nullSerialCode = 0x02;

    private boolean isNull;

    public static final byte BYTES = Integer.BYTES;

    public DT_Int() {
        value = 0;
        isNull = true;
    }

    public DT_Int(Integer value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Int(int value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
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
