package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_Real {

    private float value;

    public static final byte valueSerialCode = 0x08;

    public static final byte nullSerialCode = 0x02;

    private boolean isNull;

    public static final byte BYTES = Float.BYTES;

    public DT_Real() {
        value = 0;
        isNull = true;
    }

    public DT_Real(Float value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Real(float value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
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
