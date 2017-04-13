package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_Double {

    private double value;

    public static final byte valueSerialCode = 0x09;

    public static final byte nullSerialCode = 0x03;

    private boolean isNull;

    public static final byte BYTES = Double.BYTES;

    public DT_Double() {
        value = 0;
        isNull = true;
    }

    public DT_Double(Double value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Double(double value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
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
