package datatypes;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_Text {

    private String value;

    public static final byte valueSerialCode = 0x0C;

    public static final byte nullSerialCode = 0x00;

    private boolean isNull;

    public DT_Text() {
        value = "";
        isNull = true;
    }

    public DT_Text(String value) {
        this(value, value == null);
    }

    public DT_Text(String value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public byte getSerialCode() {
        if(isNull)
            return nullSerialCode;
        else
            return (byte)(valueSerialCode + this.value.length());
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public int getSize() {
        if(isNull)
            return 0;
        return this.value.length();
    }
}
