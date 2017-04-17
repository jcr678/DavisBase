package datatypes.base;

/**
 * Created by dakle on 13/4/17.
 */
public abstract class DT<T> {

    protected T value;

    protected boolean isNull;

    protected final byte valueSerialCode;

    protected final byte nullSerialCode;

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
