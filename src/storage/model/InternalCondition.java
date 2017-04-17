package storage.model;

/**
 * Created by dakle on 16/4/17.
 */
public class InternalCondition {

    public static final short EQUALS = 0;
    public static final short LESS_THAN = 1;
    public static final short GREATER_THAN = 2;
    public static final short LESS_THAN_EQUALS = 3;
    public static final short GREATER_THAN_EQUALS = 4;

    private byte index;

    private short condition;

    private Object value;

    public InternalCondition(byte index, short condition, Object value) {
        this.index = index;
        this.condition = condition;
        this.value = value;
    }

    public InternalCondition(int index, short condition, Object value) {
        this.index = (byte) index;
        this.condition = condition;
        this.value = value;
    }

    public byte getIndex() {
        return index;
    }

    public void setIndex(byte index) {
        this.index = index;
    }

    public short getCondition() {
        return condition;
    }

    public void setCondition(short condition) {
        this.condition = condition;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
