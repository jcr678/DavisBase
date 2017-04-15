package datatypes;

import common.Constants;
import datatypes.base.DT_Numeric;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_SmallInt extends DT_Numeric<Short> {

    public DT_SmallInt() {
        this((short) 0, true);
    }

    public DT_SmallInt(Short value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_SmallInt(short value, boolean isNull) {
        super(Constants.SMALL_INT_SERIAL_TYPE_CODE, Constants.TWO_BYTE_NULL_SERIAL_TYPE_CODE, Short.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Short value) {
        this.value = (short)(this.value + value);
    }

    @Override
    public boolean compare(DT_Numeric<Short> object2, short condition) {
        switch (condition) {
            case DT_Numeric.EQUALS:
                return value == object2.getValue();

            case DT_Numeric.GREATER_THAN:
                return value > object2.getValue();

            case DT_Numeric.LESS_THAN:
                return value < object2.getValue();

            case DT_Numeric.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case DT_Numeric.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }
}
