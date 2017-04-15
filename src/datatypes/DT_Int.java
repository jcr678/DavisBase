package datatypes;

import common.Constants;
import datatypes.base.DT_Numeric;

/**
 * Created by dakle on 10/4/17.
 */
public class DT_Int extends DT_Numeric<Integer> {

    public DT_Int() {
        this(0, true);
    }

    public DT_Int(Integer value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Int(int value, boolean isNull) {
        super(Constants.INT_SERIAL_TYPE_CODE, Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Integer.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Integer value) {
        this.value += value;
    }

    @Override
    public boolean compare(DT_Numeric<Integer> object2, short condition) {
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
