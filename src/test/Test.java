package test;

import common.Constants;
import common.Utils;
import datatypes.DT_Text;
import datatypes.base.DT;
import datatypes.base.DT_Numeric;
import storage.StorageManager;
import storage.model.DataRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dakle on 15/4/17.
 */
public class Test {

    public void run(int numberOfTestCases) {
        switch (numberOfTestCases) {
            case 1:
                fetchTableColumns(Constants.SYSTEM_COLUMNS_TABLENAME);

            case 2:
                fetchSelectiveTableColumns(Constants.SYSTEM_COLUMNS_TABLENAME);
        }
    }

    public void fetchTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    public void fetchSelectiveTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
        List<Byte> selectionIndexList = new ArrayList<>();
        selectionIndexList.add((byte) 0);
        selectionIndexList.add((byte) 1);
        selectionIndexList.add((byte) 2);
        selectionIndexList.add((byte) 5);
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, selectionIndexList, false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }
}
