package test;

import Model.DataType;
import common.Constants;
import common.Utils;
import datatypes.DT_Text;
import datatypes.base.DT;
import datatypes.base.DT_Numeric;
import helpers.UpdateStatementHelper;
import storage.StorageManager;
import storage.model.DataRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

            case 3:
                selectAll(Constants.SYSTEM_TABLES_TABLENAME);
                break;

            case 4:
                deleteTableName(Constants.SYSTEM_COLUMNS_TABLENAME);

            case 5:
                deleteTableColumns(Constants.SYSTEM_COLUMNS_TABLENAME);

            case 6:
                for (int i = 0; i < 100; i++) {
                    insertDummyTableAndColumns();
                }
        }
    }

    public void fetchTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        columnIndexList.add((byte) 3);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        valueList.add(new DT_Text("TEXT"));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
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
        selectionIndexList.add((byte) 5);
        selectionIndexList.add((byte) 2);
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, selectionIndexList, false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    private void selectAll(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Short> conditionList = new ArrayList<>();
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, columnIndexList, valueList, conditionList,false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    private void deleteTableName(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
        System.out.println(manager.deleteRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, columnIndexList, valueList, conditionList,true));
    }

    private void deleteTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Short> conditionList = new ArrayList<>();
        System.out.println(manager.deleteRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList,false));
    }

    private void insertDummyTableAndColumns() {
        Random random = new Random(System.nanoTime());
        StorageManager manager = new StorageManager();
        UpdateStatementHelper statement = new UpdateStatementHelper();
        List<String> columnNameList = new ArrayList<>();
        List<String> columnDataTypeList = new ArrayList<>();
        List<String> columnKeyConstraintList = new ArrayList<>();
        List<String> columnNullConstraintList = new ArrayList<>();
//        int startingRowId = statement.updateSystemTablesTable(String.valueOf(random.nextInt()), 5);
//        if(startingRowId >= 0) {
//            columnNameList.add("rowid");
//            columnNameList.add("table_name");
//            columnNameList.add("record_count");
//            columnNameList.add("col_tbl_st_rowid");
//            columnNameList.add("nxt_avl_col_tbl_rowid");
//            columnDataTypeList.add(DataType.INT.toString());
//            columnDataTypeList.add(DataType.TEXT.toString());
//            columnDataTypeList.add(DataType.INT.toString());
//            columnDataTypeList.add(DataType.INT.toString());
//            columnDataTypeList.add(DataType.INT.toString());
//            columnKeyConstraintList.add(null);
//            columnKeyConstraintList.add(null);
//            columnKeyConstraintList.add(null);
//            columnKeyConstraintList.add(null);
//            columnKeyConstraintList.add(null);
//            columnNullConstraintList.add("NO");
//            columnNullConstraintList.add("NO");
//            columnNullConstraintList.add("NO");
//            columnNullConstraintList.add("NO");
//            columnNullConstraintList.add("NO");
//            statement.updateSystemColumnsTable(Constants.SYSTEM_TABLES_TABLENAME, startingRowId, columnNameList, columnDataTypeList, columnKeyConstraintList, columnNullConstraintList);
//        }
    }
}
