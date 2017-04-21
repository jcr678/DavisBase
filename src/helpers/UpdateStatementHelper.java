package helpers;

import common.CatalogDB;
import common.Constants;
import common.Utils;
import datatypes.DT_Int;
import datatypes.DT_Text;
import errors.InternalException;
import storage.StorageManager;
import storage.model.DataRecord;
import storage.model.InternalColumn;
import storage.model.InternalCondition;
import storage.model.Page;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Mahesh on 15/4/17.
 */
public class UpdateStatementHelper {

    public int updateSystemTablesTable(String databaseName, String tableName, int columnCount) {
        try {
        /*
         * System Tables Table Schema:
         * Column_no    Name                                    Data_type
         *      1       rowid                                   INT
         *      2       database_name                           TEXT
         *      3       table_name                              TEXT
         *      4       record_count                            INT
         *      5       col_tbl_st_rowid                        INT
         *      6       nxt_avl_col_tbl_rowid                   INT
         */
            StorageManager manager = new StorageManager();
            List<InternalCondition> conditions = new ArrayList<>();
            conditions.add(InternalCondition.CreateCondition(CatalogDB.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DT_Text(tableName)));
            conditions.add(InternalCondition.CreateCondition(CatalogDB.TABLES_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DT_Text(databaseName)));
            List<DataRecord> result = manager.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, conditions, true);
            if (result != null && result.size() == 0) {
                int returnValue = 1;
                Page<DataRecord> page = manager.getLastRecordAndPage(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME);
                //Check if record exists
                DataRecord lastRecord = null;
                if (page.getPageRecords().size() > 0) {
                    lastRecord = page.getPageRecords().get(0);
                }
                DataRecord record = new DataRecord();
                if (lastRecord == null) {
                    record.setRowId(1);
                } else {
                    record.setRowId(lastRecord.getRowId() + 1);
                }
                record.getColumnValueList().add(new DT_Int(record.getRowId()));
                record.getColumnValueList().add(new DT_Text(databaseName));
                record.getColumnValueList().add(new DT_Text(tableName));
                record.getColumnValueList().add(new DT_Int(0));
                if (lastRecord == null) {
                    record.getColumnValueList().add(new DT_Int(1));
                    record.getColumnValueList().add(new DT_Int(columnCount + 1));
                } else {
                    DT_Int startingColumnIndex = (DT_Int) lastRecord.getColumnValueList().get(CatalogDB.TABLES_TABLE_SCHEMA_NXT_AVL_COL_TBL_ROWID);
                    returnValue = startingColumnIndex.getValue();
                    record.getColumnValueList().add(new DT_Int(returnValue));
                    record.getColumnValueList().add(new DT_Int(returnValue + columnCount));
                }
                record.populateSize();
                manager.writeRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, record);
                return returnValue;
            } else {
                Utils.printMessage(String.format("Table '%s.%s' already exists.", databaseName, tableName));
                return -1;
            }
        }
        catch (InternalException e) {
            Utils.printMessage(e.getMessage());
            return -1;
        }
    }

    public boolean updateSystemColumnsTable(String databaseName, String tableName, int startingRowId, List<InternalColumn> columns) {
        try {
        /*
         * System Tables Table Schema:
         * Column_no    Name                                    Data_type
         *      1       rowid                                   INT
         *      2       database_name                           TEXT
         *      3       table_name                              TEXT
         *      4       column_name                             TEXT
         *      5       data_type                               TEXT
         *      6       column_key                              TEXT
         *      7       ordinal_position                        TINYINT
         *      8       is_nullable                             TEXT
         */
            StorageManager manager = new StorageManager();
            if (columns != null && columns.size() == 0) return false;
            int i = 0;
            for (; i < columns.size(); i++) {
                DataRecord record = new DataRecord();
                record.setRowId(startingRowId++);
                record.getColumnValueList().add(new DT_Int(record.getRowId()));
                record.getColumnValueList().add(new DT_Text(databaseName));
                record.getColumnValueList().add(new DT_Text(tableName));
                record.getColumnValueList().add(new DT_Text(columns.get(i).getName()));
                record.getColumnValueList().add(new DT_Text(columns.get(i).getDataType()));
                record.getColumnValueList().add(new DT_Text(columns.get(i).getStringIsPrimary()));
                record.getColumnValueList().add(new DT_Int(i + 1));
                record.getColumnValueList().add(new DT_Text(columns.get(i).getStringIsNullable()));
                record.populateSize();
                if (!manager.writeRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, record)) {
                    break;
                }
            }
            return true;
        }
        catch (InternalException e) {
            Utils.printMessage(e.getMessage());
        }
        return false;
    }

    public static int incrementRowCount(String databaseName, String tableName) {
        return updateRowCount(databaseName, tableName, 1);
    }

    public static int decrementRowCount(String databaseName, String tableName) {
        return updateRowCount(databaseName, tableName, -1);
    }

    private static int updateRowCount(String databaseName, String tableName, int rowCount) {
        try {
            StorageManager manager = new StorageManager();
            List<InternalCondition> conditions = new ArrayList<>();
            conditions.add(InternalCondition.CreateCondition(CatalogDB.TABLES_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DT_Text(databaseName)));
            conditions.add(InternalCondition.CreateCondition(CatalogDB.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DT_Text(tableName)));
            List<Byte> updateColumnsIndexList = new ArrayList<>();
            updateColumnsIndexList.add(CatalogDB.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            List<Object> updateValueList = new ArrayList<>();
            updateValueList.add(new DT_Int(rowCount));
            return manager.updateRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, conditions, updateColumnsIndexList, updateValueList, true);
        }
        catch (InternalException e) {
            Utils.printMessage(e.getMessage());
        }
        return -1;
    }
}
