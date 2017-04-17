package common;

import Model.DataType;
import helpers.UpdateStatementHelper;
import storage.StorageManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dakle on 10/4/17.
 */
public class CatalogDB {

    public static final byte TABLES_TABLE_SCHEMA_ROWID = 0;
    public static final byte TABLES_TABLE_SCHEMA_TABLE_NAME = 1;
    public static final byte TABLES_TABLE_SCHEMA_RECORD_COUNT = 2;
    public static final byte TABLES_TABLE_SCHEMA_COL_TBL_ST_ROWID = 3;
    public static final byte TABLES_TABLE_SCHEMA_NXT_AVL_COL_TBL_ROWID = 4;


    public static final byte COLUMNS_TABLE_SCHEMA_ROWID = 0;
    public static final byte COLUMNS_TABLE_SCHEMA_TABLE_NAME = 1;
    public static final byte COLUMNS_TABLE_SCHEMA_COLUMN_NAME = 2;
    public static final byte COLUMNS_TABLE_SCHEMA_DATA_TYPE = 3;
    public static final byte COLUMNS_TABLE_SCHEMA_COLUMN_KEY = 4;
    public static final byte COLUMNS_TABLE_SCHEMA_ORDINAL_POSITION = 5;
    public static final byte COLUMNS_TABLE_SCHEMA_IS_NULLABLE = 6;

    public boolean createCatalogDB() {
        StorageManager manager = new StorageManager();
        UpdateStatementHelper statement = new UpdateStatementHelper();
        manager.createTable(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME + Constants.DEFAULT_FILE_EXTENSION);
        manager.createTable(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME + Constants.DEFAULT_FILE_EXTENSION);
        List<String> columnNameList = new ArrayList<>();
        List<String> columnDataTypeList = new ArrayList<>();
        List<String> columnKeyConstraintList = new ArrayList<>();
        List<String> columnNullConstraintList = new ArrayList<>();
        int startingRowId = statement.updateSystemTablesTable(Constants.SYSTEM_TABLES_TABLENAME, 5);
        startingRowId *= statement.updateSystemTablesTable(Constants.SYSTEM_COLUMNS_TABLENAME, 7);
        if(startingRowId >= 0) {
            columnNameList.add("rowid");
            columnNameList.add("table_name");
            columnNameList.add("record_count");
            columnNameList.add("col_tbl_st_rowid");
            columnNameList.add("nxt_avl_col_tbl_rowid");
            columnDataTypeList.add(DataType.INT.toString());
            columnDataTypeList.add(DataType.TEXT.toString());
            columnDataTypeList.add(DataType.INT.toString());
            columnDataTypeList.add(DataType.INT.toString());
            columnDataTypeList.add(DataType.INT.toString());
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            statement.updateSystemColumnsTable(Constants.SYSTEM_TABLES_TABLENAME, 1, columnNameList, columnDataTypeList, columnKeyConstraintList, columnNullConstraintList);
            columnNameList.clear();
            columnDataTypeList.clear();
            columnKeyConstraintList.clear();
            columnNullConstraintList.clear();
            columnNameList.add("rowid");
            columnNameList.add("table_name");
            columnNameList.add("column_name");
            columnNameList.add("data_type");
            columnNameList.add("column_key");
            columnNameList.add("ordinal_position");
            columnNameList.add("is_nullable");
            columnDataTypeList.add("INT");
            columnDataTypeList.add("TEXT");
            columnDataTypeList.add("TEXT");
            columnDataTypeList.add("TEXT");
            columnDataTypeList.add("TEXT");
            columnDataTypeList.add("TINYINT");
            columnDataTypeList.add("TEXT");
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnKeyConstraintList.add(null);
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            columnNullConstraintList.add("NO");
            statement.updateSystemColumnsTable(Constants.SYSTEM_COLUMNS_TABLENAME, 6, columnNameList, columnDataTypeList, columnKeyConstraintList, columnNullConstraintList);
        }
        return true;
    }
}
