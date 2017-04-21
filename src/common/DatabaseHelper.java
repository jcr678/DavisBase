package common;

import datatypes.DT_Int;
import datatypes.DT_Text;
import datatypes.base.DT;
import exceptions.InternalException;
import io.IOManager;
import io.model.DataRecord;
import io.model.InternalCondition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by dakle on 21/4/17.
 */
public class DatabaseHelper {

    private static DatabaseHelper databaseHelper = null;

    public static DatabaseHelper getDatabaseHelper() {
        if(databaseHelper == null) {
            return new DatabaseHelper();
        }
        return databaseHelper;
    }

    private IOManager manager;

    private DatabaseHelper() {
        manager = new IOManager();
    }

    public List<String> fetchAllTableColumns(String databaseName, String tableName) throws InternalException {
        List<String> columnNames = new ArrayList<>();
        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = manager.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnNames.add(((DT) object).getStringValue());
        }

        return columnNames;
    }

    public boolean checkNullConstraint(String databaseName, String tableName, HashMap<String, Integer> columnMap) throws InternalException {

        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = manager.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object nullValueObject = record.getColumnValueList().get(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_IS_NULLABLE);
            Object object = record.getColumnValueList().get(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);

            String isNullStr = ((DT) nullValueObject).getStringValue().toUpperCase();
            boolean isNullable = isNullStr.equals("YES");

            if (!columnMap.containsKey(((DT) object).getStringValue()) && !isNullable) {
                Utils.printMessage("Field '" + ((DT) object).getStringValue() + "' cannot be NULL");
                return false;
            }

        }

        return true;
    }

    public HashMap<String, Integer> fetchAllTableColumnDataTypes(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = manager.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        HashMap<String, Integer> columDataTypeMapping = new HashMap<>();

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            Object dataTypeObject = record.getColumnValueList().get(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_DATA_TYPE);

            String columnName = ((DT) object).getStringValue();
            int columnDataType = Utils.stringToDataType(((DT) dataTypeObject).getStringValue());
            columDataTypeMapping.put(columnName.toLowerCase(), columnDataType);
        }

        return columDataTypeMapping;
    }

    public String getTablePrimaryKey(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();

        DT_Text tableNameObj = new DT_Text(tableName);
        DT_Text primaryKeyObj = new DT_Text(SystemDatabaseHelper.PRIMARY_KEY_IDENTIFIER);
        DT_Text databaseObj = new DT_Text(databaseName);


        conditions.add(InternalCondition.CreateCondition(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, databaseObj));
        conditions.add(InternalCondition.CreateCondition(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, tableNameObj));
        conditions.add(InternalCondition.CreateCondition(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_COLUMN_KEY, InternalCondition.EQUALS, primaryKeyObj));

        List<DataRecord> records = manager.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        String columnName = "";
        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(SystemDatabaseHelper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnName = ((DT) object).getStringValue();
            break;
        }

        return columnName;
    }

    public int getTableRecordCount(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.TABLES_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(SystemDatabaseHelper.TABLES_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = manager.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, conditions, true);
        int recordCount = 0;

        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(SystemDatabaseHelper.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            recordCount = Integer.valueOf(((DT) object).getStringValue());
            break;
        }

        return recordCount;
    }

    public boolean checkIfValueForPrimaryKeyExists(String databaseName, String tableName, int value) throws InternalException {
        IOManager manager = new IOManager();
        InternalCondition condition = InternalCondition.CreateCondition(0, InternalCondition.EQUALS, new DT_Int(value));

        List<DataRecord> records = manager.findRecord(databaseName, tableName, condition, false);
        if (records.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
