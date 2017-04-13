package parser;

import common.Constants;
import common.Utils;
import datatypes.DT_Int;
import datatypes.DT_Text;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import storage.StorageManager;
import storage.model.DataRecord;
import storage.model.Page;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dakle on 30/3/17.
 */
public class CreateStatement implements StatementInterface {

    private final String TABLE_NAME = "table_name";

    JSONObject jsonObject;

    public CreateStatement() {
        jsonObject = new JSONObject();
    }

    @Override
    public JSONObject createObject(String query) {
        jsonObject.put(COMMAND, StatementKeywords.CREATE_STATEMENT);
        jsonObject.put(CODE, StatementKeywords.CREATE_STATEMENT_CODE);
        JSONObject attrsObject = getAttributes(query);
        if(attrsObject == null) return null;
        jsonObject.put(ATTRS, attrsObject);
        JSONArray childrenObject = getChildren(query);
        if(childrenObject == null) return null;
        jsonObject.put(CHILDREN, childrenObject);
        return jsonObject;
    }

    @Override
    public JSONObject getAttributes(String query) {
        return new JSONObject();
    }

    @Override
    public JSONArray getChildren(String query) {
        JSONArray children = new JSONArray();
        Pattern pattern = Pattern.compile(RegexExpressions.CREATE_STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            String subType = matcher.group(1);
            switch (subType) {
                case StatementKeywords.TABLE_STATEMENT:
                    children.add(getTableJson(query));
                    break;
            }
        }
        return children;
    }

    public JSONObject getTableJson(String query) {
        Pattern pattern = Pattern.compile(RegexExpressions.CREATE_TABLE_STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            JSONObject tableJsonObject = new JSONObject();
            tableJsonObject.put(TABLE_NAME, matcher.group(1));
            //https://regex101.com/r/K6ajX6/7
            return tableJsonObject;
        }
        return null;
    }

    public boolean createTable(String databaseName, JSONObject createTableJSON, boolean isSystemTable) {
        if(createTableJSON != null) {
            if((int)createTableJSON.get(CODE) == StatementKeywords.CREATE_STATEMENT_CODE) {
                StorageManager manager = new StorageManager();
                String tableName = (String) createTableJSON.get(TABLE_NAME);
                if(isSystemTable) {
                    manager.createFile(Constants.DEFAULT_DATA_DIRNAME + "/" + databaseName, tableName + Constants.DEFAULT_FILE_EXTENSION);
                }
                else if(!isSystemTable) {
                    manager.createFile(Constants.DEFAULT_DATA_DIRNAME + "/" + databaseName + "/" + tableName, tableName + "/" + Constants.DEFAULT_FILE_EXTENSION);
                }
            }
        }
        return false;
    }

    public int updateSystemTablesTable(String tableName, int columnCount) {
        /*
         * System Tables Table Schema:
         * Column_no    Name                                    Data_type
         *      1       rowid                                   INT
         *      2       table_name                              TEXT
         *      3       record_count                            INT
         *      4       col_tbl_st_rowid                        INT
         *      5       nxt_avl_col_tbl_rowid                   INT
         */
        StorageManager manager = new StorageManager();
        if(manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, (byte) 1, new DT_Text(tableName)) == null) {
            int returnValue = 1;
            Page<DataRecord> page = manager.getLastRecordAndPage(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME);
            //Check if record exists
            DataRecord lastRecord = null;
            if (page.getPageRecords().size() > 0) {
                lastRecord = page.getPageRecords().get(0);
            }
            DataRecord record = new DataRecord();
            if(lastRecord == null) {
                record.setRowId(1);
            }
            else {
                record.getColumnValueList().add(new DT_Int(lastRecord.getRowId() + 1));
            }
            record.getColumnValueList().add(new DT_Int(record.getRowId()));
            record.getColumnValueList().add(new DT_Text(tableName));
            record.getColumnValueList().add(new DT_Int(0));
            if(lastRecord == null) {
                record.getColumnValueList().add(new DT_Int(1));
                record.getColumnValueList().add(new DT_Int(columnCount + 1));
            }
            else {
                DT_Int startingColumnIndex = (DT_Int) lastRecord.getColumnValueList().get(3);
                returnValue = startingColumnIndex.getValue();
                record.getColumnValueList().add(new DT_Int(returnValue));
                record.getColumnValueList().add(new DT_Int(returnValue + columnCount));
            }
            record.populateSize();
            manager.writeRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, record);
            return returnValue;
        }
        else {
            System.out.println("Table already exists!");
            return -1;
        }
    }

    public boolean updateSystemColumnsTable(String tableName, int startingRowId, List<String> columnNames, List<String> columnDataType, List<String> columnKeyConstraints, List<String> columnNullConstraints) {
        /*
         * System Tables Table Schema:
         * Column_no    Name                                    Data_type
         *      1       rowid                                   INT
         *      2       table_name                              TEXT
         *      3       column_name                             TEXT
         *      4       data_type                               TEXT
         *      5       column_key                              TEXT
         *      6       ordinal_position                        TINYINT
         *      7       is_nullable                             TEXT
         */
        StorageManager manager = new StorageManager();
        if(columnNames.size() != columnDataType.size() && columnDataType.size() != columnKeyConstraints.size() && columnKeyConstraints.size() != columnNullConstraints.size()) return false;
        for(int i = 0; i < columnNames.size(); i++) {
            DataRecord record = new DataRecord();
            record.setRowId(startingRowId++);
            record.getColumnValueList().add(new DT_Int(record.getRowId()));
            record.getColumnValueList().add(new DT_Text(tableName));
            record.getColumnValueList().add(new DT_Text(columnNames.get(i)));
            record.getColumnValueList().add(new DT_Text(columnDataType.get(i)));
            record.getColumnValueList().add(new DT_Text(columnKeyConstraints.get(i)));
            record.getColumnValueList().add(new DT_Int(i + 1));
            record.getColumnValueList().add(new DT_Text(columnNullConstraints.get(i)));
            record.populateSize();
            manager.writeRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, record);
        }
        return true;
    }
}
