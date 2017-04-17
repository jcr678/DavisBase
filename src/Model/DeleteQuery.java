package Model;

import common.Constants;
import common.Utils;
import datatypes.base.DT;
import storage.StorageManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class DeleteQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public Condition condition;
    public boolean isInternal = false;

    public DeleteQuery(String databaseName, String tableName, Condition condition){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.condition = condition;
    }

    public DeleteQuery(String databaseName, String tableName, Condition condition, boolean isInternal){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.condition = condition;
        this.isInternal = isInternal;
    }

    @Override
    public Result ExecuteQuery() {

        // Delete the record.
        boolean status = false;
        int rowCount = 0;
        StorageManager manager = new StorageManager();
        if (condition == null) {
            rowCount = manager.getTableRecordCount(tableName);
            status = manager.deleteRecord(Utils.getUserDatabasePath(databaseName), tableName, (new ArrayList<>()), (new ArrayList<>()), (new ArrayList<>()), false);
        }
        else {
            rowCount = 1;
            List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
            int idx = retrievedColumns.indexOf(condition.column);
            List<Byte> columnIndexList = new ArrayList<>();
            columnIndexList.add((byte)idx);

            DT dataType = DT.CreateDT(this.condition.value);
            List<Object> valueList = new ArrayList<>();
            valueList.add(dataType);

            List<Short> conditionList = new ArrayList<>();
            conditionList.add(Utils.ConvertFromOperator(condition.operator));

            status = manager.deleteRecord(Utils.getUserDatabasePath(databaseName), tableName, (columnIndexList), (valueList), (conditionList), false);
        }

        Result result = null;
        if (status) {
            result = new Result(rowCount, this.isInternal);
        }
        else {
            result = new Result(0, this.isInternal);
        }

        return result;
    }

    @Override
    public boolean ValidateQuery() {
        /*TODO : replace with actual logic*/
        // Check if the table exists.
        if (!StorageManager.checkTableExists(Utils.getUserDatabasePath(this.databaseName), tableName)) {
            Utils.printMessage("Table " + tableName + " does not exist.");
            return false;
        }

        // Validate the columns.
        if (this.condition == null) {
            // No condition.
            return true;
        }
        else {
            // Condition is present.
            // Validate the column in the condition.
            StorageManager manager = new StorageManager();
            List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
            HashMap<String, Byte> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);

            // Validate the existence of the column.
            if(!checkConditionColumnValidity(retrievedColumns)) {
                return false;
            }


            // Validate column data type.
            String columnName = checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns);
            boolean valid = (columnName.length() > 0) ? false : true;

            if (!valid) {
                Utils.printMessage("The value of the column " + columnName + " is invalid.");
                return false;
            }
        }
        return true;
    }

    private String checkConditionValueDataTypeValidity(HashMap<String, Byte> columnDataTypeMapping, List<String> columnsList) {
        String invalidColumn = "";

        if (columnsList.contains(condition.column)) {
            int dataTypeIndex = columnDataTypeMapping.get(condition.column);
            Literal literal = condition.value;

            // Check if the data type is a integer type.
            if (dataTypeIndex != Constants.INVALID_CLASS && dataTypeIndex <= Constants.DOUBLE) {
                // The data is type of integer, real or double.
                if (!Utils.canConvertStringToDouble(literal.value)) {
                    invalidColumn = condition.column;
                }
            } else if (dataTypeIndex == Constants.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = condition.column;
                }
            } else if (dataTypeIndex == Constants.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = condition.column;
                }
            }
        }

        return invalidColumn;
    }

    private boolean checkConditionColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        String tableColumn = condition.column;
        if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
            columnsValid = false;
            invalidColumn = tableColumn;
        }

        if (!columnsValid) {
            Utils.printMessage("Column " + invalidColumn + " is not present in the table " + tableName + ".");
            return false;
        }

        return true;
    }
}
