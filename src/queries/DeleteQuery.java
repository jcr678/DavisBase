package queries;

import Model.Condition;
import Model.IQuery;
import Model.Result;
import common.Utils;
import datatypes.base.DT;
import storage.StorageManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
        int rowCount = 0;
        StorageManager manager = new StorageManager();
        if (condition == null) {
            rowCount = manager.deleteRecord(databaseName, tableName, (new ArrayList<>()), (new ArrayList<>()), (new ArrayList<>()), false);
        }
        else {
            rowCount = 1;
            List<String> retrievedColumns = manager.fetchAllTableColumns(this.databaseName, tableName);
            int idx = retrievedColumns.indexOf(condition.column);
            List<Byte> columnIndexList = new ArrayList<>();
            columnIndexList.add((byte)idx);

            DT dataType = DT.CreateDT(this.condition.value);
            List<Object> valueList = new ArrayList<>();
            valueList.add(dataType);

            List<Short> conditionList = new ArrayList<>();
            conditionList.add(Utils.ConvertFromOperator(condition.operator));

            rowCount = manager.deleteRecord(databaseName, tableName, (columnIndexList), (valueList), (conditionList), false);
        }

        Result result = new Result(rowCount, this.isInternal);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        // Check if the table exists.
        StorageManager manager = new StorageManager();
        if (!manager.checkTableExists(this.databaseName, tableName)) {
            Utils.printMissingTableError(this.databaseName, tableName);
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
            List<String> retrievedColumns = manager.fetchAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumnDataTypes(this.databaseName, tableName);

            // Validate the existence of the column.
            if(!checkConditionColumnValidity(retrievedColumns)) {
                return false;
            }

            // Validate column data type.
            if(!Utils.checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, condition)) {
                return false;
            }
        }
        return true;
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
