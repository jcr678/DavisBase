package queries;

import Model.Condition;
import Model.IQuery;
import Model.Literal;
import Model.Result;
import common.Utils;
import datatypes.base.DT;
import errors.InternalException;
import storage.StorageManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Mahesh on 15/4/17.
 */

public class UpdateQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public String columnName;
    public Literal value;
    public Condition condition;

    public UpdateQuery(String databaseName, String tableName, String columnName, Literal value, Condition condition){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.value = value;
        this.condition = condition;
    }

    @Override
    public Result ExecuteQuery() {
        try {
            // public boolean updateRecord(String databaseName, String tableName,
            // List<Byte> searchColumnsIndexList,
            // List<Object> searchKeysValueList,
            // List<Short> searchKeysConditionsList,
            // List<Byte> updateColumnIndexList,
            // List<Object> updateColumnValueList,
            // boolean isIncrement)

            StorageManager manager = new StorageManager();

            HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumnDataTypes(this.databaseName, tableName);
            List<String> retrievedColumns = manager.fetchAllTableColumns(this.databaseName, tableName);
            List<Byte> searchColumnsIndexList = getSearchColumnsIndexList(retrievedColumns);
            List<Object> searchKeysValueList = getSearchKeysValueList(columnDataTypeMapping);
            List<Short> searchKeysConditionsList = getSearchKeysConditionsList(retrievedColumns);
            List<Byte> updateColumnIndexList = getUpdateColumnIndexList(retrievedColumns);
            List<Object> updateColumnValueList = getUpdateColumnValueList(columnDataTypeMapping);

            int rowsAffected = manager.updateRecord(databaseName, tableName, searchColumnsIndexList, searchKeysValueList, searchKeysConditionsList, updateColumnIndexList, updateColumnValueList, false);

            Result result;
            result = new Result(rowsAffected);
            return result;
        }
        catch (InternalException e) {
            Utils.printMessage(e.getMessage());
        }
        return null;
    }

    @Override
    public boolean ValidateQuery() {
        try {
            StorageManager manager = new StorageManager();

            if (!manager.checkTableExists(this.databaseName, tableName)) {
                Utils.printMissingTableError(this.databaseName, tableName);
                return false;
            }

            List<String> retrievedColumns = manager.fetchAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumnDataTypes(this.databaseName, tableName);

            // Validate the columns.
            if (this.condition == null) {
                // No condition.
                // Validate the existence of the column. (Update Column)
                if (!checkColumnValidity(retrievedColumns, false)) {
                    return false;
                }

                // Validate update column data type.
                if (!checkValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, false)) {
                    return false;
                }

                return true;
            } else {
                // Validate the column in the condition.
                // Validate the existence of the column. (Condition Column)
                if (!checkColumnValidity(retrievedColumns, true)) {
                    return false;
                }

                // Validate the existence of the column. (Update Column)
                if (!checkColumnValidity(retrievedColumns, false)) {
                    return false;
                }

                // Validate condition column data type.
                if (!checkValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, true)) {
                    return false;
                }

                // Validate update column data type.
                if (!checkValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, false)) {
                    return false;
                }
            }

            return true;
        }
        catch (InternalException e) {
            Utils.printMessage(e.getMessage());
        }
        return false;
    }

    private boolean checkValueDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, boolean isConditionCheck) {
        String invalidColumn = "";

        String column = isConditionCheck ? condition.column : columnName;
        Literal columnValue = isConditionCheck ? condition.value : value;

        if (columnsList.contains(column)) {
            int dataTypeIndex = columnDataTypeMapping.get(column);
            Literal literal = columnValue;

            // Check if the data type is a integer type.
            if (literal.type != Utils.internalDataTypeToModelDataType((byte)dataTypeIndex)) {
                // Check if the data type can be updated in the literal.
                if (Utils.canUpdateLiteralDataType(literal, dataTypeIndex)) {
                    return true;
                }

                // The data is type of integer, real or double.
                invalidColumn = column;
            }
        }

        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printError("The value of the column " + invalidColumn + " is invalid.");

        }

        return valid;
    }

    private boolean checkColumnValidity(List<String> retrievedColumns, boolean isConditionCheck) {
        boolean columnsValid = true;
        String invalidColumn = "";

        String tableColumn = isConditionCheck ? condition.column : columnName;
        if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
            columnsValid = false;
            invalidColumn = tableColumn;
        }

        if (!columnsValid) {
            Utils.printError("Column " + invalidColumn + " is not present in the table " + tableName + ".");
            return false;
        }

        return true;
    }

    // public boolean updateRecord(String databaseName, String tableName,
    // List<Byte> searchColumnsIndexList,
    // List<Object> searchKeysValueList,
    // List<Short> searchKeysConditionsList,
    // List<Byte> updateColumnIndexList,
    // List<Object> updateColumnValueList,
    // boolean isIncrement)

    private List<Byte> getSearchColumnsIndexList(List<String>retrievedList) {
        List<Byte> list = new ArrayList<>();
        if (condition != null) {
            int idx = retrievedList.indexOf(condition.column);
            list.add((byte)idx);
        }

        return list;
    }

    private List<Object> getSearchKeysValueList(HashMap<String, Integer> columnDataTypeMapping) {
        List<Object> list = new ArrayList<>();
        if (condition != null) {
            byte dataTypeIndex = (byte)columnDataTypeMapping.get(this.condition.column).intValue();
            DT dataType = DT.createSystemDT(this.condition.value.value, dataTypeIndex);
            list.add(dataType);
        }

        return list;
    }

    private List<Short> getSearchKeysConditionsList(List<String>retrievedList) {
        List<Short> list = new ArrayList<>();
        if (condition != null) {
            list.add(Utils.ConvertFromOperator(condition.operator));
        }

        return list;
    }

    private List<Byte> getUpdateColumnIndexList(List<String>retrievedList) {
        List<Byte> list = new ArrayList<>();
        int idx = retrievedList.indexOf(columnName);
        list.add((byte)idx);

        return list;
    }

    private List<Object> getUpdateColumnValueList(HashMap<String, Integer> columnDataTypeMapping) {
        List<Object> list = new ArrayList<>();
        byte dataTypeIndex = (byte) columnDataTypeMapping.get(columnName).intValue();

        DT dataType = DT.createSystemDT(value.value, dataTypeIndex);
        list.add(dataType);

        return list;
    }
}
