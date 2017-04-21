package queries;

import Model.IQuery;
import Model.Literal;
import Model.Result;
import common.Constants;
import common.Utils;
import datatypes.*;
import datatypes.base.DT;
import errors.InternalException;
import storage.StorageManager;
import storage.model.DataRecord;
import java.util.*;

public class InsertQuery implements IQuery {
    public String tableName;
    public ArrayList<String> columns;
    public ArrayList<Literal> values;
    public String databaseName;

    public InsertQuery(String databaseName, String tableName, ArrayList<String> columns, ArrayList<Literal> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        try {
            // All checks are done. Now insert the values.
            StorageManager manager = new StorageManager();
            List<String> retrievedColumns = manager.fetchAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumnDataTypes(this.databaseName, tableName);

            DataRecord record = new DataRecord();
            generateRecords(record.getColumnValueList(), columnDataTypeMapping, retrievedColumns);

            int rowID = findRowID(manager, retrievedColumns);
            record.setRowId(rowID);
            record.populateSize();

            Result result = null;
            boolean status = manager.writeRecord(this.databaseName, tableName, record);
            if (status) {
                result = new Result(1);
            } else {
                result = new Result(0);
            }

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
            // validate if the table and the columns of the table.
            StorageManager manager = new StorageManager();
            if (!manager.checkTableExists(this.databaseName, tableName)) {
                Utils.printMissingTableError(this.databaseName, tableName);
                return false;
            }

            // Table columns.
            List<String> retrievedColumns = manager.fetchAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumnDataTypes(this.databaseName, tableName);

            if (columns == null) {
                // No columns are provided.
                // Check values size.
                if (values.size() > retrievedColumns.size()) {
                    Utils.printError("Column count doesn't match value count at row 1");
                    return false;
                }

                // Check Columns datatype are valid.
                Utils utils = new Utils();
                if (!utils.checkDataTypeValidity(columnDataTypeMapping, retrievedColumns, values)) {
                    return false;
                }
            } else {
                // Columns are provided.
                // Validate columns.
                // If the column list is greater than the columns in the table then throw an error.
                if (columns.size() > retrievedColumns.size()) {
                    Utils.printError("Column count doesn't match value count at row 1");
                    return false;
                }

                // Check columns validity.
                boolean areColumnsValid = checkColumnValidity(retrievedColumns);
                if (!areColumnsValid) {
                    return false;
                }

                // Check Columns datatype are valid.
                boolean areColumnsDataTypeValid = validateColumnDataTypes(columnDataTypeMapping);
                if (!areColumnsDataTypeValid) {
                    return false;
                }
            }

            // Common methods.
            // Validate null columns.
            boolean isNullConstraintValid = checkNullConstraint(manager, retrievedColumns);
            if (!isNullConstraintValid) {
                return false;
            }

            // Valid columns.
        /*Test test = new Test();
        test.fetchTableColumns(tableName);*/


            // PRIMARY KEY CONSTRAINT
            boolean isPrimaryKeyConstraintValid = checkPrimaryKeyConstraint(manager, retrievedColumns);
            if (!isPrimaryKeyConstraintValid) {
                return false;
            }
        }
        catch (InternalException e) {
            Utils.printMessage(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean validateColumnDataTypes(HashMap<String, Integer> columnDataTypeMapping) {
        if (!checkColumnDataTypeValidity(columnDataTypeMapping)) {
            return false;
        }

        return true;
    }

    private boolean checkColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (String tableColumn : columns) {
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
                break;
            }
        }

        if (!columnsValid) {
            Utils.printError("Invalid column '" + invalidColumn + "'");
            return false;
        }

        return true;
    }

    private boolean checkNullConstraint(StorageManager manager, List<String> retrievedColumnNames) throws InternalException {
        HashMap<String, Integer> columnsList = new HashMap<>();

        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                columnsList.put(columns.get(i), i);
            }
        }
        else {
            for (int i = 0; i < values.size(); i++) {
                columnsList.put(retrievedColumnNames.get(i), i);
            }
        }

        if (!manager.checkNullConstraint(this.databaseName, tableName, columnsList)) {
            return false;
        }

        return true;
    }

    private boolean checkPrimaryKeyConstraint(StorageManager manager, List<String> retrievedColumnNames) throws InternalException {

        String primaryKeyColumnName = manager.getTablePrimaryKey(databaseName, tableName);
        List<String> columnList = (columns != null) ? columns : retrievedColumnNames;

        if (primaryKeyColumnName.length() > 0) {
                if (columnList.contains(primaryKeyColumnName.toLowerCase())) {
                    // The primary key is present.
                    // Check if the same primary key with same value is present.
                    int primaryKeyIndex = columnList.indexOf(primaryKeyColumnName);
                    if (!manager.checkIfValueForPrimaryKeyExists(this.databaseName, tableName, Integer.parseInt(values.get(primaryKeyIndex).value))) {
                        // Primary key does not exist.
                    } else {
                        Utils.printError("Duplicate entry '" + values.get(primaryKeyIndex).value + "' for key 'PRIMARY'");
                        return false;
                    }
                }
        }

        return true;
    }

    private boolean checkColumnDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping) {
        String invalidColumn = "";

        for (String columnName : columns) {
            int dataTypeIndex = columnDataTypeMapping.get(columnName);
            int idx = columns.indexOf(columnName);
            Literal literal = values.get(idx);

            // Check if the data type is a integer type.
            if (dataTypeIndex != Constants.INVALID_CLASS && dataTypeIndex <= Constants.DOUBLE) {
                // The data is type of integer, real or double.

                boolean isValid = Utils.canConvertStringToDouble(literal.value);
                if (!isValid) {
                    invalidColumn = columnName;
                    break;
                }
            }
            else if (dataTypeIndex == Constants.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }
            else if (dataTypeIndex == Constants.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }
        }

        // Check the validity.
        boolean valid = (invalidColumn.length() > 0) ? false : true;

        if (!valid) {
            Utils.printError("Incorrect value for column '" + invalidColumn  + "' at row 1");
            return false;
        }

        return true;
    }

    public void generateRecords(List<Object> columnList, HashMap<String, Integer> columnDataTypeMapping, List<String> retrievedColumns) {
        for (int i=0; i < retrievedColumns.size(); i++) {
            String column = retrievedColumns.get(i);

            if (columns != null) {
                if (columns.contains(column)) {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();

                    int idx = columns.indexOf(column);

                    DT obj = getDataTypeObject(dataType);
                    String val = values.get(idx).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                } else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DT obj = getDataTypeObject(dataType);
                    obj.setNull(true);
                    columnList.add(obj);
                }
            }
            else {

                if (i < values.size()) {
                    Byte dataType = (byte) columnDataTypeMapping.get(column).intValue();

                    int columnIndex = retrievedColumns.indexOf(column);
                    DT obj = getDataTypeObject(dataType);
                    String val = values.get(columnIndex).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                }
                else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DT obj = getDataTypeObject(dataType);
                    obj.setNull(true);
                    columnList.add(obj);
                }
            }
        }
    }

    public DT getDataTypeObject(byte dataType) {

        switch (dataType) {
            case Constants.TINYINT: {
                DT_TinyInt obj = new DT_TinyInt();
                return obj;
            }
            case Constants.SMALLINT: {
                DT_SmallInt obj = new DT_SmallInt();
                return obj;
            }
            case Constants.INT: {
                DT_Int obj = new DT_Int();
                return obj;
            }
            case Constants.BIGINT: {
                DT_BigInt obj = new DT_BigInt();
                return obj;
            }
            case Constants.REAL: {
                DT_Real obj = new DT_Real();
                return obj;
            }
            case Constants.DOUBLE: {
                DT_Double obj = new DT_Double();
                return obj;
            }
            case Constants.DATE: {
                DT_Date obj = new DT_Date();
                return obj;

            }
            case Constants.DATETIME: {
                DT_DateTime obj = new DT_DateTime();
                return obj;

            }
            case Constants.TEXT: {
                DT_Text obj = new DT_Text();
                return obj;
            }
            default: {
                DT_Text obj = new DT_Text();
                return obj;
            }
        }
    }

    public Object getDataTypeValue(byte dataType, String value) {

        switch (dataType) {
            case Constants.TINYINT: {
                return Byte.parseByte(value);
            }
            case Constants.SMALLINT: {
                return Short.parseShort(value);
            }
            case Constants.INT: {
                return Integer.parseInt(value);
            }
            case Constants.BIGINT: {
                return Long.parseLong(value);
            }
            case Constants.REAL: {
                return Float.parseFloat(value);
            }
            case Constants.DOUBLE: {
                return Double.parseDouble(value);
            }
            case Constants.DATE: {
                return new Utils().getDateEpoc(value, true);
            }
            case Constants.DATETIME: {
                return new Utils().getDateEpoc(value, false);
            }
            case Constants.TEXT: {
                return value;
            }
            default: {
                return value;
            }
        }
    }


    private int findRowID (StorageManager manager, List<String> retrievedList) throws InternalException {
        int rowCount = manager.getTableRecordCount(this.databaseName, tableName);
        String primaryKeyColumnName = manager.getTablePrimaryKey(tableName, databaseName);
        if (primaryKeyColumnName.length() > 0) {
            // The primary key is present.
            // Check if the same primary key with same value is present.
            int primaryKeyIndex = (columns != null) ? columns.indexOf(primaryKeyColumnName) : retrievedList.indexOf(primaryKeyColumnName);
            int primaryKeyValue = Integer.parseInt(values.get(primaryKeyIndex).value);

            return primaryKeyValue;
        }
        else {
            return rowCount + 1;
        }
    }
}