package Model;

import common.Constants;
import common.Utils;
import datatypes.*;
import datatypes.base.DT;
import storage.StorageManager;
import storage.model.DataRecord;
import test.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class InsertQuery implements IQuery {
    public String tableName;
    public ArrayList<String> columns;
    public ArrayList<Literal> values;

    public InsertQuery(String tableName, ArrayList<String> columns, ArrayList<Literal> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
    }

    @Override
    public Result ExecuteQuery() {
        /*TODO : replace with actual logic*/
        Random random = new Random();
        Result result = new Result(random.nextInt(50));
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        /*TODO : replace with actual logic*/
        // validate if the table and the columns of the table.
        StorageManager manager = new StorageManager();
        if (!StorageManager.checkTableExists(Utils.getUserDatabasePath(Constants.DEFAULT_USER_DATABASE), tableName)) {
            Utils.printMessage("The table " + tableName + " does not exist.");
            return false;
        } else {
            // Table columns.
            List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
            HashMap<String, Byte> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);

            if (columns == null) {
                // No columns are provided.
                // Check values size.
                if (values.size() < retrievedColumns.size() || values.size() > retrievedColumns.size()) {
                    Utils.printMessage("Values overflow /underflow.");
                    return false;
                }

                // Check Columns datatype are valid.
                String invalidColumn = checkDataTypeValidity(columnDataTypeMapping, retrievedColumns);
                boolean valid = (invalidColumn.length() > 0) ? false : true;

                if (!valid) {
                    Utils.printMessage("The value of the column " + invalidColumn + " is invalid.");
                    return false;
                }
            }
            else  {
                // Columns are provided.
                // Validate columns.
                // If the column list is greater than the columns in the table then throw an error.
                if (columns.size() > retrievedColumns.size()) {
                    Utils.printMessage("Columns overflow.");
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
            Test test = new Test();
            test.fetchTableColumns(tableName);


            // PRIMARY KEY CONSTRAINT
            boolean isPrimaryKeyConstraintValid = checkPrimaryKeyConstraint(manager, retrievedColumns);
            if (!isPrimaryKeyConstraintValid) {
                return false;
            }

            // All checks are done. Now insert the values.
            DataRecord record  = new DataRecord();
            generateRecords(record.getColumnValueList(), columnDataTypeMapping, retrievedColumns);

            int rowID = findRowID(manager, retrievedColumns);
            record.setRowId(rowID);
            record.populateSize();

            boolean status = manager.writeRecord(Utils.getUserDatabasePath(Constants.DEFAULT_USER_DATABASE), tableName, record);
            if (status) {
                System.out.println("Record added successfully");
                return true;
            }
            else {
                System.out.println("Failed to add record added successfully");
                return false;
            }
        }
    }

    private boolean validateColumnDataTypes(HashMap<String, Byte> columnDataTypeMapping) {
        String invalidColumn = checkColumnDataTypeValidity(columnDataTypeMapping);
        boolean valid = (invalidColumn.length() > 0) ? false : true;

        if (!valid) {
            Utils.printMessage("The value of the column " + invalidColumn + " is invalid.");
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
            Utils.printMessage("The column " + invalidColumn + " is not present in the table " + tableName + ".");
            return false;
        }

        return true;
    }

    private boolean checkNullConstraint(StorageManager manager, List<String> retrievedColumnNames) {
        HashMap<String, Integer> columnsList = new HashMap<>();

        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                columnsList.put(columns.get(i), i);
            }
        }
        else {
            for (int i = 0; i < retrievedColumnNames.size(); i++) {
                columnsList.put(retrievedColumnNames.get(i), i);
            }
        }

        if (!manager.checkNullConstraint(tableName, columnsList)) {
            Utils.printMessage("Null constraint violated.");
            return false;
        }

        return true;
    }

    private boolean checkPrimaryKeyConstraint(StorageManager manager, List<String> retrievedColumnNames) {
        String primaryKeyColumnName = manager.getTablePrimaryKey(tableName);
        List<String> columnList = (columns != null) ? columns : retrievedColumnNames;

        if (primaryKeyColumnName.length() > 0) {
                if (columnList.contains(primaryKeyColumnName.toLowerCase())) {
                    // The primary key is present.
                    // Check if the same primary key with same value is present.
                    int primaryKeyIndex = columnList.indexOf(primaryKeyColumnName);
                    if (!manager.checkIfValueForPrimaryKeyExists(Constants.DEFAULT_USER_DATABASE, tableName, Integer.parseInt(values.get(primaryKeyIndex).value))) {
                        // Primary key does not exist.
                    } else {
                        Utils.printMessage("Primary key constraint violated. The value for column " + primaryKeyColumnName + " already exists.");
                        return false;
                    }
                }
        }

        return true;
    }

    private String checkColumnDataTypeValidity(HashMap<String, Byte> columnDataTypeMapping) {
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

        return invalidColumn;
    }

    private String checkDataTypeValidity(HashMap<String, Byte> columnDataTypeMapping, List<String> columnsList) {
        String invalidColumn = "";

        for (String columnName : columnsList) {
            int dataTypeIndex = columnDataTypeMapping.get(columnName);
            int idx = columnsList.indexOf(columnName);

            Literal literal = values.get(idx);

            // Check if the data type is a integer type.
            if (dataTypeIndex != Constants.INVALID_CLASS && dataTypeIndex <= Constants.DOUBLE) {
                // The data is type of integer, real or double.

                boolean isValid = Utils.canConvertStringToDouble(literal.value);
                if (!isValid) {
                    invalidColumn = columnName;
                    break;
                }
            } else if (dataTypeIndex == Constants.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            } else if (dataTypeIndex == Constants.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }
        }

        return invalidColumn;
    }

    public void generateRecords(List<Object> columnList, HashMap<String, Byte> columnDataTypeMapping, List<String> retrievedColumns) {
        for (String column : retrievedColumns) {
            if (columns != null) {
                if (columns.contains(column)) {
                    Byte dataType = columnDataTypeMapping.get(column);

                    int idx = columns.indexOf(column);

                    DT obj = getDataTypeObject(dataType);
                    String val = values.get(idx).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                } else {
                    Byte dataType = columnDataTypeMapping.get(column);
                    DT obj = getDataTypeObject(dataType);

                    //obj.setNull(true);
                    columnList.add(obj);
                }
            }
            else {
                Byte dataType = columnDataTypeMapping.get(column);

                int columnIndex = retrievedColumns.indexOf(column);
                DT obj = getDataTypeObject(dataType);
                String val = values.get(columnIndex).toString();

                obj.setValue(getDataTypeValue(dataType, val));
                columnList.add(obj);
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
                return getDateEpoc(value, true);
            }
            case Constants.DATETIME: {
                return getDateEpoc(value, false);
            }
            case Constants.TEXT: {
                return value;
            }
            default: {
                return value;
            }
        }
    }

    private long getDateEpoc(String value, Boolean isDate) {
        DateFormat formatter = null;
        if (isDate) {
          formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        formatter.setLenient(false);
        Date date = null;
        try {
            date = formatter.parse(value);

            /* Define the time zone for Dallas CST */
            ZoneId zoneId = ZoneId.of ( "America/Chicago" );

            /* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(),
                    ZoneId.systemDefault());

            /* ZonedDateTime toLocalDate() method will display in a simple format */
            System.out.println(zdt.toLocalDate());
            /* Convert a ZonedDateTime object to epochSeconds
            *  This value can be store 8-byte integer to a binary
            *  file using RandomAccessFile writeLong()
            */

            long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
            return epochSeconds;
        }
        catch (ParseException ex) {
            System.out.println("Exception "+ex);
            return 0;
        }
    }

    public int findRowID (StorageManager manager, List<String> retrievedList) {
        int rowCount = manager.getTableRecordCount(tableName);
        String primaryKeyColumnName = manager.getTablePrimaryKey(tableName);
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