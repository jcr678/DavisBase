package common;

import Model.Condition;
import Model.DataType;
import Model.Literal;
import Model.Operator;
import datatypes.*;
import datatypes.base.DT_Numeric;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Mahesh on 8/4/17.
 */
public class Utils {

    public static String getDatabasePath(String databaseName) {
        return Constants.DEFAULT_DATA_DIRNAME + "/" + databaseName;
    }

    public static void printError(String errorMessage) {
        printMessage(errorMessage);
    }

    public static void printMissingDatabaseError(String databaseName) {
        printError("The database '" + databaseName + "' does not exist");
    }

    public static void printMissingTableError(String database, String tableName) {
        printError("Table '" + database + "." + tableName + "' doesn't exist.");
    }

    public static void printDuplicateTableError(String database, String tableName) {
        printError("Table '" + database + "." + tableName + "' already exist.");
    }

    public static void printMessage(String str) {
        System.out.println(str);
    }

    public static void printUnknownColumnValueError(String columnName, String value) {
        printMessage(String.format("Incorrect value: '%s' for column '%s'", value, columnName));
    }

    public static byte resolveClass(Object object) {
        if(object.getClass().equals(DT_TinyInt.class)) {
            return Constants.TINYINT;
        }
        else if(object.getClass().equals(DT_SmallInt.class)) {
            return Constants.SMALLINT;
        }
        else if(object.getClass().equals(DT_Int.class)) {
            return Constants.INT;
        }
        else if(object.getClass().equals(DT_BigInt.class)) {
            return Constants.BIGINT;
        }
        else if(object.getClass().equals(DT_Real.class)) {
            return Constants.REAL;
        }
        else if(object.getClass().equals(DT_Double.class)) {
            return Constants.DOUBLE;
        }
        else if(object.getClass().equals(DT_Date.class)) {
            return Constants.DATE;
        }
        else if(object.getClass().equals(DT_DateTime.class)) {
            return Constants.DATETIME;
        }
        else if(object.getClass().equals(DT_Text.class)) {
            return Constants.TEXT;
        }
        else {
            return Constants.INVALID_CLASS;
        }
    }

    public static byte stringToDataType(String string) {
        if(string.compareToIgnoreCase("TINYINT") == 0) {
            return Constants.TINYINT;
        }
        else if(string.compareToIgnoreCase("SMALLINT") == 0) {
            return Constants.SMALLINT;
        }
        else if(string.compareToIgnoreCase("INT") == 0) {
            return Constants.INT;
        }
        else if(string.compareToIgnoreCase("BIGINT") == 0) {
            return Constants.BIGINT;
        }
        else if(string.compareToIgnoreCase("REAL") == 0) {
            return Constants.REAL;
        }
        else if(string.compareToIgnoreCase("DOUBLE") == 0) {
            return Constants.DOUBLE;
        }
        else if(string.compareToIgnoreCase("DATE") == 0) {
            return Constants.DATE;
        }
        else if(string.compareToIgnoreCase("DATETIME") == 0) {
            return Constants.DATETIME;
        }
        else if(string.compareToIgnoreCase("TEXT") == 0) {
            return Constants.TEXT;
        }
        else {
            return Constants.INVALID_CLASS;
        }
    }

    public static DataType internalDataTypeToModelDataType(byte type) {
        switch (type) {
            case Constants.TINYINT:
                return DataType.TINYINT;
            case Constants.SMALLINT:
                return DataType.SMALLINT;
            case Constants.INT:
                return DataType.INT;
            case Constants.BIGINT:
                return DataType.BIGINT;
            case Constants.REAL:
                return DataType.REAL;
            case Constants.DOUBLE:
                return DataType.DOUBLE;
            case Constants.DATE:
                return DataType.DATE;
            case Constants.DATETIME:
                return DataType.DATETIME;
            case Constants.TEXT:
                return DataType.TEXT;
            default:
                return null;
        }
    }

    public static boolean canConvertStringToDouble(String value) {
        try {
            Double dVal = Double.parseDouble(value);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public static boolean isvalidDateFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setLenient(false);
        try {
            Date dateObj = formatter.parse(date);
        } catch (ParseException e) {
            //If input date is in different format or invalid.
            return false;
        }

        return true;
    }

    public static boolean isvalidDateTimeFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setLenient(false);
        try {
            Date dateObj = formatter.parse(date);
        } catch (ParseException e) {
            //If input date is in different format or invalid.
            return false;
        }

        return true;
    }

    public static Short ConvertFromOperator(Operator operator) {
        switch (operator){
            case EQUALS: return DT_Numeric.EQUALS;
            case GREATER_THAN_EQUAL: return DT_Numeric.GREATER_THAN_EQUALS;
            case GREATER_THAN: return DT_Numeric.GREATER_THAN;
            case LESS_THAN_EQUAL: return DT_Numeric.LESS_THAN_EQUALS;
            case LESS_THAN: return DT_Numeric.LESS_THAN;
        }

        return null;
    }

    public static boolean checkConditionValueDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, Condition condition) {
        String invalidColumn = "";
        Literal literal = null;

        if (columnsList.contains(condition.column)) {
            int dataTypeIndex = columnDataTypeMapping.get(condition.column);
            literal = condition.value;

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

        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printUnknownColumnValueError(invalidColumn, literal.value);
        }

        return valid;
    }

    public static long getDateEpoc(String value, Boolean isDate) {
        DateFormat formatter = null;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        formatter.setLenient(false);
        Date date;
        try {
            date = formatter.parse(value);

            /* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(),
                    ZoneId.systemDefault());

            /* ZonedDateTime toLocalDate() method will display in a simple format */
            //System.out.println(zdt.toLocalDate());
            /* Convert a ZonedDateTime object to epochSeconds
            *  This value can be store 8-byte integer to a binary
            *  file using RandomAccessFile writeLong()
            */

            return zdt.toInstant().toEpochMilli() / 1000;
        }
        catch (ParseException ex) {
            return 0;
        }
    }

    public static String getDateEpocAsString(long value, Boolean isDate) {
        /* Define the time zone for Dallas CST */
        ZoneId zoneId = ZoneId.of ( "America/Chicago" );

        Instant i = Instant.ofEpochSecond (value);
        ZonedDateTime zdt2 = ZonedDateTime.ofInstant (i, zoneId);
        Date date = Date.from(zdt2.toInstant());

        DateFormat formatter = null;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        formatter.setLenient(false);

        String dateStr = formatter.format(date);
        return dateStr;
    }

    public boolean checkDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, List<Literal> values) {
        String invalidColumn = "";
        Literal invalidLiteral = null;


        for (int i =0; i < values.size(); i++) {
            String columnName = columnsList.get(i);

            // Get the data type for the column with name 'columnName'.
            // Retrieve literal for the corresponding column from the user input.
            int dataTypeId = columnDataTypeMapping.get(columnName);

            // Retrieve the user input.
            int idx = columnsList.indexOf(columnName);
            Literal literal = values.get(idx);
            invalidLiteral = literal;

            // Check if the data type is a integer type.
            // If the data type any of the Integer's, Real's or Doubles, then these values, can be represented as a double.
            // Check if the value can be parsed as a Double, if YES then the data type is valid else returns false.
            if (literal.type != Utils.internalDataTypeToModelDataType((byte)dataTypeId)) {
                invalidColumn = columnName;
                break;
            }

            // NOTE: If the data type is of type text, any text is accepted, hence no check is explicitly added for the TEXT field.
        }

        // Check if any data type violation has occurred.
        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printUnknownColumnValueError(invalidColumn, invalidLiteral.value);
            return false;
        }

        return true;
    }

    public static boolean RecursivelyDelete(File file){
        if(file == null) return true;
        boolean isDeleted = false;

        if(file.isDirectory()) {
            for (File childFile : file.listFiles()) {
                if (childFile.isFile()) {
                    isDeleted = childFile.delete();
                    if (!isDeleted) return false;
                } else {
                    isDeleted = RecursivelyDelete(childFile);
                    if (!isDeleted) return false;
                }
            }
        }

        return file.delete();
    }
}
