package common;

import Model.DataType;
import datatypes.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by dakle on 8/4/17.
 */
public class Utils {

    /** return the DakleBase VERSION */
    public static String getVersion() {
        return Constants.VERSION;
    }

    public static String getCopyright() {
        return Constants.COPYRIGHT;
    }

    public static void displayVersion() {
        System.out.println("DakleBaseLite Version " + getVersion());
        System.out.println(getCopyright());
    }

    public static String getSystemDatabasePath() {
        return Constants.DEFAULT_DATA_DIRNAME + "/" + Constants.DEFAULT_CATALOG_DATABASENAME;
    }

    public static String getUserDatabasePath(String database) {
        return Constants.DEFAULT_DATA_DIRNAME + "/" + database;
    }

    public static void printError(String errorMessage) {
        printMessage(errorMessage);
    }

    public static void printMissingDatabaseError(String databaseName) {
        printError("The database " + databaseName + " does not exist");
    }

    public static void printMissingDefaultDatabaseError() {
        printError("The database " + Constants.DEFAULT_USER_DATABASE + " does not exist");
    }

    public static void printMissingTableError(String tableName) {
        printError("The table " + tableName + " does not exist");
    }

    public static void printMessage(String str) {
        System.out.println(str);
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

    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    public static String line(String s, int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

}
