package errors;

/**
 * Created by dakle on 17/4/17.
 */
public class DatabaseException extends Exception {

    public static String BASE_ERROR_STRING = "ERROR(100): ";
    public static String UNKNOWN_DATABASE_EXCEPTION = BASE_ERROR_STRING + "Unknown database '%1'";
    public static String DATABASE_NOT_FOUND_EXCEPTION = BASE_ERROR_STRING + "Can't drop database '%1'; database doesn't exist";
    public static String DATABASE_EXISTS_EXCEPTION = BASE_ERROR_STRING + "Can't create database '%1'; database exists";
    public static String GENERIC_EXCEPTION = BASE_ERROR_STRING + " Can't perform database operation. %1";

    public DatabaseException(String message, String databaseName) {
        super(message.replace("%1", databaseName));
    }

}
