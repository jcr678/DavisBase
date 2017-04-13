package common;

/**
 * Created by dakle on 8/4/17.
 */
public interface Constants {

    /* This can be changed to whatever you like */
    String PROMPT = "daklesql> ";
    String VERSION = "v1.0b(example)";
    String COPYRIGHT = "©2016 Parag Pravin Dakle";

    String DEFAULT_FILE_EXTENSION = ".tbl";
    String DEFAULT_DATA_DIRNAME = "data";
    String DEFAULT_CATALOG_DATABASENAME = "catalog";
    String DEFAULT_USER_DATABASENAME = "user_data";
    String SYSTEM_TABLES_TABLENAME = "davisbase_tables";
    String SYSTEM_COLUMNS_TABLENAME = "davisbase_columns";

    //DataType Class Constants
    byte INVALID_CLASS = -1;
    byte TINYINT = 0;
    byte SMALLINT = 1;
    byte INT = 2;
    byte BIGINT = 3;
    byte REAL = 4;
    byte DOUBLE = 5;
    byte DATE = 6;
    byte DATETIME = 7;
    byte TEXT = 8;
}
