package common;

/**
 * Created by Mahesh on 8/4/17.
 */
public interface Constants {

    /* This can be changed to whatever you like */
    String PROMPT = "daklesql> ";
    String VERSION = "v1.0b(example)";
    String COPYRIGHT = "©2016 Parag Pravin Dakle";

    String DEFAULT_FILE_EXTENSION = ".tbl";
    String DEFAULT_DATA_DIRNAME = "data";
    String DEFAULT_CATALOG_DATABASENAME = "catalog";
    String SYSTEM_TABLES_TABLENAME = "davisbase_tables";
    String SYSTEM_COLUMNS_TABLENAME = "davisbase_columns";

    String PRIMARY_KEY_PRESENT = "PRI";
    String CONSTRAINT_ABSENT = "NO";

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

    //Serial Code Constants
    byte ONE_BYTE_NULL_SERIAL_TYPE_CODE = 0x00;
    byte TWO_BYTE_NULL_SERIAL_TYPE_CODE = 0x01;
    byte FOUR_BYTE_NULL_SERIAL_TYPE_CODE = 0x02;
    byte EIGHT_BYTE_NULL_SERIAL_TYPE_CODE = 0x03;
    byte TINY_INT_SERIAL_TYPE_CODE = 0x04;
    byte SMALL_INT_SERIAL_TYPE_CODE = 0x05;
    byte INT_SERIAL_TYPE_CODE = 0x06;
    byte BIG_INT_SERIAL_TYPE_CODE = 0x07;
    byte REAL_SERIAL_TYPE_CODE = 0x08;
    byte DOUBLE_SERIAL_TYPE_CODE = 0x09;
    byte DATE_TIME_SERIAL_TYPE_CODE = 0x0A;
    byte DATE_SERIAL_TYPE_CODE = 0x0B;
    byte TEXT_SERIAL_TYPE_CODE = 0x0C;
}
