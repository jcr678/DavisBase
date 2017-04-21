package QueryParser;

import Model.*;
import common.Constants;
import common.Utils;
import queries.*;

import java.io.File;
import java.util.ArrayList;

public class DatabaseHelper {

    public static final String SELECT_COMMAND = "SELECT";
    public static final String DROP_TABLE_COMMAND = "DROP TABLE";
    public static final String DROP_DATABASE_COMMAND = "DROP DATABASE";
    public static final String HELP_COMMAND = "HELP";
    public static final String VERSION_COMMAND = "VERSION";
    public static final String EXIT_COMMAND = "EXIT";
    public static final String QUIT_COMMAND = "QUIT";
    public static final String SHOW_TABLES_COMMAND = "SHOW TABLES";
    public static final String SHOW_DATABASES_COMMAND = "SHOW DATABASES";
    public static final String INSERT_COMMAND = "INSERT INTO";
    public static final String DELETE_COMMAND = "DELETE FROM";
    public static final String UPDATE_COMMAND = "UPDATE";
    public static final String CREATE_TABLE_COMMAND = "CREATE TABLE";
    public static final String CREATE_DATABASE_COMMAND = "CREATE DATABASE";
    public static final String USE_DATABASE_COMMAND = "USE";
    public static final String DESC_TABLE_COMMAND = "DESC";
    public static final String DESCRIBE_TABLE_COMMAND = "DESCRIBE";
    private static final String NO_DATABASE_SELECTED_MESSAGE = "No database selected";
    public static final String USE_HELP_MESSAGE = "Please use 'HELP' to see a list of commands";

    public static String CurrentDatabaseName = "";
    public static String prompt = "davisql> ";
    private static String version = "v1.0b";
    private static String copyright = "©2017 Parag Pravin Dakle";

    public static IQuery ShowTableListQueryHandler() {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new ShowTableQuery(DatabaseHelper.CurrentDatabaseName);
    }

    public static IQuery DropTableQueryHandler(String tableName) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DropTableQuery(DatabaseHelper.CurrentDatabaseName, tableName);
    }

    public static void UnrecognisedCommand(String userCommand, String message) {
        System.out.println("Unrecognised Command " + userCommand);
        System.out.println("Message : " + message);
    }

    public static IQuery SelectQueryHandler(String[] attributes, String tableName, String conditionString) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        boolean isSelectAll = false;
        SelectQuery query;
        ArrayList<String> columns = new ArrayList<>();
        for(String attribute : attributes){
            columns.add(attribute.trim());
        }

        if(columns.size() == 1 && columns.get(0).equals("*")) {
            isSelectAll = true;
            columns = null;
        }

        if(conditionString.equals("")){
            query = new SelectQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, null, isSelectAll);
            return query;
        }

        Condition condition = Condition.CreateCondition(conditionString);
        if(condition == null) return null;

        ArrayList<Condition> conditionList = new ArrayList<>();
        conditionList.add(condition);
        query = new SelectQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, conditionList, isSelectAll);
        return query;
    }

    public static void ShowVersionQueryHandler() {
        System.out.println("DavisBaseLite Version " + getVersion());
        System.out.println(getCopyright());
    }

    private static String getVersion() {
        return version;
    }

    private static String getCopyright() {
        return copyright;
    }

    public static void HelpQueryHandler() {
        System.out.println(line("*",80));
        System.out.println("SUPPORTED COMMANDS");
        System.out.println("All commands below are case insensitive");
        System.out.println();
        System.out.println("\tUSE DATABASE database_name;                      Changes current database.");
        System.out.println("\tCREATE DATABASE database_name;                   Creates an empty database.");
        System.out.println("\tSHOW DATABASES;                                  Displays all databases.");
        System.out.println("\tDROP DATABASE database_name;                     Remove database.");
        System.out.println("\tSHOW TABLES;                                     Displays all tables in current database.");
        System.out.println("\tDESC|DESCRIBE table_name;                                 Displays table schema.");
        System.out.println("\tCREATE TABLE table_name (                        Creates a table in current database.");
        System.out.println("\t\t<column_name> <datatype> [PRIMARY KEY / NOT NULL]");
        System.out.println("\t\t...);");
        System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
        System.out.println("\tSELECT <column_list> FROM table_name             Display records whose rowid is <id>.");
        System.out.println("\t\t[WHERE rowid = <value>];");
        System.out.println("\tINSERT INTO table_name                           Inserts a record into the table.");
        System.out.println("\t\t[(<column1>, ...)] VALUES (<value1>, <value2>, ...);");
        System.out.println("\tDELETE FROM table_name [WHERE condition];        Deletes a record from a table.");
        System.out.println("\tUPDATE table_name SET <conditions>               Updates a record from a table.");
        System.out.println("\t\t[WHERE condition];");
        System.out.println("\tVERSION;                                         Show the program version.");
        System.out.println("\tHELP;                                            Show this help information");
        System.out.println("\tEXIT or QUIT;                                    Exit the program");
        System.out.println();
        System.out.println();
        System.out.println(line("*",80));
    }

    public static String line(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    public static IQuery InsertQueryHandler(String tableName, String columnsString, String valuesList) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;
        ArrayList<String> columns = null;
        ArrayList<Literal> values = new ArrayList<>();

        if(!columnsString.equals("")) {
            columns = new ArrayList<>();
            String[] columnList = columnsString.split(",");
            for(String column : columnList){
                columns.add(column.trim());
            }
        }

        for(String value : valuesList.split(",")){
            Literal literal = Literal.CreateLiteral(value.trim());
            if(literal == null) return null;
            values.add(literal);
        }

        if(columns != null && columns.size() != values.size()){
            DatabaseHelper.UnrecognisedCommand("", "Number of columns and values don't match");
            return null;
        }

        query = new InsertQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, values);
        return query;
    }

    public static IQuery DeleteQueryHandler(String tableName, String conditionString) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;

        if(conditionString.equals("")){
            query = new DeleteQuery(DatabaseHelper.CurrentDatabaseName, tableName, null);
            return query;
        }

        Condition condition = Condition.CreateCondition(conditionString);
        if(condition == null) return null;

        ArrayList<Condition> conditions = new ArrayList<>();
        conditions.add(condition);

        query = new DeleteQuery(DatabaseHelper.CurrentDatabaseName, tableName, conditions);
        return query;
    }

    public static IQuery UpdateQuery(String tableName, String clauseString, String conditionString) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;

        Condition clause = Condition.CreateCondition(clauseString);
        if(clause == null) return null;

        if(clause.operator != Operator.EQUALS){
            DatabaseHelper.UnrecognisedCommand(clauseString, "SET clause should only contain = operator");
            return null;
        }

        if(conditionString.equals("")){
            query = new UpdateQuery(DatabaseHelper.CurrentDatabaseName, tableName, clause.column, clause.value, null);
            return query;
        }

        Condition condition = Condition.CreateCondition(conditionString);
        if(condition == null) return null;

        query = new UpdateQuery(DatabaseHelper.CurrentDatabaseName, tableName, clause.column, clause.value, condition);
        return query;
    }

    public static IQuery CreateTableQueryHandler(String tableName, String columnsPart) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;
        boolean hasPrimaryKey = false;
        ArrayList<Column> columns = new ArrayList<>();
        String[] columnsList = columnsPart.split(",");

        for(String columnEntry : columnsList){
            Column column = Column.CreateColumn(columnEntry.trim());
            if(column == null) return null;
            columns.add(column);
        }

        for (int i = 0; i < columnsList.length; i++) {
            if (columnsList[i].toLowerCase().endsWith("primary key")) {
                if (i == 0) {
                    if (columns.get(i).type == DataType.INT) {
                        hasPrimaryKey = true;
                    } else {
                        DatabaseHelper.UnrecognisedCommand(columnsList[i], "PRIMARY KEY has to have INT datatype");
                        return null;
                    }
                }
                else {
                    DatabaseHelper.UnrecognisedCommand(columnsList[i], "Only first column should be PRIMARY KEY and has to have INT datatype.");
                    return null;
                }

            }
        }

        query = new CreateTableQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, hasPrimaryKey);
        return query;
    }

    public static IQuery DropDatabaseQueryHandler(String databaseName) {
        return new DropDatabaseQuery(databaseName);
    }

    public static IQuery ShowDatabaseListQueryHandler() {
        return new ShowDatabaseQuery();
    }

    public static IQuery UseDatabaseQueryHandler(String databaseName) {
        return new UseDatabaseQuery(databaseName);
    }

    public static IQuery CreateDatabaseQueryHandler(String databaseName) {
        return new CreateDatabaseQuery(databaseName);
    }

    public static boolean IsDatabaseExists(String databaseName) {

        if (databaseName == null || databaseName.length() == 0) {
            DatabaseHelper.UnrecognisedCommand("", USE_HELP_MESSAGE);
            return false;
        }

        File dirFile = new File(Utils.getDatabasePath(databaseName));
        return dirFile.exists();
    }

    public static boolean isTableExists(String databaseName, String tableName) {
        if (tableName == null || databaseName == null || tableName.length() == 0 || databaseName.length() == 0) {
            DatabaseHelper.UnrecognisedCommand("", USE_HELP_MESSAGE);
            return false;
        }

        File tableFile = new File(String.format("%s/%s/%s%s", Constants.DEFAULT_DATA_DIRNAME, databaseName, tableName, Constants.DEFAULT_FILE_EXTENSION));
        return tableFile.exists();
    }

    public static void ExecuteQuery(IQuery query) {
        if(query!= null && query.ValidateQuery()){
            Result result = query.ExecuteQuery();
            if(result != null){
                result.Display();
            }
        }
    }

    public static IQuery DescTableQueryHandler(String tableName) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DescTableQuery(DatabaseHelper.CurrentDatabaseName, tableName);
    }
}
