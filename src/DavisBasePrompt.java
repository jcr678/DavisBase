import query.base.IQuery;
import query.parser.QueryParser;
import common.SystemDatabaseHelper;
import common.Constants;

import java.io.File;
import java.util.Scanner;

/**
 * Created by Mahesh on 15/4/17.
 */

public class DavisBasePrompt {

  private static boolean isExit = false;
  private static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {

        InitializeDatabase();
		splashScreen();

        while(!isExit) {
            System.out.print(QueryParser.prompt);
            String userCommand = scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
    }

    private static void splashScreen() {
        System.out.println(QueryParser.line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
        QueryParser.ShowVersionQueryHandler();
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(QueryParser.line("-",80));
    }

    private static void parseUserCommand (String userCommand) {
        if(userCommand.toLowerCase().equals(QueryParser.SHOW_TABLES_COMMAND.toLowerCase())){
            IQuery query = QueryParser.ShowTableListQueryHandler();
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(QueryParser.SHOW_DATABASES_COMMAND.toLowerCase())){
            IQuery query = QueryParser.ShowDatabaseListQueryHandler();
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(QueryParser.HELP_COMMAND.toLowerCase())){
            QueryParser.HelpQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(QueryParser.VERSION_COMMAND.toLowerCase())){
            QueryParser.ShowVersionQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(QueryParser.EXIT_COMMAND.toLowerCase()) ||
                userCommand.toLowerCase().equals(QueryParser.QUIT_COMMAND.toLowerCase())){

            System.out.println("Exiting Database...");
            isExit = true;
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.USE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.USE_DATABASE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(QueryParser.USE_DATABASE_COMMAND.length());
            IQuery query = QueryParser.UseDatabaseQueryHandler(databaseName.trim());
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.DESC_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.DESC_TABLE_COMMAND) && !PartsEqual(userCommand, QueryParser.DESCRIBE_TABLE_COMMAND)) {
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String tableName;
            if(userCommand.toLowerCase().startsWith(QueryParser.DESCRIBE_TABLE_COMMAND.toLowerCase()))
                tableName = userCommand.substring(QueryParser.DESCRIBE_TABLE_COMMAND.length());
            else
                tableName = userCommand.substring(QueryParser.DESC_TABLE_COMMAND.length());
            IQuery query = QueryParser.DescTableQueryHandler(tableName.trim());
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.DROP_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.DROP_TABLE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String tableName = userCommand.substring(QueryParser.DROP_TABLE_COMMAND.length());
            IQuery query = QueryParser.DropTableQueryHandler(tableName.trim());
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.DROP_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.DROP_DATABASE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(QueryParser.DROP_DATABASE_COMMAND.length());
            IQuery query = QueryParser.DropDatabaseQueryHandler(databaseName.trim());
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.SELECT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.SELECT_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            int index = userCommand.toLowerCase().indexOf("from");
            if(index == -1) {
                QueryParser.UnrecognisedCommand(userCommand, "Expected FROM keyword");
                return;
            }

            String attributeList = userCommand.substring(QueryParser.SELECT_COMMAND.length(), index).trim();
            String restUserQuery = userCommand.substring(index + "from".length());

            index = restUserQuery.toLowerCase().indexOf("where");
            if(index == -1) {
                String tableName = restUserQuery.trim();
                IQuery query = QueryParser.SelectQueryHandler(attributeList.split(","), tableName, "");
                QueryParser.ExecuteQuery(query);
                return;
            }

            String tableName = restUserQuery.substring(0, index);
            String conditions = restUserQuery.substring(index + "where".length());
            IQuery query = QueryParser.SelectQueryHandler(attributeList.split(","), tableName.trim(), conditions);
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.INSERT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.INSERT_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String columns = "";

            int valuesIndex = userCommand.toLowerCase().indexOf("values");
            if(valuesIndex == -1) {
                QueryParser.UnrecognisedCommand(userCommand, "Expected VALUES keyword");
                return;
            }

            String columnOptions = userCommand.toLowerCase().substring(0, valuesIndex);
            int openBracketIndex = columnOptions.indexOf("(");

            if(openBracketIndex != -1) {
                tableName = userCommand.substring(QueryParser.INSERT_COMMAND.length(), openBracketIndex).trim();
                int closeBracketIndex = userCommand.indexOf(")");
                if(closeBracketIndex == -1) {
                    QueryParser.UnrecognisedCommand(userCommand, "Expected ')'");
                    return;
                }

                columns = userCommand.substring(openBracketIndex + 1, closeBracketIndex).trim();
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(QueryParser.INSERT_COMMAND.length(), valuesIndex).trim();
            }

            String valuesList = userCommand.substring(valuesIndex + "values".length()).trim();
            if(!valuesList.startsWith("(")){
                QueryParser.UnrecognisedCommand(userCommand, "Expected '('");
                return;
            }

            if(!valuesList.endsWith(")")){
                QueryParser.UnrecognisedCommand(userCommand, "Expected ')'");
                return;
            }

            valuesList = valuesList.substring(1, valuesList.length()-1);
            IQuery query = QueryParser.InsertQueryHandler(tableName, columns, valuesList);
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.DELETE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.DELETE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String condition = "";
            int index = userCommand.toLowerCase().indexOf("where");
            if(index == -1) {
                tableName = userCommand.substring(QueryParser.DELETE_COMMAND.length()).trim();
                IQuery query = QueryParser.DeleteQueryHandler(tableName, condition);
                QueryParser.ExecuteQuery(query);
                return;
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(QueryParser.DELETE_COMMAND.length(), index).trim();
            }

            condition = userCommand.substring(index + "where".length());
            IQuery query = QueryParser.DeleteQueryHandler(tableName, condition);
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.UPDATE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.UPDATE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String conditions = "";
            int setIndex = userCommand.toLowerCase().indexOf("set");
            if(setIndex == -1) {
                QueryParser.UnrecognisedCommand(userCommand, "Expected SET keyword");
                return;
            }

            String tableName = userCommand.substring(QueryParser.UPDATE_COMMAND.length(), setIndex).trim();
            String clauses = userCommand.substring(setIndex + "set".length());
            int whereIndex = userCommand.toLowerCase().indexOf("where");
            if(whereIndex == -1){
                IQuery query = QueryParser.UpdateQuery(tableName, clauses, conditions);
                QueryParser.ExecuteQuery(query);
                return;
            }

            clauses = userCommand.substring(setIndex + "set".length(), whereIndex).trim();
            conditions = userCommand.substring(whereIndex + "where".length());
            IQuery query = QueryParser.UpdateQuery(tableName, clauses, conditions);
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.CREATE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.CREATE_DATABASE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(QueryParser.CREATE_DATABASE_COMMAND.length());
            IQuery query = QueryParser.CreateDatabaseQueryHandler(databaseName.trim());
            QueryParser.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryParser.CREATE_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryParser.CREATE_TABLE_COMMAND)){
                QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
                return;
            }

            int openBracketIndex = userCommand.toLowerCase().indexOf("(");
            if(openBracketIndex == -1) {
                QueryParser.UnrecognisedCommand(userCommand, "Expected (");
                return;
            }

            if(!userCommand.endsWith(")")){
                QueryParser.UnrecognisedCommand(userCommand, "Missing )");
                return;
            }

            String tableName = userCommand.substring(QueryParser.CREATE_TABLE_COMMAND.length(), openBracketIndex).trim();
            String columnsPart = userCommand.substring(openBracketIndex + 1, userCommand.length()-1);
            IQuery query = QueryParser.CreateTableQueryHandler(tableName, columnsPart);
            QueryParser.ExecuteQuery(query);
        }
        else{
            QueryParser.UnrecognisedCommand(userCommand, QueryParser.USE_HELP_MESSAGE);
        }
    }

    private static boolean PartsEqual(String userCommand, String expectedCommand) {
        String[] userParts = userCommand.toLowerCase().split(" ");
        String[] actualParts = expectedCommand.toLowerCase().split(" ");

        for(int i=0;i<actualParts.length;i++){
            if(!actualParts[i].equals(userParts[i])){
                return false;
            }
        }

        return true;
    }

    private static void InitializeDatabase() {
        File baseDir = new File(Constants.DEFAULT_DATA_DIRNAME);
        if(!baseDir.exists()) {
            File catalogDir = new File(Constants.DEFAULT_DATA_DIRNAME + "/" + Constants.DEFAULT_CATALOG_DATABASENAME);
            if(!catalogDir.exists()) {
                if(catalogDir.mkdirs()) {
                    new SystemDatabaseHelper().createCatalogDB();
                }
            }
        }

    }
}