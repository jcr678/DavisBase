import Model.IQuery;
import Model.Result;
import QueryParser.DatabaseHelper;
import common.CatalogDB;
import common.Constants;
import storage.StorageManager;

import java.io.File;
import java.util.Scanner;

public class UserPrompt {

    static boolean isExit = false;
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {

		splashScreen();
        String userCommand = "";

        // Initialize the database.
        initializeDatabase();
        //new Test().run(1);

        while(!isExit) {
            System.out.print(DatabaseHelper.prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
    }

    public static void splashScreen() {
        System.out.println(DatabaseHelper.line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
        System.out.println("DavisBaseLite Version ");
        DatabaseHelper.ShowVersionQueryHandler();
        System.out.println(DatabaseHelper.getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(DatabaseHelper.line("-",80));
    }

    public static void parseUserCommand (String userCommand) {
		
		if(userCommand.toLowerCase().equals(DatabaseHelper.SHOW_TABLES_COMMAND.toLowerCase())){
		    IQuery query = DatabaseHelper.ShowTableListQueryHandler();
		    ExecuteQuery(query);
		    return;
        }
        else if(userCommand.toLowerCase().equals(DatabaseHelper.HELP_COMMAND.toLowerCase())){
            DatabaseHelper.HelpQueryHandler();
            return;
        }
        else if(userCommand.toLowerCase().equals(DatabaseHelper.VERSION_COMMAND.toLowerCase())){
            DatabaseHelper.ShowVersionQueryHandler();
            return;
        }
        else if(userCommand.toLowerCase().equals(DatabaseHelper.EXIT_COMMAND.toLowerCase()) ||
                userCommand.toLowerCase().equals(DatabaseHelper.QUIT_COMMAND.toLowerCase())){
            System.out.println("Exiting Database...");
            isExit = true;
            return;
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.DROP_COMMAND.toLowerCase())){
            String tableName = userCommand.substring(DatabaseHelper.DROP_COMMAND.length());
            IQuery query = DatabaseHelper.DropTableQueryHandler(tableName.trim());
            ExecuteQuery(query);
            return;
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.SELECT_COMMAND.toLowerCase())){
            int index = userCommand.toLowerCase().indexOf("from");
            if(index == -1) {
                DatabaseHelper.UnrecognisedCommand(userCommand, "Expected FROM keyword");
                return;
            }

            String attributeList = userCommand.substring(DatabaseHelper.SELECT_COMMAND.length(), index).trim();
            String restUserQuery = userCommand.substring(index + "from".length());

            index = restUserQuery.toLowerCase().indexOf("where");
            if(index == -1) {
                String tableName = restUserQuery.trim();
                IQuery query = DatabaseHelper.SelectQueryHandler(attributeList.split(","), tableName, "");
                ExecuteQuery(query);
                return;
            }

            String tableName = restUserQuery.substring(0, index);
            String conditions = restUserQuery.substring(index + "where".length());
            IQuery query = DatabaseHelper.SelectQueryHandler(attributeList.split(","), tableName.trim(), conditions);
            ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.INSERT_COMMAND.toLowerCase())){
            String tableName = "";
            String columns = "";

            int valuesIndex = userCommand.toLowerCase().indexOf("values");
            if(valuesIndex == -1) {
                DatabaseHelper.UnrecognisedCommand(userCommand, "Expected VALUES keyword");
                return;
            }

            String columnOptions = userCommand.toLowerCase().substring(0, valuesIndex);
            int openBracketIndex = columnOptions.indexOf("(");

            if(openBracketIndex != -1) {
                tableName = userCommand.substring(DatabaseHelper.INSERT_COMMAND.length(), openBracketIndex).trim();
                int closeBracketIndex = userCommand.indexOf(")");
                if(closeBracketIndex == -1) {
                    DatabaseHelper.UnrecognisedCommand(userCommand, "Expected ')'");
                    return;
                }

                columns = userCommand.substring(openBracketIndex + 1, closeBracketIndex).trim();
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(DatabaseHelper.INSERT_COMMAND.length(), valuesIndex).trim();
            }

            String valuesList = userCommand.substring(valuesIndex + "values".length()).trim();
            if(!valuesList.startsWith("(")){
                DatabaseHelper.UnrecognisedCommand(userCommand, "Expected '('");
                return;
            }

            if(!valuesList.endsWith(")")){
                DatabaseHelper.UnrecognisedCommand(userCommand, "Expected ')'");
                return;
            }

            valuesList = valuesList.substring(1, valuesList.length()-1);
            IQuery query = DatabaseHelper.InsertQueryHandler(tableName, columns, valuesList);
            ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.DELETE_COMMAND.toLowerCase())){
            String tableName = "";
            String condition = "";
            int index = userCommand.toLowerCase().indexOf("where");
            if(index == -1) {
                tableName = userCommand.substring(DatabaseHelper.DELETE_COMMAND.length()).trim();
                IQuery query = DatabaseHelper.DeleteQueryHandler(tableName, condition);
                ExecuteQuery(query);
                return;
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(DatabaseHelper.DELETE_COMMAND.length(), index).trim();
            }

            condition = userCommand.substring(index + "where".length());
            IQuery query = DatabaseHelper.DeleteQueryHandler(tableName, condition);
            ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.UPDATE_COMMAND.toLowerCase())){
            String tableName = "";
            String conditions = "";
            int setIndex = userCommand.toLowerCase().indexOf("set");
            if(setIndex == -1) {
                DatabaseHelper.UnrecognisedCommand(userCommand, "Expected SET keyword");
                return;
            }

            tableName = userCommand.substring(DatabaseHelper.UPDATE_COMMAND.length(), setIndex).trim();
            String clauses = userCommand.substring(setIndex + "set".length());
            int whereIndex = userCommand.toLowerCase().indexOf("where");
            if(whereIndex == -1){
                IQuery query = DatabaseHelper.UpdateQuery(tableName, clauses, conditions);
                ExecuteQuery(query);
                return;
            }

            clauses = userCommand.substring(setIndex + "set".length(), whereIndex).trim();
            conditions = userCommand.substring(whereIndex + "where".length());
            IQuery query = DatabaseHelper.UpdateQuery(tableName, clauses, conditions);
            ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.CREATE_TABLE_COMMAND.toLowerCase())){
            String tableName = "";
            IQuery query = null;

            int openBracketIndex = userCommand.toLowerCase().indexOf("(");
            if(openBracketIndex == -1) {
                QueryParser.DatabaseHelper.UnrecognisedCommand(userCommand, "Expected (");
                return;
            }

            if(!userCommand.endsWith(")")){
                QueryParser.DatabaseHelper.UnrecognisedCommand(userCommand, "Missing )");
                return;
            }

            tableName = userCommand.substring(DatabaseHelper.CREATE_TABLE_COMMAND.length(), openBracketIndex).trim();
            String columnsPart = userCommand.substring(openBracketIndex + 1, userCommand.length()-1);

            query = DatabaseHelper.CreateTableQueryHandler(tableName, columnsPart);
            ExecuteQuery(query);
            return;
        }
        else{
            DatabaseHelper.UnrecognisedCommand(userCommand, "Please use 'HELP' to see a list of commands");
        }
    }

    private static void ExecuteQuery(IQuery query) {
        if(query!= null && query.ValidateQuery()){
            Result result = query.ExecuteQuery();
            result.Display();
        }
    }

    // Initialize the database.
    public static void initializeDatabase() {
        File baseDir = new File(Constants.DEFAULT_DATA_DIRNAME);
        if(baseDir.exists()) {
            File catalogDir = new File(Constants.DEFAULT_DATA_DIRNAME + "/" + Constants.DEFAULT_CATALOG_DATABASENAME);
            if(!catalogDir.exists()) {
                if(catalogDir.mkdir()) {
                    new CatalogDB().createCatalogDB();
                }
            }
        }

        // Check if the default database exists.
        if (StorageManager.defaultDatabaseExists()) {
            // Default database exist.
            //System.out.println("Database already created.");

        }
        else  {
            StorageManager manager = new StorageManager();
            boolean create = manager.createDatabase(Constants.DEFAULT_USER_DATABASE);
            if (create) {
                //System.out.println("Created new database.");
            }
            else {
                //System.out.println("Failed to create new database.");
            }
        }

    }
}