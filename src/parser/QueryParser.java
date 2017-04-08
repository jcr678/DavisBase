package parser;


import common.Utils;
import console.ConsoleWriter;
import org.json.simple.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dakle on 30/3/17.
 */
public class QueryParser implements StatementKeywords{

    public JSONObject parse(String query) {
        if(basicValidation(query)) {
            Pattern firstWordPattern = Pattern.compile("(\\w)+");
            Matcher firstWordMatcher = firstWordPattern.matcher(query);
            firstWordMatcher.find();
            String keyword = firstWordMatcher.group(0);
            JSONObject jsonObject = null;
            switch (keyword.toLowerCase()) {
                case CREATE_STATEMENT:
                    CreateStatement statement = new CreateStatement();
                    return statement.createObject(query);

                case SHOW_STATEMENT:
                    break;

                case DROP_STATEMENT:
                    break;

                case INSERT_STATEMENT:
                    break;

                case UPDATE_STATEMENT:
                    break;

                case DELETE_STATEMENT:
                    break;

                case SELECT_STATEMENT:
                    break;

                case EXIT_STATEMENT:
                    break;

                case HELP_STATEMENT:
                    showHelp();
                    break;

                default:
                    break;
            }
            return jsonObject;
        }
        return null;
    }

    private boolean basicValidation(String query) {
        Pattern pattern = Pattern.compile(RegexExpressions.STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        boolean isMatch = matcher.matches();
        if(!isMatch) ConsoleWriter.displayMessage("Invalid Command");
        return isMatch;
    }

    /**
     *  Help: Display supported commands
     */
    private void showHelp() {
        System.out.println(Utils.line("*",80));
        System.out.println("SUPPORTED COMMANDS");
        System.out.println("All commands below are case insensitive");
        System.out.println();
        System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
        System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
        System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
        System.out.println("\tVERSION;                                         Show the program VERSION.");
        System.out.println("\tHELP;                                            Show this help information");
        System.out.println("\tEXIT;                                            Exit the program");
        System.out.println();
        System.out.println();
        System.out.println(Utils.line("*",80));
    }
}
