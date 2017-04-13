import common.CatalogDB;
import common.Constants;
import common.Utils;
import parser.QueryParser;

import java.io.File;
import java.util.Scanner;

/**
 * Created by dakle on 30/3/17.
 */
public class DavisBase {

    static boolean isExit = false;

    /*
	 *  The Scanner class is used to collect user commands from the PROMPT
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand
	 *  String is re-populated.
	 */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String args[]) {
        initializeDatabase();
        QueryParser parser = new QueryParser();

        /* Display the welcome screen */
        splashScreen();

		/* Variable to collect user input from the PROMPT */
        String userCommand = "";

        while(!isExit) {
            System.out.print(Constants.PROMPT);
			/* toLowerCase() renders command case insensitive */
            userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
            // userCommand = userCommand.replace("\n", "").replace("\r", "");
            parser.parse(userCommand);
        }
        System.out.println("Exiting...");
    }

    /** ***********************************************************************
     *  Method definitions
     */

    /**
     *  Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(Utils.line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
        System.out.println("DavisBaseLite Version " + Utils.getVersion());
        System.out.println(Utils.getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(Utils.line("-",80));
    }

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

    }
}
