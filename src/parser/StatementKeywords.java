package parser;

/**
 * Created by dakle on 30/3/17.
 */
public interface StatementKeywords {

    //DDL Keywords and Codes
    String CREATE_STATEMENT = "create";
    String SHOW_STATEMENT = "show";
    String DROP_STATEMENT = "drop";
    String TABLE_STATEMENT = "table";
    int CREATE_STATEMENT_CODE = 0;
    int SHOW_STATEMENT_CODE = 1;
    int DROP_STATEMENT_CODE = 2;
    int TABLE_STATEMENT_CODE = 3;

    //DML Keywords and Codes
    String INSERT_STATEMENT = "insert";
    String DELETE_STATEMENT = "delete";
    String UPDATE_STATEMENT = "update";
    int INSERT_STATEMENT_CODE = 10;
    int DELETE_STATEMENT_CODE = 11;
    int UPDATE_STATEMENT_CODE = 12;

    //VDL Keywords and Codes
    String SELECT_STATEMENT = "select";
    int SELECT_STATEMENT_CODE = 20;

    //System Keywords and Codes
    String EXIT_STATEMENT = "exit";
    String HELP_STATEMENT = "help";
    int EXIT_STATEMENT_CODE = 100;
    int HELP_STATEMENT_CODE = 101;
}
