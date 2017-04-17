package Model;

import QueryParser.DatabaseHelper;

import java.io.File;

/**
 * Created by dhruv on 4/12/2017.
 */
public class DropDatabaseQuery implements IQuery {
    public String databaseName;

    public DropDatabaseQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        /*TODO : Replace using constants file*/
        String DEFAULT_DATA_DIRNAME = "data";
        File database = new File(DEFAULT_DATA_DIRNAME + "/" + this.databaseName);
        boolean isDeleted = database.delete();

        if(!isDeleted){
            System.out.println(String.format("Unable to delete database '%s'", this.databaseName));
            return null;
        }

        if(DatabaseHelper.CurrentDatabaseName == this.databaseName){
            DatabaseHelper.CurrentDatabaseName = "";
        }

        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DatabaseHelper.IsDatabaseExists(this.databaseName);

        if(!databaseExists){
            System.out.println(String.format("Database '%s' dosent exist", this.databaseName));
            return false;
        }

        return true;
    }
}
