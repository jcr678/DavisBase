package queries;

import model.IQuery;
import model.Result;
import QueryParser.DatabaseHelper;
import common.Utils;

import java.io.File;

/**
 * Created by Mahesh on 15/4/17.
 */

public class CreateDatabaseQuery implements IQuery {
    public String databaseName;

    public CreateDatabaseQuery(String databaseName){
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        File database = new File(Utils.getDatabasePath(this.databaseName));
        boolean isCreated = database.mkdir();

        if(!isCreated){
            System.out.println(String.format("Unable to create database '%s'", this.databaseName));
            return null;
        }

        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DatabaseHelper.IsDatabaseExists(this.databaseName);

        if(databaseExists){
            System.out.println(String.format("Database '%s' already exists", this.databaseName));
            return false;
        }

        return true;
    }
}
