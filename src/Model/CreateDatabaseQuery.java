package Model;

import QueryParser.DatabaseHelper;

import java.io.File;
import java.util.ArrayList;

public class CreateDatabaseQuery implements IQuery {
    public String databaseName;

    public CreateDatabaseQuery(String databaseName){
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        /*TODO : Replace using constants file*/
        String DEFAULT_DATA_DIRNAME = "data";
        File database = new File(DEFAULT_DATA_DIRNAME + "/" + this.databaseName);
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
