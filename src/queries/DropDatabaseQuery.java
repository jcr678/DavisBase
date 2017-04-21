package queries;

import Model.Condition;
import Model.IQuery;
import Model.Result;
import QueryParser.DatabaseHelper;
import common.Constants;
import common.Utils;

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

        Condition condition = Condition.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        IQuery deleteEntryQuery = new DeleteQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, condition, true);
        deleteEntryQuery.ExecuteQuery();

        deleteEntryQuery = new DeleteQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, condition, true);
        deleteEntryQuery.ExecuteQuery();

        boolean isDeleted = RecursivelyDelete(database);

        if(!isDeleted){
            Utils.printError(String.format("Unable to delete database '%s'", this.databaseName));
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
            Utils.printError(String.format("Database '%s' doesn't exist", this.databaseName));
            return false;
        }

        return true;
    }

    public boolean RecursivelyDelete(File file){
        if(file == null) return true;
        boolean isDeleted = false;

        if(file.isDirectory()) {
            for (File childFile : file.listFiles()) {
                if (childFile.isFile()) {
                    isDeleted = childFile.delete();
                    if (!isDeleted) return false;
                } else {
                    isDeleted = RecursivelyDelete(childFile);
                    if (!isDeleted) return false;
                }
            }
        }

        return file.delete();
    }
}
