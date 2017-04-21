package query;

import model.Condition;
import query.base.IQuery;
import model.Result;
import query.parser.QueryParser;
import common.Constants;
import common.Utils;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by Mahesh on 4/12/2017.
 */
public class DropTableQuery implements IQuery {
    public String databaseName;
    public String tableName;

    public DropTableQuery(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public Result ExecuteQuery() {

        ArrayList<Condition> conditionList = new ArrayList<>();
        Condition condition = Condition.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        conditionList.add(condition);

        condition = Condition.CreateCondition(String.format("table_name = '%s'", this.tableName));
        conditionList.add(condition);

        IQuery deleteEntryQuery = new DeleteQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, conditionList, true);
        deleteEntryQuery.ExecuteQuery();

        deleteEntryQuery  = new DeleteQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditionList, true);
        deleteEntryQuery.ExecuteQuery();

        File table = new File(String.format("%s/%s/%s%s", Constants.DEFAULT_DATA_DIRNAME, this.databaseName, this.tableName, Constants.DEFAULT_FILE_EXTENSION));
        boolean isDeleted = Utils.RecursivelyDelete(table);

        if(!isDeleted){
            Utils.printError(String.format("Unable to delete table '%s.%s'", this.databaseName, this.tableName));
            return null;
        }


        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean tableExists = QueryParser.isTableExists(this.databaseName, this.tableName);

        if(!tableExists){
            Utils.printError(String.format("Unknown table '%s.%s'", this.databaseName, this.tableName));
            return false;
        }

        return true;
    }
}
