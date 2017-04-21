package queries;

import model.Condition;
import model.IQuery;
import model.Result;
import model.ResultSet;
import QueryParser.DatabaseHelper;
import common.Constants;
import common.Utils;

import java.util.ArrayList;

/**
 * Created by Mahesh on 4/12/2017.
 */
public class ShowTableQuery implements IQuery {

    public String databaseName;

    public ShowTableQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("table_name");

        Condition condition = Condition.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        ArrayList<Condition> conditionList = new ArrayList<>();
        conditionList.add(condition);

        IQuery query = new SelectQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, columns, conditionList, false);
        if (query.ValidateQuery()) {
            ResultSet resultSet = (ResultSet) query.ExecuteQuery();
            return resultSet;
        }

        return null;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DatabaseHelper.IsDatabaseExists(this.databaseName);
        if(!databaseExists){
            Utils.printError(String.format("Unknown database '%s'", this.databaseName));
        }
        return databaseExists;
    }
}
