package query;

import query.base.IQuery;
import model.Result;
import query.parser.QueryParser;
import common.Utils;

/**
 * Created by Mahesh on 4/12/2017.
 */

public class UseDatabaseQuery implements IQuery {
    public String databaseName;

    public UseDatabaseQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        QueryParser.CurrentDatabaseName = this.databaseName;
        Utils.printMessage("Database changed");
        return null;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = QueryParser.IsDatabaseExists(this.databaseName);
        if(!databaseExists){
            Utils.printMessage(String.format("Unknown database '%s'", this.databaseName));
        }

        return databaseExists;
    }
}
