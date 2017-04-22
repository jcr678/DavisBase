package query.vdl;

import common.DatabaseHelper;
import query.QueryHandler;
import query.base.IQuery;
import query.model.result.Result;
import common.Utils;

/**
 * Created by parag on 4/12/2017.
 */

public class UseDatabaseQuery implements IQuery {
    public String databaseName;

    public UseDatabaseQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        QueryHandler.ActiveDatabaseName = this.databaseName;
        Utils.printMessage("Database changed");
        return null;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DatabaseHelper.getDatabaseHelper().databaseExists(this.databaseName);
        if(!databaseExists){
            Utils.printMissingDatabaseError(this.databaseName);
        }

        return databaseExists;
    }
}
