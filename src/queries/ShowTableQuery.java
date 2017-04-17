package queries;

import Model.IQuery;
import Model.Result;
import Model.ResultSet;
import common.Constants;

import java.util.ArrayList;

/**
 * Created by dhruv on 4/12/2017.
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
        IQuery query = new SelectQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME,
                columns,null, false);
        ResultSet resultSet = (ResultSet) query.ExecuteQuery();
        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        return true;
    }
}
