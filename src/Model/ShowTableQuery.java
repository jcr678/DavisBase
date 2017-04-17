package Model;

import common.Constants;
import common.Utils;
import storage.StorageManager;
import storage.model.DataRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
