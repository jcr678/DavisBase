package Model;

import QueryParser.DatabaseHelper;

import java.io.File;

/**
 * Created by dhruv on 4/12/2017.
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
        /*TODO : Replace using constants file*/
        String DEFAULT_DATA_DIRNAME = "data";
        String CATALOG_TABLE = "davisbase_tables";
        String CATALOG_DATABASE = "catalog";
        String TABLE_FILE_EXTENSION = "tbl";

        Condition condition = Condition.CreateCondition(String.format("table_name = '%s'", this.tableName));
        IQuery deleteEntryQuery = new DeleteQuery(CATALOG_DATABASE, CATALOG_TABLE, condition, true);
        DatabaseHelper.ExecuteQuery(deleteEntryQuery);

        File table = new File(String.format("%s/%s/%s.%s", DEFAULT_DATA_DIRNAME, this.databaseName, this.tableName, TABLE_FILE_EXTENSION));
        boolean isDeleted = table.delete();

        if(!isDeleted){
            System.out.println(String.format("Unable to delete table '%s.%s'", this.databaseName, this.tableName));
            return null;
        }

        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean tableExists = DatabaseHelper.isTableExists(this.databaseName, this.tableName);

        if(!tableExists){
            System.out.println(String.format("Unknown table '%s.%s'", this.databaseName, this.tableName));
            return false;
        }

        return true;
    }
}
