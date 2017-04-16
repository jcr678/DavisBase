package Model;

/**
 * Created by dhruv on 4/12/2017.
 */
public class DropTableQuery implements IQuery {
    public String tableName;

    public DropTableQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public Result ExecuteQuery() {
        /*TODO : replace with actual logic*/
        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        /*TODO : replace with actual logic*/
        return true;
    }
}
