package Model;

import java.util.Random;

public class DeleteQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public Condition condition;
    public boolean isInternal = false;

    public DeleteQuery(String databaseName, String tableName, Condition condition){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.condition = condition;
    }

    public DeleteQuery(String databaseName, String tableName, Condition condition, boolean isInternal){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.condition = condition;
        this.isInternal = isInternal;
    }

    @Override
    public Result ExecuteQuery() {
        /*TODO : replace with actual logic*/
        Random random = new Random();
        Result result = new Result(random.nextInt(50), this.isInternal);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        /*TODO : replace with actual logic*/
        return true;
    }
}
