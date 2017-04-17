package Model;

import java.util.Random;

public class UpdateQuery implements IQuery{
    public String databaseName;
    public String tableName;
    public String columnName;
    public Literal value;
    public Condition condition;

    public UpdateQuery(String databaseName, String tableName, String columnName, Literal value, Condition condition){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.value = value;
        this.condition = condition;
    }

    @Override
    public Result ExecuteQuery() {
        Random random = new Random();
        Result result = new Result(random.nextInt(50));
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        /*TODO : replace with actual logic*/
        return true;
    }
}
