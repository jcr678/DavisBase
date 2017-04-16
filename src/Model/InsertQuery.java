package Model;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InsertQuery implements IQuery {
    public String tableName;
    public ArrayList<String> columns;
    public ArrayList<Literal> values;

    public InsertQuery(String tableName, ArrayList<String> columns, ArrayList<Literal> values){
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
    }

    @Override
    public Result ExecuteQuery() {
        /*TODO : replace with actual logic*/
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
