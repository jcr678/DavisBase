package queries;

import model.*;
import common.Constants;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by Mahesh on 4/12/2017.
 */

public class ShowDatabaseQuery implements IQuery {
    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("Database");
        ResultSet resultSet = ResultSet.CreateResultSet();
        resultSet.setColumns(columns);
        ArrayList<Record> records = GetDatabases();

        for(Record record : records){
            resultSet.addRecord(record);
        }

        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        return true;
    }

    private ArrayList<Record> GetDatabases(){
        ArrayList<Record> records = new ArrayList<>();

        File baseData = new File(Constants.DEFAULT_DATA_DIRNAME);

        for(File data : baseData.listFiles()){
            if(!data.isDirectory()) continue;
            Record record = Record.CreateRecord();
            record.put("Database", Literal.CreateLiteral(String.format("\"%s\"", data.getName())));
            records.add(record);
        }

        return records;
    }
}
