package Model;

import common.Constants;
import common.Utils;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by dhruv on 4/12/2017.
 */
public class ShowTableQuery implements IQuery {
    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("Tables_in_database");
        ResultSet resultSet = ResultSet.CreateResultSet();
        resultSet.setColumns(columns);
        ArrayList<Record> records = DummyData();

        for(Record record : records){
            resultSet.addRecord(record);
        }

        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        /*TODO : replace with actual logic*/
        return true;
    }

    private ArrayList<Record> DummyData(){
        ArrayList<Record> records = new ArrayList<>();
        ArrayList<String> dataList = new ArrayList<>();
        /*dataList.add("Company");
        dataList.add("Employee");
        dataList.add("Subjects");
        dataList.add("Projects");
        dataList.add("Semester");
        dataList.add("SomeTableNameThatIsVeryVeryLong");
        dataList.add("SomeTableNameThatIsLonger");*/

        File file = new File(Utils.getUserDatabasePath(Constants.DEFAULT_USER_DATABASE));
        for (File dbFile : file.listFiles()) {
            dataList.add(dbFile.getName().substring(0, dbFile.getName().length()-4));
        }


        for(String data : dataList){
            Record record = Record.CreateRecord();
            record.put("Tables_in_database", Literal.CreateLiteral(String.format("\"%s\"", data)));
            records.add(record);
        }

        return records;
    }
}
