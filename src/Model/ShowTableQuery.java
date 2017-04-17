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
        List<String> dataList = new ArrayList<>();
        /*dataList.add("Company");
        dataList.add("Employee");
        dataList.add("Subjects");
        dataList.add("Projects");
        dataList.add("Semester");
        dataList.add("SomeTableNameThatIsVeryVeryLong");
        dataList.add("SomeTableNameThatIsLonger");*/


        ArrayList<String> columns = new ArrayList<>();
        columns.add("table_name");
        IQuery query = new SelectQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME,
                columns,null, false);
        ResultSet resultSet = (ResultSet) query.ExecuteQuery();
        resultSet.Display();

        return records;
    }
}
