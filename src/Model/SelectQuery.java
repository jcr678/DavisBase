package Model;

import java.util.ArrayList;

public class SelectQuery implements IQuery{
    public String tableName;
    public ArrayList<String> columns;
    public boolean isSelectAll;
    public Condition condition;

    public SelectQuery(String tableName, ArrayList<String> columns, Condition condition, boolean isSelectAll){
        this.tableName = tableName;
        this.columns = columns;
        this.condition = condition;
        this.isSelectAll = isSelectAll;
    }

    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("FirstName");
        columns.add("LastName");
        columns.add("Id");
        columns.add("Score");
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
        dataList.add("Dhruva Pendharkar 512 2.0");
        dataList.add("Parag Dakle 64 3.0");
        dataList.add("Mahesh S 64 3.8");
        dataList.add("Takshak Desai 164 5.5");
        dataList.add("Sahil Dhoked 640 9.34");
        dataList.add("Chirodeep Roy 964 3.22");

        for(String data : dataList){
            String[] parts = data.split(" ");
            Record record = Record.CreateRecord();
            record.put("FirstName", Literal.CreateLiteral(String.format("\"%s\"", parts[0])));
            record.put("LastName", Literal.CreateLiteral(String.format("\"%s\"", parts[1])));
            record.put("Id", Literal.CreateLiteral(parts[2]));
            record.put("Score", Literal.CreateLiteral(parts[3]));
            records.add(record);
        }

        return records;
    }
}
