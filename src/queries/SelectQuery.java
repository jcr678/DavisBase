package queries;

import Model.*;
import common.CatalogDB;
import common.Constants;
import common.Utils;
import datatypes.DT_Text;
import datatypes.base.DT;
import javafx.util.Pair;
import storage.StorageManager;
import storage.model.DataRecord;
import storage.model.InternalCondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SelectQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public ArrayList<String> columns;
    public boolean isSelectAll;
    public Condition condition;

    public SelectQuery(String databaseName, String tableName, ArrayList<String> columns, Condition condition, boolean isSelectAll){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = columns;
        this.condition = condition;
        this.isSelectAll = isSelectAll;
    }

    @Override
    public Result ExecuteQuery() {
        ResultSet resultSet = ResultSet.CreateResultSet();

        ArrayList<Record> records = GetData();
        resultSet.setColumns(this.columns);
        for(Record record : records){
            resultSet.addRecord(record);
        }

        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        Pair<HashMap<String, Integer>, HashMap<Integer, String>> maps = mapOrdinalIdToColumnName(this.tableName);
        HashMap<String, Integer> columnToIdMap = maps.getKey();
        StorageManager manager = new StorageManager();

        // Check if the table exists.
        if (!manager.checkTableExists(Utils.getUserDatabasePath(this.databaseName), tableName)) {
            Utils.printMissingTableError(tableName);
            return false;
        }

        HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);

        // Validate column data type.
        if (condition != null) {
            List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
            if (!Utils.checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, condition)) {
                return false;
            }
        }

        if(this.columns != null){
            for(String column : this.columns){
                if(!columnToIdMap.containsKey(column)){
                    Utils.printError(String.format("Unknown column '%s' in table '%s'", column, this.tableName));
                    return false;
                }
            }
        }

        if(condition != null){
            if(!columnToIdMap.containsKey(condition.column)){
                Utils.printError((String.format("Unknown column '%s' in table '%s'", condition.column, this.tableName)));
                return false;
            }
        }

        return true;
    }

    private ArrayList<Record> GetData(){
        ArrayList<Record> records = new ArrayList<>();
        Pair<HashMap<String, Integer>, HashMap<Integer, String>> maps = mapOrdinalIdToColumnName(this.tableName);
        HashMap<String, Integer> columnToIdMap = maps.getKey();
        ArrayList<Byte> columnsList = new ArrayList<>();
        List<DataRecord> internalRecords = null;
        StorageManager manager = new StorageManager();

        ArrayList<Short> operators = new ArrayList<>();
        ArrayList<Byte> columnIndices = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        if(condition != null){

            if (columnToIdMap.containsKey(this.condition.column)) {
                columnIndices.add(columnToIdMap.get(this.condition.column).byteValue());
            }

            DT dataType = DT.CreateDT(this.condition.value);
            values.add(dataType);

            Short operatorShort = Utils.ConvertFromOperator(condition.operator);
            operators.add(operatorShort);
        }

        if(this.columns == null) {
            internalRecords = manager.findRecord(Utils.getUserDatabasePath(this.databaseName),
                    this.tableName, columnIndices, values, operators, false);

            HashMap<Integer, String> idToColumnMap = maps.getValue();
            this.columns = new ArrayList<>();
            for (int i=0; i<columnToIdMap.size();i++) {
                if(idToColumnMap.containsKey(i)){
                    columnsList.add((byte)i);
                    this.columns.add(idToColumnMap.get(i));
                }
            }
        }
        else {
            for (String column : this.columns) {
                if (columnToIdMap.containsKey(column)) {
                    columnsList.add(columnToIdMap.get(column).byteValue());
                }
            }

            internalRecords = manager.findRecord(Utils.getUserDatabasePath(this.databaseName),
                    this.tableName, columnIndices, values, operators, columnsList, false);
        }

        Byte[] columnIds = new Byte[columnsList.size()];
        int k = 0;
        for(Byte column : columnsList){
            columnIds[k] = column;
            k++;
        }

        HashMap<Integer, String> idToColumnMap = maps.getValue();
        Arrays.sort(columnIds);
        for(DataRecord internalRecord : internalRecords){
            Object[] dataTypes = new DT[internalRecord.getColumnValueList().size()];
            k=0;
            for(Object columnValue : internalRecord.getColumnValueList()){
                dataTypes[k] = columnValue;
                k++;
            };
            Record record = Record.CreateRecord();
            for(int i=0;i<columnIds.length;i++) {
                Literal literal = null;
                if(idToColumnMap.containsKey((int)columnIds[i])) {
                    literal = Literal.CreateLiteral((DT)dataTypes[i], Utils.resolveClass(dataTypes[i]));
                    record.put(idToColumnMap.get((int)columnIds[i]), literal);
                }
            }
            records.add(record);
        }

        return records;
    }


    public Pair<HashMap<String, Integer>, HashMap<Integer, String>> mapOrdinalIdToColumnName(String tableName) {
        HashMap<Integer, String> idToColumnMap = new HashMap<>();
        HashMap<String, Integer> columnToIdMap = new HashMap<>();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(new InternalCondition(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DT_Text(tableName)));

        StorageManager manager = new StorageManager();
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            idToColumnMap.put(i, ((DT) object).getStringValue());
            columnToIdMap.put(((DT) object).getStringValue(), i);
        }

        return new Pair<>(columnToIdMap, idToColumnMap);
    }
}
