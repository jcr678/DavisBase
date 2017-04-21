package query;

import common.DatabaseHelper;
import common.Utils;
import datatypes.base.DT;
import exceptions.InternalException;
import io.IOManager;
import io.model.InternalCondition;
import model.Condition;
import model.Result;
import query.base.IQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Mahesh on 15/4/17.
 */

public class DeleteQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public ArrayList<Condition> conditions;
    public boolean isInternal = false;

    public DeleteQuery(String databaseName, String tableName, ArrayList<Condition> conditions){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conditions = conditions;
    }

    public DeleteQuery(String databaseName, String tableName, ArrayList<Condition> conditions, boolean isInternal){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conditions = conditions;
        this.isInternal = isInternal;
    }

    @Override
    public Result ExecuteQuery() {

        try {
            // Delete the record.
            int rowCount = 0;
            IOManager manager = new IOManager();

            if (conditions == null) {
                rowCount = manager.deleteRecord(databaseName, tableName, (new ArrayList<>()));
            } else {
                List<InternalCondition> conditionList = new ArrayList<>();
                InternalCondition internalCondition = new InternalCondition();

                for (Condition condition : this.conditions) {
                    internalCondition = new InternalCondition();
                    List<String> retrievedColumns = DatabaseHelper.getDatabaseHelper().fetchAllTableColumns(this.databaseName, tableName);
                    int idx = retrievedColumns.indexOf(condition.column);
                    internalCondition.setIndex((byte) idx);

                    DT dataType = DT.CreateDT(condition.value);
                    internalCondition.setValue(dataType);

                    internalCondition.setConditionType(Utils.ConvertFromOperator(condition.operator));
                    conditionList.add(internalCondition);
                }

                rowCount = manager.deleteRecord(databaseName, tableName, conditionList);

            }

            Result result = new Result(rowCount, this.isInternal);
            return result;
        } catch (InternalException e) {
            Utils.printMessage(e.getMessage());
        }
        return null;
    }

    @Override
    public boolean ValidateQuery() {
        try {
            // Check if the table exists.
            IOManager manager = new IOManager();
            if (!manager.checkTableExists(this.databaseName, tableName)) {
                Utils.printMissingTableError(this.databaseName, tableName);
                return false;
            }

            // Validate the columns.
            if (this.conditions == null) {
                // No condition.
                return true;
            } else {
                // Condition is present.
                // Validate the column in the condition.
                List<String> retrievedColumns = DatabaseHelper.getDatabaseHelper().fetchAllTableColumns(this.databaseName, tableName);
                HashMap<String, Integer> columnDataTypeMapping = DatabaseHelper.getDatabaseHelper().fetchAllTableColumnDataTypes(this.databaseName, tableName);

                for (Condition condition : this.conditions) {
                    // Validate the existence of the column.
                    if (!checkConditionColumnValidity(retrievedColumns)) {
                        return false;
                    }

                    // Validate column data type.
                    if (!Utils.checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, condition)) {
                        return false;
                    }
                }
            }
        } catch (InternalException e) {
            Utils.printMessage(e.getMessage());
            return false;
        }
        return true;
    }


    private boolean checkConditionColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (Condition condition : this.conditions) {
            String tableColumn = condition.column;
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
            }

            if (!columnsValid) {
                Utils.printMessage("Column " + invalidColumn + " is not present in the table " + tableName + ".");
                return false;
            }
        }

        return true;
    }
}
