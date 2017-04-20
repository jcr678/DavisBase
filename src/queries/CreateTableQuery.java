package queries;

import Model.Column;
import Model.IQuery;
import Model.Result;
import common.Constants;
import common.Utils;
import helpers.UpdateStatementHelper;
import storage.StorageManager;
import storage.model.InternalColumn;

import java.util.ArrayList;
import java.util.List;

public class CreateTableQuery implements IQuery {
    public String tableName;
    public ArrayList<Column> columns;
    public boolean hasPrimaryKey;
    public String databaseName;

    public CreateTableQuery(String databaseName, String tableName, ArrayList<Column> columns, boolean hasPrimaryKey){
        this.tableName = tableName;
        this.columns = columns;
        this.hasPrimaryKey = hasPrimaryKey;
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        StorageManager storageManager = new StorageManager();

        // Check if database exists
        if (!storageManager.databaseExists(this.databaseName)) {
            // Database does not exist.
            Utils.printMissingDatabaseError(databaseName);
            return false;
        }
        

        if (storageManager.checkTableExists(this.databaseName, tableName)) {
            // Table already exists.
            Utils.printDuplicateTableError(tableName);
            return false;
        }
        else {
            // Create new table.
            boolean status = storageManager.createTable(this.databaseName, tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (!status) {
                Utils.printError("Failed to create table " + tableName);
                return false;
            }
            else {
                Utils.printMessage("Table " + tableName + " successfully created.");
                List<InternalColumn> columnsList = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    InternalColumn internalColumn = new InternalColumn();
                        /*  1. Add column name.
                            2. Add column data type.
                            3. Add column key constraint.
                            4. Add column null constraint.
                         */

                    Column column = columns.get(i);
                    internalColumn.setName(column.name);
                    internalColumn.setDataType(column.type.toString());

                    // Set the primary key constraint.
                    if (hasPrimaryKey && i == 0) {
                        internalColumn.setPrimary(true);
                    }
                    else {
                        internalColumn.setPrimary(false);
                    }

                    // Set the NULL constraints.
                    if (hasPrimaryKey && i == 0) {
                        internalColumn.setNullable(false);
                    }
                    else if (column.isNull) {
                        internalColumn.setNullable(true);
                    }
                    else {
                        internalColumn.setNullable(false);
                    }

                    columnsList.add(internalColumn);
                }

                UpdateStatementHelper statement = new UpdateStatementHelper();
                int startingRowId = statement.updateSystemTablesTable(this.databaseName, tableName, columns.size());
                boolean systemTableUpdateStatus = statement.updateSystemColumnsTable(this.databaseName, tableName, startingRowId, columnsList);
                if (systemTableUpdateStatus) {
                    Utils.printMessage("System table successfully updated.");
                }
            }
        }


        return true;
    }
}
