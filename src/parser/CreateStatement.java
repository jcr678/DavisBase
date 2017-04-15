package parser;

import common.Constants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import storage.StorageManager;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dakle on 30/3/17.
 */
public class CreateStatement implements StatementInterface {

    private final String TABLE_NAME = "table_name";

    JSONObject jsonObject;

    public CreateStatement() {
        jsonObject = new JSONObject();
    }

    @Override
    public JSONObject createObject(String query) {
        jsonObject.put(COMMAND, StatementKeywords.CREATE_STATEMENT);
        jsonObject.put(CODE, StatementKeywords.CREATE_STATEMENT_CODE);
        JSONObject attrsObject = getAttributes(query);
        if(attrsObject == null) return null;
        jsonObject.put(ATTRS, attrsObject);
        JSONArray childrenObject = getChildren(query);
        if(childrenObject == null) return null;
        jsonObject.put(CHILDREN, childrenObject);
        return jsonObject;
    }

    @Override
    public JSONObject getAttributes(String query) {
        return new JSONObject();
    }

    @Override
    public JSONArray getChildren(String query) {
        JSONArray children = new JSONArray();
        Pattern pattern = Pattern.compile(RegexExpressions.CREATE_STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            String subType = matcher.group(1);
            switch (subType) {
                case StatementKeywords.TABLE_STATEMENT:
                    children.add(getTableJson(query));
                    break;
            }
        }
        return children;
    }

    public JSONObject getTableJson(String query) {
        Pattern pattern = Pattern.compile(RegexExpressions.CREATE_TABLE_STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            JSONObject tableJsonObject = new JSONObject();
            tableJsonObject.put(TABLE_NAME, matcher.group(1));
            //https://regex101.com/r/K6ajX6/7
            return tableJsonObject;
        }
        return null;
    }

    public boolean createTable(String databaseName, JSONObject createTableJSON, boolean isSystemTable) {
        if(createTableJSON != null) {
            if((int)createTableJSON.get(CODE) == StatementKeywords.CREATE_STATEMENT_CODE) {
                StorageManager manager = new StorageManager();
                String tableName = (String) createTableJSON.get(TABLE_NAME);
                if(isSystemTable) {
                    manager.createTable(Constants.DEFAULT_DATA_DIRNAME + "/" + databaseName, tableName + Constants.DEFAULT_FILE_EXTENSION);
                }
                else if(!isSystemTable) {
                    manager.createTable(Constants.DEFAULT_DATA_DIRNAME + "/" + databaseName + "/" + tableName, tableName + "/" + Constants.DEFAULT_FILE_EXTENSION);
                }
            }
        }
        return false;
    }


}
