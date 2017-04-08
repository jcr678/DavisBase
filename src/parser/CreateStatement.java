package parser;

import org.json.simple.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dakle on 30/3/17.
 */
public class CreateStatement implements StatementInterface {

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
        JSONObject childrenObject = getChildren(query);
        if(childrenObject == null) return null;
        jsonObject.put(CHILDREN, childrenObject);
        return jsonObject;
    }

    @Override
    public JSONObject getAttributes(String query) {
        return new JSONObject();
    }

    @Override
    public JSONObject getChildren(String query) {
        Pattern pattern = Pattern.compile(RegexExpressions.CREATE_STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            String subType = matcher.group(1);
            switch (subType) {
                case StatementKeywords.TABLE_STATEMENT:
                    return getTableJson(query);
            }
        }
        return null;
    }

    public JSONObject getTableJson(String query) {
        Pattern pattern = Pattern.compile(RegexExpressions.CREATE_TABLE_STATEMENT_VALIDATION_REGEX);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            JSONObject tableJsonObject = new JSONObject();
            tableJsonObject.put("name", matcher.group(1));
            //https://regex101.com/r/K6ajX6/7
            return tableJsonObject;
        }
        return null;
    }
}
