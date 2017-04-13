package parser;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Created by dakle on 2/4/17.
 */
public interface StatementInterface {

    String COMMAND = "command";
    String CODE = "code";
    String ATTRS = "attrs";
    String CHILDREN = "children";

    public JSONObject createObject(String query);

    public JSONObject getAttributes(String query);

    public JSONArray getChildren(String query);
}
