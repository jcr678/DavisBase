package query.base;

import query.model.result.Result;

/**
 * Created by parag on 15/4/17.
 */

public interface IQuery {
    Result ExecuteQuery();
    boolean ValidateQuery();
}
