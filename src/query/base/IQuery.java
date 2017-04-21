package query.base;

import model.Result;

/**
 * Created by Mahesh on 15/4/17.
 */

public interface IQuery {
    Result ExecuteQuery();
    boolean ValidateQuery();
}
