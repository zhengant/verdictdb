package org.verdictdb.sqlwriter;

import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class QueryToSql {
  
  public static String convert(SqlSyntax syntax, SqlConvertible query) throws VerdictDBException {
    if (query == null) {
      throw new VerdictDBValueException("null value passed");
    }
    
    if (query instanceof SelectQuery) {
      SelectQueryToSql tosql = new SelectQueryToSql(syntax);
      return tosql.toSql((SelectQuery) query);
    }
    else if (query instanceof CreateTableAsSelectQuery) {
      CreateTableToSql tosql = new CreateTableToSql(syntax);
      return tosql.toSql((CreateTableAsSelectQuery) query);
    }
    else if (query instanceof DropTableQuery) {
      DropTableToSql tosql = new DropTableToSql(syntax);
      return tosql.toSql((DropTableQuery) query);
    }
    else if (query instanceof SetOperationRelation) {
      SetOperationToSql tosql = new SetOperationToSql(syntax);
      return tosql.toSql((SetOperationRelation) query);
    }
    else {
      throw new VerdictDBTypeException(query);
    }
  }

}
