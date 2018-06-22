package org.verdictdb.core.execution;

import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.exception.VerdictDbException;

public class CreateAsSelectExecutionNode extends QueryExecutionNode {
  
  String schemaName;
  
  String tableName;
  
  SelectQuery query;
  
  public CreateAsSelectExecutionNode(DbmsConnection conn, String schemaName, String tableName, SelectQuery query) {
    super(conn);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.query = query;
  }

  @Override
  public void executeNode(
      List<ExecutionResult> resultFromChildren, 
      BlockingDeque<ExecutionResult> resultQueue) {
    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(schemaName, tableName, query);
    CreateTableToSql toSql = new CreateTableToSql(conn.getSyntax());
    try {
      String sql = toSql.toSql(createQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    resultQueue.add(ExecutionResult.completeResult());
  }

}
