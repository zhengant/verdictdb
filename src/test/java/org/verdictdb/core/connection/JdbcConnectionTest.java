package org.verdictdb.core.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.HiveSyntax;

public class JdbcConnectionTest {
  
static Connection conn;
  
  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:testconn;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
  }

  @Test
  public void testJdbcConnection() throws VerdictDBDbmsException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));
    
    JdbcConnection jdbc = new JdbcConnection(conn, new HiveSyntax());
    
    jdbc.execute("CREATE TABLE PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      jdbc.execute(String.format("INSERT INTO PERSON(id, name) VALUES(%s, '%s')", id, name));
    }
  }

}
