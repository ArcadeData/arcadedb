package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class MatchResultTest extends TestHelper {

  /**
   * MATCH query with NOT pattern fails to return unique vertices
   * https://github.com/ArcadeData/arcadedb/issues/1689
   */
  @Test
  public void testIssue1689() {
    database.transaction(() -> database.command("sqlscript", "CREATE VERTEX TYPE Person IF NOT EXISTS;\n"
        + "CREATE PROPERTY Person.role IF NOT EXISTS STRING;\n"
        + "CREATE VERTEX TYPE House IF NOT EXISTS;\n"
        + "CREATE EDGE TYPE LivesIn IF NOT EXISTS;\n"
//        + "CREATE EDGE TYPE DummyEdge IF NOT EXISTS;\n"
//        + "DELETE FROM LivesIn;\n"
//        + "DELETE FROM Person;\n"
//        + "DELETE FROM House;\n"
        + "CREATE VERTEX House;\n"
        + "CREATE VERTEX Person SET role='mom';\n"
        + "CREATE VERTEX Person SET role='dad';\n"
        + "CREATE VERTEX Person SET role='child';\n"
        + "CREATE EDGE LivesIn FROM (SELECT FROM Person) TO (SELECT FROM House);"));

    final ResultSet resultSet = database.query("sql", "MATCH {TYPE: Person, AS: personVertex} -LivesIn-> {TYPE: House}\n"
        + ", NOT {AS: personVertex} -DummyEdge-> {TYPE: House}\n"
        + "RETURN personVertex");

    Set<RID> set = new HashSet<>();
    while (resultSet.hasNext()) {
      final Vertex next = resultSet.nextIfAvailable().getProperty("personVertex");
      set.add(next.getIdentity());
    }

    assertThat(set.size()).isEqualTo(3);
  }
}
