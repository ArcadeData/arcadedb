package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/4993
 * <p>
 * FOREACH with CASE WHEN exists(pattern) must honor the CASE condition: the FOREACH body
 * must only execute for rows where the pattern predicate is true.
 */
class Issue4993ForeachCaseExistsTest {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final String databasePath = "./target/databases/testopencypher-4993-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void seed() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:User {active: true}), (b:User {active: false}), (a)-[:FRIEND]->(b)");
      database.command("opencypher", "CREATE (c:User {active: true})");
    });
  }

  private long countFlagged() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (u:User) WHERE u.flagged = true RETURN count(u) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    return ((Number) r.getProperty("cnt")).longValue();
  }

  @Test
  void foreachCaseWhenExistsFunctionStyle() {
    seed();
    database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (u:User {active: true})
            FOREACH (ignore IN CASE WHEN EXISTS((u)-[:FRIEND]->(:User {active: false})) THEN [1] ELSE [] END |
              SET u.flagged = true
            )"""));

    assertThat(countFlagged()).isEqualTo(1L);
  }

  @Test
  void foreachCaseWhenSizeControlCase() {
    seed();
    database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (u:User {active: true})
            FOREACH (ignore IN CASE WHEN size([(u)-[:FRIEND]->(f:User) WHERE f.active = false | f]) > 0 THEN [1] ELSE [] END |
              SET u.flagged = true
            )"""));

    assertThat(countFlagged()).isEqualTo(1L);
  }

  @Test
  void existsFunctionStyleInReturn() {
    // The same function-style exists(pattern) must evaluate as a pattern predicate in any expression
    // context, not just FOREACH. Here it is projected directly in RETURN.
    seed();
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (u:User {active: true})
        RETURN u.active AS active, EXISTS((u)-[:FRIEND]->(:User {active: false})) AS hasInactiveFriend
        ORDER BY hasInactiveFriend DESC""");

    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("hasInactiveFriend")).isTrue();
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("hasInactiveFriend")).isFalse();
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void foreachCaseWhenExistsSubqueryControlCase() {
    seed();
    database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (u:User {active: true})
            FOREACH (ignore IN CASE WHEN EXISTS { (u)-[:FRIEND]->(:User {active: false}) } THEN [1] ELSE [] END |
              SET u.flagged = true
            )"""));

    assertThat(countFlagged()).isEqualTo(1L);
  }
}
