package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for FOREACH clause in OpenCypher queries.
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3328
 */
class OpenCypherForeachTest {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final String databasePath = "./target/databases/testopencypher-foreach-" + testInfo.getTestMethod().get().getName();
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

  @Test
  void foreachCreateEdges_issue3328() {
    // Regression test for issue #3328: FOREACH creating edges from matched nodes
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:TestNode {name: 'Root'})");
    });

    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (root:TestNode {name: 'Root'})
          FOREACH (i IN [1, 2, 3] |
            CREATE (root)-[:HAS_ITEM]->(:Item {id: i})
          )""");
    });

    // Verify: 3 items should have been created
    final ResultSet verify = database.query("opencypher",
        "MATCH (:TestNode)-[:HAS_ITEM]->(item:Item) RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    assertThat(((Number) r.getProperty("total")).longValue()).isEqualTo(3L);
  }

  @Test
  void foreachCreateNodes() {
    // Simple FOREACH creating standalone nodes
    database.transaction(() -> {
      database.command("opencypher",
          """
          FOREACH (name IN ['Alice', 'Bob', 'Charlie'] |
            CREATE (:Person {name: name})
          )""");
    });

    final ResultSet verify = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY name");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  void foreachWithMatchContext() {
    // Create initial data
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (:Person {name: 'Bob'})");
    });

    // Use FOREACH inside a query with MATCH context
    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (p:Person)
          FOREACH (tag IN ['developer', 'tester'] |
            CREATE (p)-[:HAS_TAG]->(:Tag {value: tag})
          )""");
    });

    // Each person should have 2 tags = 4 total
    final ResultSet verify = database.query("opencypher",
        "MATCH (:Person)-[:HAS_TAG]->(t:Tag) RETURN count(t) AS total");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) verify.next().getProperty("total")).longValue()).isEqualTo(4L);
  }

  @Test
  void foreachPassesThroughInputRows() {
    // FOREACH should pass through input rows unchanged
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          """
          CREATE (root:Root {name: 'test'})
          FOREACH (i IN [1, 2] |
            CREATE (:Child {id: i})
          )
          RETURN root.name AS name""");

      assertThat(result.hasNext()).isTrue();
      assertThat((String) result.next().getProperty("name")).isEqualTo("test");
    });
  }

  @Test
  void foreachWithSetClause() {
    // Create initial data
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:Item {id: 1, status: 'pending'})");
      database.command("opencypher",
          "CREATE (:Item {id: 2, status: 'pending'})");
      database.command("opencypher",
          "CREATE (:Item {id: 3, status: 'pending'})");
    });

    // Use FOREACH with SET to update nodes
    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (item:Item)
          WITH collect(item) AS items
          FOREACH (item IN items |
            SET item.status = 'done'
          )""");
    });

    // All items should be 'done'
    final ResultSet verify = database.query("opencypher",
        "MATCH (item:Item) WHERE item.status = 'done' RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) verify.next().getProperty("total")).longValue()).isEqualTo(3L);
  }

  // Issue #4185: nodes deleted in FOREACH must not be visible to a later MATCH in the same query
  @Test
  void laterMatchDoesNotSeeNodeDeletedInForeach() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    final ResultSet rs = database.command("opencypher",
        """
            MATCH p = ()-[*]->()
            WITH relationships(p) AS rels
            FOREACH (r IN rels | DELETE endNode(r) DELETE r)
            WITH 1 AS dummy
            MATCH (n:Node)
            RETURN n.name AS name
            ORDER BY name""");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      names.add(r.getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }

  // Issue #4185: DETACH DELETE inside FOREACH removes both the vertex and connected edges in one iteration
  @Test
  void detachDeleteInsideForeachClearsBothEndsAndEdge() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.command("opencypher",
        """
            MATCH p = ()-[*]->()
            WITH relationships(p) AS rels
            FOREACH (r IN rels | DETACH DELETE endNode(r))
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (verify.hasNext()) {
      names.add(verify.next().getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }

  // Issue #4185: nested FOREACH with DELETE in inner loop isolates iterations correctly
  @Test
  void nestedForeachDeleteWorks() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:Node {name:'X'}), (:Node {name:'Y'}), (:Node {name:'Z'})");
    });

    database.command("opencypher",
        """
            MATCH (n:Node)
            WITH collect(n) AS allNodes
            FOREACH (group IN [allNodes] |
              FOREACH (node IN group | DELETE node)
            )
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN count(n) AS total");
    assertThat(verify.hasNext()).isTrue();
    assertThat(verify.next().<Number>getProperty("total").longValue()).isZero();
  }

  // Issue #4185: mixed SET and DELETE inside FOREACH - SET on survivor runs before iteration deletes flush
  @Test
  void mixedSetAndDeleteInsideForeach() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.command("opencypher",
        """
            MATCH (a:Node {name:'A'})-[r:REL]->(b:Node {name:'B'})
            WITH a, r, b
            FOREACH (x IN [1] |
              SET a.tagged = true
              DELETE r
              DELETE b
            )
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher",
        "MATCH (n:Node) RETURN n.name AS name, n.tagged AS tagged ORDER BY name");
    final List<String> rows = new ArrayList<>();
    while (verify.hasNext()) {
      final Result r = verify.next();
      rows.add(r.<String>getProperty("name") + ":" + r.<Boolean>getProperty("tagged"));
    }
    assertThat(rows).containsExactly("A:true");
  }

  // Issue #4185: FOREACH+DELETE inside an already-active outer transaction reuses that transaction
  @Test
  void foreachDeleteInsideOuterTransaction() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.transaction(() -> {
      database.command("opencypher",
          """
              MATCH p = ()-[*]->()
              WITH relationships(p) AS rels
              FOREACH (r IN rels | DELETE endNode(r) DELETE r)""");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (verify.hasNext()) {
      names.add(verify.next().getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }

  // Issue #4185: FOREACH+DELETE alone still works when there is no later MATCH
  @Test
  void foreachDeleteAloneStillWorks() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.command("opencypher",
        """
            MATCH p = ()-[*]->()
            WITH relationships(p) AS rels
            FOREACH (r IN rels | DELETE endNode(r) DELETE r)
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (verify.hasNext()) {
      names.add(verify.next().getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }
}
