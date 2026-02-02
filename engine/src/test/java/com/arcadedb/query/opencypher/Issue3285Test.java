package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub Issue #3285: In New Cypher Engine - Traversal happens for One Hop.
 * <p>
 * The issue reports that multi-hop traversals in chained MATCH patterns return incorrect results.
 * Given: one -[:EDG]-> two -[:EDG]-> three
 * Query: MATCH(a:A {name:'one'})-[:EDG]->(aa:A)-[:EDG]->(aaa:A) RETURN aaa
 * Expected: returns vertex 'three'
 * Actual (buggy): returns vertex 'two' - traversal stops at one hop
 * <p>
 * This issue was reported for version 26.1.1.
 */
class Issue3285Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-3285");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create schema matching the issue description
    database.getSchema().createVertexType("A");
    database.getSchema().createEdgeType("EDG");

    // Create test data: one -> two -> three
    database.transaction(() -> {
      final Vertex one = database.newVertex("A").set("name", "one").save();
      final Vertex two = database.newVertex("A").set("name", "two").save();
      final Vertex three = database.newVertex("A").set("name", "three").save();

      one.newEdge("EDG", two, true, (Object[]) null).save();
      two.newEdge("EDG", three, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void exactQueryFromIssue() {
    // This is the EXACT query string from the issue (no spaces, lowercase 'return')
    final ResultSet result = database.query("opencypher",
        "MATCH(a:A {name:'one'})-[:EDG]->(aa:A)-[:EDG]->(aaa:A) return aaa");

    assertThat(result.hasNext()).as("Should have at least one result").isTrue();
    final Result row = result.next();

    // Single variable returns are returned as elements via toElement()
    Vertex aaa;
    if (row.isElement()) {
      aaa = (Vertex) row.toElement();
    } else {
      aaa = (Vertex) row.getProperty("aaa");
    }
    assertThat(aaa).as("Variable 'aaa' should not be null").isNotNull();

    // The issue reports that this returns 'two' instead of 'three'
    assertThat((String) aaa.get("name")).isEqualTo("three");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void multiHopChainedPatternReturnsCorrectVertex() {
    // Same query with standard formatting
    final ResultSet result = database.query("opencypher",
        "MATCH (a:A {name:'one'})-[:EDG]->(aa:A)-[:EDG]->(aaa:A) RETURN aaa");

    assertThat(result.hasNext()).as("Should have at least one result").isTrue();
    final Result row = result.next();

    // Single variable returns are returned as elements via toElement()
    Vertex aaa;
    if (row.isElement()) {
      aaa = (Vertex) row.toElement();
    } else {
      aaa = (Vertex) row.getProperty("aaa");
    }
    assertThat(aaa).as("Variable 'aaa' should not be null").isNotNull();

    // Verify correct vertex is returned at the end of the 2-hop chain
    assertThat((String) aaa.get("name")).isEqualTo("three");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void multiHopChainedPatternReturnsAllVariables() {
    // Test returning all three variables to ensure correct binding
    final ResultSet result = database.query("opencypher",
        "MATCH (a:A {name:'one'})-[:EDG]->(aa:A)-[:EDG]->(aaa:A) RETURN a, aa, aaa");

    assertThat(result.hasNext()).as("Should have at least one result").isTrue();
    final Result row = result.next();

    final Vertex a = (Vertex) row.getProperty("a");
    final Vertex aa = (Vertex) row.getProperty("aa");
    final Vertex aaa = (Vertex) row.getProperty("aaa");

    assertThat(a).isNotNull();
    assertThat(aa).isNotNull();
    assertThat(aaa).isNotNull();

    assertThat((String) a.get("name")).isEqualTo("one");
    assertThat((String) aa.get("name")).isEqualTo("two");
    assertThat((String) aaa.get("name")).isEqualTo("three");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void singleHopPattern() {
    // Verify single hop works correctly
    final ResultSet result = database.query("opencypher",
        "MATCH (a:A {name:'one'})-[:EDG]->(aa:A) RETURN aa");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    // Single variable returns are returned as elements via toElement()
    Vertex aa;
    if (row.isElement()) {
      aa = (Vertex) row.toElement();
    } else {
      aa = (Vertex) row.getProperty("aa");
    }

    assertThat((String) aa.get("name")).isEqualTo("two");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void threeHopChainedPattern() {
    // Add a fourth vertex to test longer chains
    database.transaction(() -> {
      final ResultSet findThree = database.query("opencypher", "MATCH (v:A {name:'three'}) RETURN v");
      final Result threeResult = findThree.next();
      // Single variable returns are returned as elements
      final Vertex three = threeResult.isElement() ? (Vertex) threeResult.toElement() : (Vertex) threeResult.getProperty("v");
      final Vertex four = database.newVertex("A").set("name", "four").save();
      three.newEdge("EDG", four, true, (Object[]) null).save();
    });

    // Test 3-hop chain
    final ResultSet result = database.query("opencypher",
        "MATCH (a:A {name:'one'})-[:EDG]->(b:A)-[:EDG]->(c:A)-[:EDG]->(d:A) RETURN a, b, c, d");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    final Vertex a = (Vertex) row.getProperty("a");
    final Vertex b = (Vertex) row.getProperty("b");
    final Vertex c = (Vertex) row.getProperty("c");
    final Vertex d = (Vertex) row.getProperty("d");

    assertThat((String) a.get("name")).isEqualTo("one");
    assertThat((String) b.get("name")).isEqualTo("two");
    assertThat((String) c.get("name")).isEqualTo("three");
    assertThat((String) d.get("name")).isEqualTo("four");

    assertThat(result.hasNext()).isFalse();
  }
}
