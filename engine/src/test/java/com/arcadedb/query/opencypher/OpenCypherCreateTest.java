package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CREATE clause in OpenCypher queries.
 */
public class OpenCypherCreateTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-create").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testCreateSingleVertex() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "CREATE (n:Person {name: 'Alice', age: 30})");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Object vertex = r.getProperty("n");
      assertThat(vertex).isInstanceOf(Vertex.class);

      final Vertex v = (Vertex) vertex;
      assertThat(v.getTypeName()).isEqualTo("Person");
      assertThat((String) v.get("name")).isEqualTo("Alice");
      assertThat((Integer) v.get("age")).isEqualTo(30);
    });

    // Verify vertex was persisted
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void testCreateMultipleVertices() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob'})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie'})");
    });

    // Verify all vertices were created
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  void testCreateVertexWithReturn() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n.name, n.age");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      assertThat((String) r.getProperty("n.name")).isEqualTo("Alice");
      assertThat((Integer) r.getProperty("n.age")).isEqualTo(30);
    });
  }

  @Test
  void testCreateRelationship() {
    database.transaction(() -> {
      // First create two vertices
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob'})");
    });

    database.transaction(() -> {
      // Then create a relationship between them
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[r:KNOWS]->(b) RETURN r");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Object edge = r.getProperty("r");
      assertThat(edge).isInstanceOf(Edge.class);

      final Edge e = (Edge) edge;
      assertThat(e.getTypeName()).isEqualTo("KNOWS");
    });

    // Verify relationship was persisted
    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void testCreatePathWithProperties() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[r:WORKS_AT {since: 2020}]->(c:Company {name: 'ArcadeDB'}) RETURN a, r, c");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result res = result.next();

      // Check person
      final Vertex person = (Vertex) res.getProperty("a");
      assertThat(person).isNotNull();
      assertThat((String) person.get("name")).isEqualTo("Alice");

      // Check relationship
      final Edge worksAt = (Edge) res.getProperty("r");
      assertThat(worksAt).isNotNull();
      assertThat((Integer) worksAt.get("since")).isEqualTo(2020);

      // Check company
      final Vertex company = (Vertex) res.getProperty("c");
      assertThat(company).isNotNull();
      assertThat((String) company.get("name")).isEqualTo("ArcadeDB");
    });
  }

  @Test
  void testCreateChainedPath() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'}) RETURN a, b, c");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result res = result.next();

      final Vertex alice = (Vertex) res.getProperty("a");
      final Vertex bob = (Vertex) res.getProperty("b");
      final Vertex charlie = (Vertex) res.getProperty("c");

      assertThat((String) alice.get("name")).isEqualTo("Alice");
      assertThat((String) bob.get("name")).isEqualTo("Bob");
      assertThat((String) charlie.get("name")).isEqualTo("Charlie");
    });

    // Verify the chain was created
    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c:Person {name: 'Charlie'}) RETURN b");
    assertThat(verify.hasNext()).isTrue();
    final Vertex bob = (Vertex) verify.next().getProperty("b");
    assertThat((String) bob.get("name")).isEqualTo("Bob");
  }

  @Test
  void testCreateWithoutLabel() {
    database.getSchema().createVertexType("Vertex");

    database.transaction(() -> {
      // Create vertex without label (should default to "Vertex")
      final ResultSet result = database.command("opencypher", "CREATE (n {name: 'Test'}) RETURN n");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Vertex v = (Vertex) r.getProperty("n");
      assertThat(v.getTypeName()).isEqualTo("Vertex");
      assertThat((String) v.get("name")).isEqualTo("Test");
    });
  }

  @Test
  void testCreateWithMatchContext() {
    // Create a person first
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
    });

    // Match the person and create a relationship to a new person
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, r, b");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result res = result.next();

      final Vertex alice = (Vertex) res.getProperty("a");
      final Edge knows = (Edge) res.getProperty("r");
      final Vertex bob = (Vertex) res.getProperty("b");

      assertThat((String) alice.get("name")).isEqualTo("Alice");
      assertThat(knows.getTypeName()).isEqualTo("KNOWS");
      assertThat((String) bob.get("name")).isEqualTo("Bob");
    });
  }

  @Test
  void testCreateMultiplePathsInOneQuery() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) RETURN a, b");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result res = result.next();

      final Vertex alice = (Vertex) res.getProperty("a");
      final Vertex bob = (Vertex) res.getProperty("b");

      assertThat((String) alice.get("name")).isEqualTo("Alice");
      assertThat((String) bob.get("name")).isEqualTo("Bob");
    });

    // Verify both vertices were created
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }
}
