package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.*;

import static com.arcadedb.graph.Vertex.DIRECTION.IN;
import static com.arcadedb.graph.Vertex.DIRECTION.OUT;
import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDatabaseJavaApiIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createUnidirectionalEdge() {
    assertThat(
        new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(DATABASE_NAME)).isTrue();

    ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_RETRIES, 3);
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, configuration);

    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE FriendOf UNIDIRECTIONAL");

    MutableVertex me = database.newVertex("Person").set("name", "me").save();
    MutableVertex you = database.newVertex("Person").set("name", "you").save();

    MutableEdge friendOf = me.newEdge("FriendOf", you).save();

    assertThat(friendOf.getOut()).isEqualTo(me);
    assertThat(friendOf.getIn()).isEqualTo(you);

    assertThat(me.getVertices(OUT).iterator().hasNext()).isTrue();
    assertThat(me.getVertices(OUT).iterator().next()).isEqualTo(you);
    assertThat(me.getVertices(IN).iterator().hasNext()).isFalse();

    assertThat(you.getVertices(IN).iterator().hasNext()).isTrue();
    assertThat(you.getVertices(OUT).iterator().hasNext()).isFalse();

    Iterable<Edge> friends = me.getEdges(IN, "FriendOf");
    assertThat(friends).containsExactly(friendOf);

    assertThat(me.getString("name")).isEqualTo("me");
    assertThat(you.getString("name")).isEqualTo("you");

    me.reload();
    you.reload();

    assertThat(me.getString("name")).isEqualTo("me");
    assertThat(you.getString("name")).isEqualTo("you");

    database.close();
  }

  @Test
  public void test2() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    assertThat(
        new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(DATABASE_NAME)).isTrue();

    String schema = """
            create vertex type Customer if not exists;
            create property Customer.name if not exists string;
            create property Customer.surname if not exists string;
            create index if not exists on Customer (name, surname) unique;
        """;

    database.begin();
    database.command("sqlscript", schema);
    database.commit();

    database.transaction(() -> {
      database.acquireLock().type("Customer").lock();
      database.newVertex("Customer")
          .set("name", "John")
          .set("surname", "Doe")
          .save();
    });

    database.begin();
    database.acquireLock().type("Customer").lock();
    database.commit();

    database.close();
  }

  @Test
  public void testExplicitLock() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    final int TOT = 100;

    database.getSchema().getOrCreateVertexType("Node");

    final AtomicInteger committed = new AtomicInteger(0);
    final AtomicInteger caughtExceptions = new AtomicInteger(0);

    final RID[] rid = new RID[1];

    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Node");
      v.set("id", 0);
      v.set("name", "Exception(al)");
      v.set("surname", "Test");
      v.save();
      rid[0] = v.getIdentity();
    });

    final int CONCURRENT_THREADS = 16;

    // SPAWN ALL THE THREADS AND INCREMENT ONE BY ONE THE ID OF THE VERTEX
    final Thread[] threads = new Thread[CONCURRENT_THREADS];
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      threads[i] = new Thread(() -> {
        final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

        for (int k = 0; k < TOT; ++k) {
          try {
            db.transaction(() -> {
              db.acquireLock().type("Node").lock();

              final MutableVertex v = db.lookupByRID(rid[0]).asVertex().modify();
              v.set("id", v.getInteger("id") + 1);
              v.save();
            });

            committed.incrementAndGet();

          } catch (Exception e) {
            caughtExceptions.incrementAndGet();
            e.printStackTrace();
          }
        }

        db.close();

      });
      threads[i].start();
    }

    // WAIT FOR ALL THE THREADS
    for (int i = 0; i < CONCURRENT_THREADS; i++)
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        // IGNORE IT
      }

    assertThat(database.countType("Node", true)).isEqualTo(1);

    assertThat(rid[0].asVertex().getInteger("id")).isEqualTo(CONCURRENT_THREADS * TOT);
    assertThat(committed.get()).isEqualTo(CONCURRENT_THREADS * TOT);
    assertThat(caughtExceptions.get()).isEqualTo(0);
    assertThat(committed.get() + caughtExceptions.get()).isEqualTo(TOT * CONCURRENT_THREADS);

    database.close();
  }

  @Test
  void saveRemoteEdgeOnGivenBucket() {
    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    Bucket edgeBucket = db.getSchema().createBucket("EdgeBucket");
    db.getSchema().createEdgeType("FriendOf").addBucket(edgeBucket);
    db.getSchema().createVertexType("Person");

    MutableVertex me = db.newVertex("Person").set("name", "me").save();
    MutableVertex you = db.newVertex("Person").set("name", "you").save();
    MutableEdge friendOf = me.newEdge("bucket:EdgeBucket", you);

    assertThat(friendOf.getOut()).isEqualTo(me.getIdentity());
    assertThat(friendOf.getIn()).isEqualTo(you.getIdentity());

  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

}
