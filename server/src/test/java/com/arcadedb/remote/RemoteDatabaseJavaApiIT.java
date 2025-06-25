package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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

    assertThat(me.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
    assertThat(me.getVertices(Vertex.DIRECTION.OUT).iterator().next()).isEqualTo(you);
    assertThat(me.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isFalse();

    assertThat(you.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isTrue();
    assertThat(you.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isFalse();

    Iterable<Edge> friends = me.getEdges(Vertex.DIRECTION.IN, "FriendOf");
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
  public void testExplicitLock() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    final int TOT = 500;

    database.getSchema().getOrCreateVertexType("Node");
    database.getSchema().getOrCreateEdgeType("Arc");

    final AtomicInteger committed = new AtomicInteger(0);
    final AtomicInteger caughtExceptions = new AtomicInteger(0);

    final RID[] rid = new RID[2];

    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Node");
      v.set("id", 0);
      v.set("name", "Exception(al)");
      v.set("surname", "Test");
      v.save();
      rid[0] = v.getIdentity();
    });
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Node");
      v.set("id", 1);
      v.set("name", "Exception(al)");
      v.set("surname", "Test");
      v.save();
      rid[1] = v.getIdentity();
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
              db.acquireLock()
                  .type("Node")
                  .type("Arc")
                  .lock();

              final MutableVertex v = db.lookupByRID(rid[0]).asVertex().modify();
              v.set("id", v.getInteger("id") + 1);
              v.save();
              db.command("sql", "CREATE EDGE Arc FROM " + rid[0] + " TO " + rid[1]);
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

    assertThat(database.countType("Node", true)).isEqualTo(2);
    assertThat(database.countType("Arc", true)).isEqualTo(TOT * CONCURRENT_THREADS);

    assertThat(rid[0].asVertex().getInteger("id")).isEqualTo(CONCURRENT_THREADS * TOT);
    assertThat(committed.get()).isEqualTo(CONCURRENT_THREADS * TOT);
    assertThat(caughtExceptions.get()).isEqualTo(0);
    assertThat(committed.get() + caughtExceptions.get()).isEqualTo(TOT * CONCURRENT_THREADS);

    database.close();
  }

  @Test
  void loadTest() throws InterruptedException {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.getSchema().getOrCreateVertexType("Node");
    database.getSchema().getOrCreateEdgeType("Arc");

    Supplier<Integer> idSupplier = new Supplier<>() {
      private final AtomicInteger id = new AtomicInteger(0);

      @Override
      public Integer get() {
        return id.getAndIncrement();
      }
    };

    ExecutorService executor = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 5; i++) {
      // CREATE 1000 VERTICES IN PARALLEL
      executor.submit(() -> {
            final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
                BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
            for (int j = 0; j < 1000; j++) {
              try {
                db.transaction(() -> {
//                db.acquireLock()
//                    .type("Node")
//                    .lock();
                  db.command("sql", "CREATE VERTEX Node SET id = ?", idSupplier.get());
                }, false, 30);
                if (j % 100 == 0) {
                  System.out.println("Created " + j + " vertices");
                }
              } catch (Exception e) {
                System.out.println("Error creating vertex: " + e.getMessage());
              }
            }
          }
      );

      TimeUnit.SECONDS.sleep(1);

      executor.submit(() -> {
        final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
        for (int j = 0; j < 1000; j++) {
          try {
            db.transaction(() -> {
//            db.acquireLock()
//                .type("Node")
//                .type("Arc")
//                .lock();
              db.command("sql",
                  "CREATE EDGE Arc FROM (SELECT FROM Node WHERE id = ?) TO (SELECT FROM Node WHERE id = ?)", idSupplier.get() % 10,
                  idSupplier.get() % 10);

            }, false, 30);
            if (j % 100 == 0) {
              System.out.println("Created " + j + " edges");
            }
          } catch (Exception e) {
            System.out.println("Error creating edge: " + e.getMessage());
          }
        }
      });

    }

    executor.shutdown();
    while (!executor.isTerminated()) {
      System.out.println("Current Nodes = " + database.countType("Node", true));
      System.out.println("Current Arcs = " + database.countType("Arc", true));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    System.out.println("Current Nodes = " + database.countType("Node", true));
    System.out.println("Current Arcs = " + database.countType("Arc", true));

    assertThat(database.countType("Node", true)).isEqualTo(5 * 1000);
    assertThat(database.countType("Arc", true)).isEqualTo(5 * 1000);
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
