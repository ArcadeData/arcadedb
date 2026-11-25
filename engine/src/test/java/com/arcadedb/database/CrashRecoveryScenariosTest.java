/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.CrashingChildJvm;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Each scenario runs its writes in a child JVM that then ends with {@code Runtime.halt()}, what a process kill leaves on disk; the
 * database is opened again here and must hold exactly the committed work: every committed change, nothing of a transaction still
 * open at the kill, and indexes that agree with the records ({@link TestHelper}'s integrity check runs after every scenario).
 */
@Tag("slow")
class CrashRecoveryScenariosTest extends TestHelper {
  private static final int CHILD_TIMEOUT_SECONDS = 300;

  enum Scenario {
    BIG_TRANSACTION(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", String.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 100_000; i++)
          database.newDocument("T").set("k", "k" + i).set("v", i).save();
        database.command("sql", "DELETE FROM T WHERE v % 2 = 0");
      });
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(50_000);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 'k2'")).isZero();
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 'k3'")).isEqualTo(1);
    }),

    MANY_SMALL_TRANSACTIONS(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      for (int i = 0; i < 5_000; i++) {
        final int k = i;
        database.transaction(() -> database.newDocument("T").set("k", k).save());
      }
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(5_000);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k >= 4990")).isEqualTo(10);
    }),

    MANY_UPDATES_OF_ONE_RECORD(database -> {
      database.getSchema().createDocumentType("T").createProperty("v", Integer.class);
      database.command("sql", "CREATE INDEX ON T (v) NOTUNIQUE");
      database.transaction(() -> database.newDocument("T").set("id", 1).set("v", 0).save());
      for (int i = 1; i <= 5_000; i++) {
        final int v = i;
        database.transaction(() -> database.command("sql", "UPDATE T SET v = ? WHERE id = 1", v));
      }
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(1);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE v = 5000")).isEqualTo(1);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE v < 5000")).isZero();
    }),

    INSERT_UPDATE_DELETE(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.command("sql", "CREATE PROPERTY T.g STRING");
      database.command("sql", "CREATE INDEX ON T (g) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument("T").set("k", i).set("g", "g" + (i % 7)).save();
      });
      database.transaction(() -> database.command("sql", "UPDATE T SET g = 'moved' WHERE k % 3 = 0"));
      database.transaction(() -> database.command("sql", "DELETE FROM T WHERE k % 5 = 0"));
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(2_400);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE g = 'moved'")).isEqualTo(800);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 15")).isZero();
    }),

    OPEN_TRANSACTION_AT_THE_KILL(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument("T").set("k", i).save();
      });
      // left open: none of it may survive
      database.begin();
      for (int i = 1_000; i < 2_000; i++)
        database.newDocument("T").set("k", i).save();
      database.command("sql", "DELETE FROM T WHERE k < 500");
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(1_000);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k < 500")).isEqualTo(500);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k >= 1000")).isZero();
    }),

    DROP_AND_RECREATE_TYPE(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument("T").set("k", i).save();
      });
      database.command("sql", "DROP TYPE T UNSAFE");
      database.getSchema().createDocumentType("T").createProperty("k", String.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("T").set("k", "new" + i).save();
      });
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(10);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 'new3'")).isEqualTo(1);
    }),

    RENAME_TYPE_WITH_DATA(database -> {
      database.getSchema().createDocumentType("Src").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON Src (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument("Src").set("k", i).save();
      });
      database.command("sql", "ALTER TYPE Src NAME Dst");
      database.transaction(() -> database.newDocument("Dst").set("k", 1_000).save());
    }, database -> {
      assertThat(database.getSchema().existsType("Src")).isFalse();
      assertThat(count(database, "SELECT count(@rid) AS c FROM Dst")).isEqualTo(1_001);
      assertThat(count(database, "SELECT count(*) AS c FROM Dst WHERE k = 1000")).isEqualTo(1);
    }),

    RENAME_THEN_RECREATE_THE_OLD_NAME(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.newDocument("T").set("k", i).save();
      });
      database.command("sql", "ALTER TYPE T NAME Old");
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.transaction(() -> database.newDocument("T").set("k", -1).save());
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM Old")).isEqualTo(100);
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(1);
    }),

    CREATE_INDEXES_ON_POPULATED_TYPE(database -> {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> {
        for (int i = 0; i < 20_000; i++)
          database.newDocument("T").set("k", i).set("g", "g" + (i % 13)).set("t", "word" + (i % 17) + " other").save();
      });
      database.command("sql", "CREATE PROPERTY T.k INTEGER");
      database.command("sql", "CREATE PROPERTY T.g STRING");
      database.command("sql", "CREATE PROPERTY T.t STRING");
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.command("sql", "CREATE INDEX ON T (g, k) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON T (t) FULL_TEXT");
    }, database -> {
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 777")).isEqualTo(1);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE g = 'g3'")).isEqualTo(1_539);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE SEARCH_INDEX('T[t]', 'word5') = true")).isEqualTo(1_177);
      // the unique index still refuses a duplicate
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("k", 777).save()))
          .isInstanceOf(DuplicatedKeyException.class);
    }),

    DROP_INDEXES(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument("T").set("k", i % 10).save();
      });
      database.command("sql", "DROP INDEX `T[k]`");
      database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
      database.command("sql", "DROP INDEX `T[k]`");
    }, database -> {
      assertThat(database.getSchema().existsIndex("T[k]")).isFalse();
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 4")).isEqualTo(100);
    }),

    TRUNCATE_THEN_REFILL(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          database.newDocument("T").set("k", i).save();
      });
      database.command("sql", "TRUNCATE TYPE T UNSAFE");
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.newDocument("T").set("k", i).save();
      });
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(100);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 4000")).isZero();
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k = 99")).isEqualTo(1);
    }),

    GRAPH_WITH_A_HUB(database -> {
      database.getSchema().createVertexType("V").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON V (k) UNIQUE");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("V").set("k", -1).save();
        for (int i = 0; i < 6_000; i++)
          hub.newEdge("E", database.newVertex("V").set("k", i).save());
      });
      database.transaction(() -> database.command("sql", "DELETE FROM V WHERE k >= 0 AND k % 4 = 0"));
    }, database -> {
      final Vertex hub = database.query("sql", "SELECT FROM V WHERE k = -1").next().getVertex().orElseThrow();
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(4_500);
      assertThat(count(database, "SELECT count(@rid) AS c FROM E")).isEqualTo(4_500);
      assertThat(count(database, "SELECT count(@rid) AS c FROM V")).isEqualTo(4_501);
    }),

    LARGE_RECORDS(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newDocument("T").set("k", i).set("blob", "x".repeat(200_000 + i)).save();
      });
      database.transaction(() -> database.command("sql", "UPDATE T SET blob = blob + 'tail' WHERE k % 2 = 0"));
    }, database -> {
      // odd keys kept their content, even ones gained the tail
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k % 2 = 1 AND blob.length() = 200000 + k")).isEqualTo(25);
      assertThat(count(database, "SELECT count(*) AS c FROM T WHERE k % 2 = 0 AND blob.length() = 200004 + k")).isEqualTo(25);
    }),

    UNIQUE_CONSTRAINT_AFTER_RECOVERY(database -> {
      database.getSchema().createDocumentType("T").createProperty("k", String.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument("T").set("k", "k" + i).save();
      });
      database.transaction(() -> database.command("sql", "UPDATE T SET k = 'renamed' WHERE k = 'k10'"));
    }, database -> {
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("k", "k5").save()))
          .isInstanceOf(DuplicatedKeyException.class);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("k", "renamed").save()))
          .isInstanceOf(DuplicatedKeyException.class);
      // the old key of the renamed record is free again
      database.transaction(() -> database.newDocument("T").set("k", "k10").save());
      assertThat(count(database, "SELECT count(@rid) AS c FROM T")).isEqualTo(1_001);
    }),

    SEVERAL_TYPES_IN_ONE_TRANSACTION(database -> {
      for (final String type : List.of("A", "B", "C"))
        database.getSchema().createDocumentType(type).createProperty("k", Integer.class);
      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++) {
          database.newDocument("A").set("k", i).save();
          if (i % 2 == 0)
            database.newDocument("B").set("k", i).save();
          if (i % 3 == 0)
            database.newDocument("C").set("k", i).save();
        }
      });
    }, database -> {
      assertThat(count(database, "SELECT count(@rid) AS c FROM A")).isEqualTo(2_000);
      assertThat(count(database, "SELECT count(@rid) AS c FROM B")).isEqualTo(1_000);
      assertThat(count(database, "SELECT count(@rid) AS c FROM C")).isEqualTo(667);
    });

    private final Consumer<Database> setUp;
    private final Consumer<Database> verify;

    Scenario(final Consumer<Database> setUp, final Consumer<Database> verify) {
      this.setUp = setUp;
      this.verify = verify;
    }

    void setUp(final Database database) {
      setUp.accept(database);
    }

    void verify(final Database database) {
      verify.accept(database);
    }
  }

  /**
   * The child: runs the scenario's writes on the database at {@code args[1]}, then halts. With {@code args[2]} "unflushed" the
   * page flush is held for the whole scenario, so the data files hold none of its pages at the kill and only the WAL can bring
   * the committed work back.
   */
  public static void main(final String[] args) throws Exception {
    final Scenario scenario = Scenario.valueOf(args[0]);
    if ("unflushed".equals(args[2]))
      // the whole scenario's pages stay in memory: no cap, or the commits would wait for a flush that never comes
      GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM.setValue(0);
    final DatabaseFactory factory = new DatabaseFactory(args[1]);
    final Database database = factory.open();
    if ("unflushed".equals(args[2]))
      ((DatabaseInternal) database).getPageManager().suspendFlushAndExecute(database, () -> writeAndHalt(scenario, database));
    else
      writeAndHalt(scenario, database);
  }

  private static void writeAndHalt(final Scenario scenario, final Database database) {
    scenario.setUp(database);
    System.out.println("scenario " + scenario + " written");
    Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
  }

  /**
   * Scenarios that cannot run with the flush held: thousands of commits would keep every page version in memory, and renaming a
   * type waits for its pages to reach the disk.
   */
  private static final Set<Scenario> FLUSH_REQUIRED = EnumSet.of(Scenario.MANY_SMALL_TRANSACTIONS, Scenario.MANY_UPDATES_OF_ONE_RECORD,
      Scenario.RENAME_TYPE_WITH_DATA, Scenario.RENAME_THEN_RECREATE_THE_OLD_NAME);

  static Stream<Arguments> scenarios() {
    return Arrays.stream(Scenario.values()).flatMap(s -> FLUSH_REQUIRED.contains(s) ?
        Stream.of(Arguments.of(s, "flushed")) :
        Stream.of(Arguments.of(s, "flushed"), Arguments.of(s, "unflushed")));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("scenarios")
  void theDatabaseHoldsTheCommittedWorkAfterAKill(final Scenario scenario, final String pages) throws Exception {
    final String path = database.getDatabasePath();
    database.close();

    final CrashingChildJvm.Result child = CrashingChildJvm.run(CrashRecoveryScenariosTest.class, CHILD_TIMEOUT_SECONDS,
        scenario.name(), path, pages);
    assertThat(child.exitCode()).as("child output: %s", child.output()).isEqualTo(CrashingChildJvm.CRASH_EXIT_CODE);
    assertThat(child.output()).contains("scenario " + scenario + " written");

    database = factory.open();
    scenario.verify(database);
    // the integrity check after every test (TestHelper) compares the indexes with the records
  }

  private static long count(final Database database, final String sql) {
    try (final ResultSet rows = database.query("sql", sql)) {
      return rows.next().<Number>getProperty("c").longValue();
    }
  }
}
