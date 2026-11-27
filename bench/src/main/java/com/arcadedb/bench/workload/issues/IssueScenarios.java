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
package com.arcadedb.bench.workload.issues;

import com.arcadedb.bench.runner.Progress;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.bench.workload.issues.IssueScenario.Statement;
import com.arcadedb.remote.RemoteDatabase;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;

import static com.arcadedb.bench.workload.issues.IssueScenario.Statement.cypher;
import static com.arcadedb.bench.workload.issues.IssueScenario.Statement.sql;

/**
 * The ten scenarios of the issues workload, in the order of the plan's table ("Measured on main ..."): statements, reference
 * forms, limits and datasets are the table's. The sizes at S and up are the table's datasets; XS only proves that every case
 * runs, and its slow forms stay under the 5 ms below which a ratio is not judged.
 */
public final class IssueScenarios {
  /** Statements per script of a load: one transaction each. */
  private static final int LOAD_BATCH = 2_000;

  @FunctionalInterface
  interface Row {
    void append(StringBuilder script, int i);
  }

  private IssueScenarios() {
  }

  public static List<IssueScenario> all() {
    return List.of(issue12725(), issue12713(), issue12281(), issue11963(), issue12522(), issue12141(), issue11701(), issue11871(),
        issue11682(), issue11700());
  }

  static int pick(final Scale scale, final int xs, final int s, final int m, final int l) {
    return switch (scale) {
      case XS -> xs;
      case S -> s;
      case M -> m;
      case L -> l;
    };
  }

  // ---- #12725: openCypher IN [100 literals] against IN $list (the literal list was re-evaluated for every row)

  static int rows12725(final Scale scale) {
    return pick(scale, 2_000, 50_000, 100_000, 200_000);
  }

  static RatioScenario issue12725() {
    final StringBuilder literals = new StringBuilder();
    final List<String> values = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      if (i > 0)
        literals.append(", ");
      literals.append("'name").append(i).append('\'');
      values.add("name" + i);
    }
    // 50 rows per name, so the 100 names match 5,000 rows at every scale from S: an answer well under the 20,000 buffered rows
    return new RatioScenario(12725, 3.0, List.of(cypher("MATCH (n:T) WHERE n.k IN [" + literals + "] RETURN n.k")),
        List.of(cypher("MATCH (n:T) WHERE n.k IN $list RETURN n.k")), (scale, seed) -> List.of(Map.of("list", values)),
        (db, scale, seed) -> {
          final int n = rows12725(scale);
          final int domain = Math.max(100, n / 50);
          schema(db, "CREATE VERTEX TYPE T", "CREATE PROPERTY T.k STRING");
          insert(db, n, (b, i) -> b.append("INSERT INTO T SET k = 'name").append(i % domain).append("';\n"));
        });
  }

  // ---- #12713: SQL DATETIME against a string literal (the literal was parsed for every record)

  static final LocalDateTime CUT_12713 = LocalDateTime.of(2024, 6, 1, 0, 0);

  static RatioScenario issue12713() {
    // the reference binds the instant as a LocalDateTime parameter: date('<const>', '<fmt>') is itself evaluated per record today
    return new RatioScenario(12713, 2.5, List.of(sql("SELECT count(*) AS n FROM Event WHERE ts > '2024-06-01 00:00:00'")),
        List.of(sql("SELECT count(*) AS n FROM Event WHERE ts > :d")), (scale, seed) -> List.of(Map.of("d", CUT_12713)),
        (db, scale, seed) -> {
          final int n = pick(scale, 2_000, 100_000, 400_000, 1_000_000);
          final long from = LocalDateTime.of(2024, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
          final long step = 366L * 24 * 3600 * 1000 / n;
          schema(db, "CREATE DOCUMENT TYPE Event", "CREATE PROPERTY Event.ts DATETIME", "CREATE PROPERTY Event.val LONG");
          insert(db, n, (b, i) -> b.append("INSERT INTO Event SET ts = ").append(from + i * step).append(", val = ").append(i)
              .append(";\n"));
        }).hintWhenReferenceCountsZero("the server likely predates the typed $localDateTime parameter of the remote client (#12187),"
        + " so it bound the reference's LocalDateTime as a map");
  }

  // ---- #12281: value-first and NOT comparisons on an indexed id (they planned a type scan)

  static RatioScenario issue12281() {
    return new RatioScenario(12281, 10.0,
        List.of(sql("SELECT id FROM T WHERE 5 = id"), sql("SELECT id FROM T WHERE :lo < id"), sql("SELECT id FROM T WHERE NOT (id >= 3)")),
        List.of(sql("SELECT id FROM T WHERE id = 5"), sql("SELECT id FROM T WHERE id > :lo"), sql("SELECT id FROM T WHERE id < 3")),
        (scale, seed) -> List.of(Map.of("lo", rows12281(scale) - 10)), (db, scale, seed) -> {
          schema(db, "CREATE VERTEX TYPE T", "CREATE PROPERTY T.id INTEGER", "CREATE INDEX ON T (id) UNIQUE");
          insert(db, rows12281(scale), (b, i) -> b.append("INSERT INTO T SET id = ").append(i).append(";\n"));
        });
  }

  static int rows12281(final Scale scale) {
    return pick(scale, 2_000, 200_000, 400_000, 1_000_000);
  }

  // ---- #11963: a UNIQUE key AND a NOTUNIQUE low-selectivity tenant (the plan picked the tenant's index)

  static int rows11963(final Scale scale) {
    return pick(scale, 2_000, 50_000, 200_000, 1_000_000);
  }

  static RatioScenario issue11963() {
    return new RatioScenario(11963, 10.0, List.of(sql("SELECT unique_key FROM E WHERE unique_key = :k AND tenant = :t")),
        List.of(sql("SELECT unique_key FROM E WHERE unique_key = :k")), (scale, seed) -> {
          final int n = rows11963(scale);
          final List<Map<String, Object>> params = new ArrayList<>();
          // 16 keys spread over the type, all in the big tenant
          for (int j = 0; j < 16; j++) {
            int i = (int) ((long) n * j / 16);
            if (!bigTenant(i))
              i++;
            params.add(Map.of("k", key11963(i), "t", "big"));
          }
          return params;
        }, (db, scale, seed) -> {
          schema(db, "CREATE VERTEX TYPE E BUCKETS 8", "CREATE PROPERTY E.unique_key STRING", "CREATE PROPERTY E.tenant STRING",
              "CREATE INDEX ON E (unique_key) UNIQUE", "CREATE INDEX ON E (tenant) NOTUNIQUE");
          insert(db, rows11963(scale), (b, i) -> b.append("INSERT INTO E SET unique_key = '").append(key11963(i))
              .append("', tenant = '").append(bigTenant(i) ? "big" : "small").append("';\n"));
        });
  }

  // six records in seven are in one tenant
  private static boolean bigTenant(final int i) {
    return i % 7 != 6;
  }

  private static String key11963(final int i) {
    return "k" + i;
  }

  // ---- #12522: openCypher unlabelled node pinned by an indexed property (it walked every vertex)

  static final String[] TYPES_12522 = { "Entity", "Episodic", "Community", "Saga" };

  static int entities12522(final Scale scale) {
    return pick(scale, 2_000, 50_000, 200_000, 1_000_000);
  }

  static int others12522(final Scale scale) {
    return pick(scale, 200, 2_000, 8_000, 40_000);
  }

  /** The uuid of record {@code i} of type number {@code t}: random, but the same for the same seed. */
  static String uuid12522(final long seed, final int t, final int i) {
    final SplittableRandom r = new SplittableRandom(seed * 31 + t * 1_000_003L + i);
    return new UUID(r.nextLong(), r.nextLong()).toString();
  }

  static RatioScenario issue12522() {
    return new RatioScenario(12522, 20.0, List.of(cypher("MATCH (n {uuid: $u}) RETURN n.uuid")),
        List.of(cypher("MATCH (n:Entity {uuid: $u}) RETURN n.uuid")), (scale, seed) -> {
          final int n = entities12522(scale);
          final List<Map<String, Object>> params = new ArrayList<>();
          for (int j = 0; j < 16; j++)
            params.add(Map.of("u", uuid12522(seed, 0, (int) ((long) n * j / 16))));
          return params;
        }, (db, scale, seed) -> {
          // the issue's schema: an index on uuid in every vertex type, created with the openCypher syntax, the property not declared
          for (final String type : TYPES_12522) {
            db.command("sql", "CREATE VERTEX TYPE " + type).close();
            db.command("opencypher", "CREATE INDEX FOR (n:" + type + ") ON (n.uuid)").close();
          }
          for (int t = 0; t < TYPES_12522.length; t++) {
            final int type = t;
            insert(db, t == 0 ? entities12522(scale) : others12522(scale), (b, i) -> b.append("INSERT INTO ")
                .append(TYPES_12522[type]).append(" SET uuid = '").append(uuid12522(seed, type, i)).append("';\n"));
          }
        });
  }

  // ---- #12141: openCypher WITH d ORDER BY d.key LIMIT k against the RETURN form (the WITH form sorted the whole type)

  static RatioScenario issue12141() {
    return new RatioScenario(12141, 4.0, List.of(cypher("MATCH (d:D) WITH d ORDER BY d.key LIMIT 10 RETURN d.key")),
        List.of(cypher("MATCH (d:D) RETURN d.key ORDER BY d.key LIMIT 10")), (scale, seed) -> List.of(Map.of()), (db, scale, seed) -> {
          schema(db, "CREATE VERTEX TYPE D", "CREATE PROPERTY D.key STRING", "CREATE INDEX ON D (key) UNIQUE");
          insert(db, pick(scale, 2_000, 100_000, 400_000, 1_000_000),
              (b, i) -> b.append("INSERT INTO D SET key = '").append(String.format("k%07d", i)).append("';\n"));
        });
  }

  // ---- #11701: SQL ORDER BY an indexed key LIMIT n over a supertype (it scanned and sorted)

  static RatioScenario issue11701() {
    return new RatioScenario(11701, 5.0, List.of(sql("SELECT k FROM Rec ORDER BY k ASC LIMIT 10")),
        List.of(sql("SELECT k FROM Rec WHERE k >= 0 ORDER BY k ASC LIMIT 10")), (scale, seed) -> List.of(Map.of()),
        (db, scale, seed) -> {
          schema(db, "CREATE VERTEX TYPE Rec", "CREATE PROPERTY Rec.k LONG (MANDATORY true, NOTNULL true)",
              "CREATE INDEX ON Rec (k) UNIQUE", "CREATE VERTEX TYPE SubRec EXTENDS Rec");
          insert(db, pick(scale, 2_000, 400_000, 1_000_000, 2_000_000),
              (b, i) -> b.append("INSERT INTO SubRec SET k = ").append(i).append(";\n"));
        });
  }

  // ---- #11871: SQL min() over a range under LIMIT or TIMEOUT (the clause disabled the ordered fetch)

  static int rows11871(final Scale scale) {
    return pick(scale, 2_000, 400_000, 1_000_000, 2_000_000);
  }

  static RatioScenario issue11871() {
    final String plain = "SELECT min(k) AS m FROM T WHERE k > :lo";
    return new RatioScenario(11871, 5.0, List.of(sql(plain + " LIMIT 10"), sql(plain + " TIMEOUT 60000 EXCEPTION")),
        List.of(sql(plain), sql(plain)), (scale, seed) -> List.of(Map.of("lo", rows11871(scale) / 2)), (db, scale, seed) -> {
          schema(db, "CREATE VERTEX TYPE T", "CREATE PROPERTY T.k INTEGER", "CREATE INDEX ON T (k) UNIQUE");
          insert(db, rows11871(scale), (b, i) -> b.append("INSERT INTO T SET k = ").append(i).append(";\n"));
        });
  }

  // ---- #11682: openCypher WHERE n.key IN $ids with an index seek (quadratic in |ids|)

  static int[] ids11682(final Scale scale) {
    return scale == Scale.XS ? new int[] { 250, 500, 1_000 } : new int[] { 4_000, 8_000, 16_000 };
  }

  static int rows11682(final Scale scale) {
    return pick(scale, 2_000, 32_000, 128_000, 512_000);
  }

  static ExponentScenario issue11682() {
    // a count, so the answer is one row at every size: |ids| rows over HTTP would make the cost linear by serialization alone
    final Statement statement = cypher("MATCH (n:L) WHERE n.key IN $ids RETURN count(n) AS n");
    return new ExponentScenario(11682, 1.3, (scale, seed) -> {
      final int n = rows11682(scale);
      final List<ExponentScenario.Point> points = new ArrayList<>();
      for (final int size : ids11682(scale)) {
        // existing keys spread over the whole type
        final List<Long> ids = new ArrayList<>(size);
        final int stride = n / size;
        for (int i = 0; i < size; i++)
          ids.add((long) i * stride);
        points.add(new ExponentScenario.Point(size, statement, Map.of("ids", ids), size, true));
      }
      return points;
    }, (db, scale, seed) -> {
      schema(db, "CREATE VERTEX TYPE L", "CREATE PROPERTY L.key LONG", "CREATE INDEX ON L (key) UNIQUE");
      insert(db, rows11682(scale), (b, i) -> b.append("INSERT INTO L SET key = ").append(i).append(";\n"));
    });
  }

  // ---- #11700: SQL keyset page on a common indexed equality (the page read the whole equality group)

  static final int PAGE_11700 = 100;

  /**
   * The sizes of the sweep, n/4, n/2 and n, one type each; the group of the page holds a tenth of every type. The page starts
   * after key n/2 of its type, so it is full (exactly {@value #PAGE_11700} rows) when the group holds 100 keys after the middle:
   * size/2/10 >= 100, size >= 2,000. XS (2k, 4k, 8k) is the smallest sweep that keeps every page full; at 2k the page is the
   * last 100 keys of the group (1003..1993), exactly.
   */
  static int[] sizes11700(final Scale scale) {
    final int n = pick(scale, 8_000, 640_000, 1_280_000, 2_560_000);
    return new int[] { n / 4, n / 2, n };
  }

  static ExponentScenario issue11700() {
    return new ExponentScenario(11700, 0.55, (scale, seed) -> {
      final int[] sizes = sizes11700(scale);
      final List<ExponentScenario.Point> points = new ArrayList<>();
      for (int t = 0; t < sizes.length; t++)
        // :after in the middle of the group: the page is the next 100 keys of g3 after it
        points.add(new ExponentScenario.Point(sizes[t], sql("SELECT external_id FROM Chunk" + t
            + " WHERE graph_name = :v AND external_id > :after ORDER BY external_id LIMIT " + PAGE_11700),
            Map.of("v", "g3", "after", key11700(sizes[t] / 2)), PAGE_11700, false));
      return points;
    }, (db, scale, seed) -> {
      final int[] sizes = sizes11700(scale);
      for (int t = 0; t < sizes.length; t++) {
        final String type = "Chunk" + t;
        schema(db, "CREATE VERTEX TYPE " + type, "CREATE PROPERTY " + type + ".external_id STRING",
            "CREATE PROPERTY " + type + ".graph_name STRING", "CREATE INDEX ON " + type + " (external_id) UNIQUE",
            "CREATE INDEX ON " + type + " (graph_name) NOTUNIQUE");
        insert(db, sizes[t], (b, i) -> b.append("INSERT INTO ").append(type).append(" SET external_id = '").append(key11700(i))
            .append("', graph_name = 'g").append(i % 10).append("';\n"));
      }
    });
  }

  static String key11700(final int i) {
    return String.format("c%08d", i);
  }

  // ---- loading

  static void schema(final RemoteDatabase db, final String... statements) {
    for (final String statement : statements)
      db.command("sql", statement).close();
  }

  /** Inserts rows {@code 0..n-1} by scripts of {@value #LOAD_BATCH} statements, one transaction each. */
  static void insert(final RemoteDatabase db, final int n, final Row row) {
    final StringBuilder script = new StringBuilder(LOAD_BATCH * 80);
    for (int from = 0; from < n; from += LOAD_BATCH) {
      script.setLength(0);
      script.append("BEGIN;\n");
      final int to = Math.min(from + LOAD_BATCH, n);
      for (int i = from; i < to; i++)
        row.append(script, i);
      script.append("COMMIT;");
      db.command("sqlscript", script.toString()).close();
      Progress.advance();
    }
  }
}
