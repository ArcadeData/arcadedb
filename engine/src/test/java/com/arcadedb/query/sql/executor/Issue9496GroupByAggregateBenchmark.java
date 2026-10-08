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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;

/**
 * Issue #9496: the TPC-H Q1 and top-parts aggregations of the report, and the variants it used to tell the cost of every
 * aggregate from the cost of the scan, on a {@code LineItem} type shaped like the report's (three buckets, the same
 * property types). The timings are logged at INFO.
 * <p>
 * {@code -Dissue9496.rows=N} sets the rows (1M by default), {@code -Dissue9496.keep=true} keeps the database between runs
 * so a second run skips the load, {@code -Dissue9496.parallel=false} times the sequential aggregation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue9496GroupByAggregateBenchmark {
  private static final int REPS = Integer.getInteger("issue9496.reps", 7);

  private static final String[][] QUERIES = {
      { "count_only", "SELECT l_returnflag, l_linestatus, count(*) AS n FROM LineItem GROUP BY l_returnflag, l_linestatus" },
      { "four_sums", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS a, sum(l_extendedprice) AS b, sum(l_discount) AS c, "
          + "sum(l_tax) AS d, count(*) AS n FROM LineItem GROUP BY l_returnflag, l_linestatus" },
      { "one_expr_sum", "SELECT l_returnflag, l_linestatus, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS c, "
          + "count(*) AS n FROM LineItem GROUP BY l_returnflag, l_linestatus" },
      { "q1_no_where", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
          + "sum(l_extendedprice * (1 - l_discount)) AS sum_disc, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
          + "avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS n "
          + "FROM LineItem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus" },
      { "q1", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
          + "sum(l_extendedprice * (1 - l_discount)) AS sum_disc, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
          + "avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS n "
          + "FROM LineItem WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus" },
      { "top_parts", "SELECT l_partkey, sum(l_extendedprice * (1 - l_discount)) AS rev FROM LineItem GROUP BY l_partkey "
          + "ORDER BY rev DESC, l_partkey ASC LIMIT 10" },
      { "top_parts_count", "SELECT l_partkey, count(*) AS n FROM LineItem GROUP BY l_partkey ORDER BY n DESC, l_partkey ASC LIMIT 10" },
      { "few_parts_count", "SELECT l_partkey % 4 AS p, count(*) AS n FROM LineItem GROUP BY l_partkey % 4 ORDER BY n DESC, p ASC LIMIT 10" } };

  public static void main(final String[] args) {
    new Issue9496GroupByAggregateBenchmark().timeAggregations();
  }

  @Test
  void timeAggregations() {
    final int rows = Integer.getInteger("issue9496.rows", 1_000_000);
    final boolean keep = Boolean.getBoolean("issue9496.keep");
    final boolean parallel = Boolean.parseBoolean(System.getProperty("issue9496.parallel", "true"));
    final String only = System.getProperty("issue9496.only");
    final String path = "target/databases/Issue9496GroupByAggregateBenchmark-" + rows;

    final DatabaseFactory factory = new DatabaseFactory(path);
    final Database database;
    if (keep && factory.exists())
      database = factory.open();
    else {
      if (factory.exists())
        FileUtils.deleteRecursively(new File(path));
      database = factory.create();
      load(database, rows);
    }

    try {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
      for (final String[] q : QUERIES) {
        if (only != null && !Arrays.asList(only.split(",")).contains(q[0]))
          continue;
        if (Boolean.getBoolean("issue9496.explain"))
          try (final ResultSet rs = database.query("sql", "EXPLAIN " + q[1])) {
            LogManager.instance().log(this, Level.INFO, "Issue #9496 %s plan:%n%s", null, q[0], rs.next().getProperty("executionPlanAsString"));
          }
        final double ms = time(database, q[1]);
        LogManager.instance().log(this, Level.INFO, "Issue #9496 %-16s %s %9.1f ms", null, q[0], parallel ? "parallel  " : "sequential",
            ms);
      }
    } finally {
      if (keep)
        database.close();
      else
        database.drop();
    }
  }

  private static void load(final Database database, final int rows) {
    final DocumentType type = database.getSchema().buildDocumentType().withName("LineItem").withTotalBuckets(3).create();
    type.createProperty("l_orderkey", Type.LONG);
    type.createProperty("l_partkey", Type.LONG);
    type.createProperty("l_quantity", Type.DOUBLE);
    type.createProperty("l_extendedprice", Type.DOUBLE);
    type.createProperty("l_discount", Type.DOUBLE);
    type.createProperty("l_tax", Type.DOUBLE);
    type.createProperty("l_returnflag", Type.STRING);
    type.createProperty("l_linestatus", Type.STRING);
    type.createProperty("l_shipdate", Type.STRING);
    type.createProperty("l_shipmode", Type.STRING);

    final String[] modes = { "AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK" };
    final Random rnd = new Random(1);
    final int parts = Math.max(1, rows / 30);
    for (int b = 0; b < rows; b += 50_000) {
      final int from = b;
      database.transaction(() -> {
        for (int i = from; i < Math.min(rows, from + 50_000); i++) {
          final boolean open = rnd.nextInt(2) == 0;
          final String flag = open ? "N" : String.valueOf("ARN".charAt(rnd.nextInt(3)));
          database.newDocument("LineItem").set("l_orderkey", (long) i / 4, "l_partkey", (long) (1 + rnd.nextInt(parts)),
              "l_quantity", (double) (1 + rnd.nextInt(50)), "l_extendedprice", rnd.nextInt(10_000_000) / 100.0,
              "l_discount", rnd.nextInt(11) / 100.0, "l_tax", rnd.nextInt(9) / 100.0, "l_returnflag", flag,
              "l_linestatus", open ? "O" : "F",
              "l_shipdate", String.format("199%d-%02d-%02d", 2 + rnd.nextInt(7), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28)),
              "l_shipmode", modes[rnd.nextInt(modes.length)]).save();
        }
      });
    }
    database.command("sql", "CREATE INDEX ON LineItem (l_shipdate) NOTUNIQUE");
  }

  private static double time(final Database database, final String query) {
    for (int i = 0; i < 3; i++)
      run(database, query);
    final double[] t = new double[REPS];
    for (int r = 0; r < REPS; r++) {
      final long begin = System.nanoTime();
      run(database, query);
      t[r] = (System.nanoTime() - begin) / 1e6;
    }
    Arrays.sort(t);
    return t[REPS / 2];
  }

  private static void run(final Database database, final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
