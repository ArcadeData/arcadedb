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
 */
package com.arcadedb.query.sql.parser;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Benchmark comparing ANTLR vs JavaCC SQL parser performance.
 */
public class ParserBenchmark {

  private static       Database database;
  private static final int      WARMUP_ITERATIONS    = 100;
  private static final int      BENCHMARK_ITERATIONS = 1000;

  // Test queries
  private static final String SIMPLE_SELECT =
      "SELECT name, age, email FROM Person WHERE active = true ORDER BY name LIMIT 10";

  private static final String COMPLEX_SELECT =
      "SELECT name, age, total, status " +
          "FROM Person " +
          "WHERE (age > 18 AND age < 65) " +
          "AND (country = 'USA' OR country = 'Canada' OR country = 'Mexico') " +
          "AND ((status = 'pending' AND total > 100) OR (status = 'completed' AND total > 500)) " +
          "AND (verified = true OR (score > 80 AND level >= 5)) " +
          "AND NOT (banned = true OR suspended = true) " +
          "AND (createdDate > '2024-01-01' AND createdDate < '2024-12-31') " +
          "ORDER BY total DESC, name ASC " +
          "SKIP 20 LIMIT 50";

  private static final String MATCH_QUERY_1 =
      "MATCH {type: Person, as: p, where: (name = 'John' AND age > 25)} " +
          ".out('Follows'){as: followed, where: (verified = true)} " +
          ".out('Likes'){as: liked, maxDepth: 3} " +
          "RETURN p.name, followed.name, liked.title " +
          "ORDER BY p.name " +
          "LIMIT 100";

  private static final String MATCH_QUERY_2 =
      "MATCH {type: Employee, as: emp, where: (department = 'Engineering')} " +
          ".out('WorksOn'){as: project, where: (status = 'active')} " +
          ".in('ManagedBy'){as: manager} " +
          ".out('ReportsTo'){as: director, optional: true} " +
          ", {as: emp}.out('HasSkill'){as: skill, where: (level >= 3)} " +
          "RETURN emp.name, project.name, manager.name, director.name, skill.name " +
          "GROUP BY emp.name " +
          "ORDER BY emp.name";

  // Individual statements that are parsed separately to measure parsing time
  private static final String[] SQL_STATEMENTS = {
      "SELECT FROM User WHERE email = 'test@example.com'",
      "UPDATE User SET lastLogin = sysdate() WHERE active = true",
      "INSERT INTO LoginHistory SET userId = #1:0, timestamp = sysdate(), ip = '192.168.1.1'",
      "SELECT FROM Order WHERE userId = #1:0 AND status = 'pending'",
      "UPDATE Order SET status = 'processing' WHERE userId = #1:0 AND status = 'pending'",
      "INSERT INTO AuditLog SET action = 'login', userId = #1:0, details = 'User logged in'",
      "SELECT FROM Notification WHERE userId = #1:0 AND viewed = false",
      "UPDATE Notification SET viewed = true WHERE userId = #1:0 AND viewed = false",
      "INSERT INTO Session SET userId = #1:0, token = uuid(), expires = sysdate()",
      "DELETE FROM TempData WHERE createdAt < '2024-01-01'"
  };

  private static final String MANY_PARENTHESIS = "SELECT (((((((1 > 0))))))) AS ref0 FROM t0;";

  @BeforeAll
  static void setup() {
    final String dbPath = "target/databases/parser_benchmark";
    deleteDirectory(new File(dbPath));
    database = new DatabaseFactory(dbPath).create();
  }

  @AfterAll
  static void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void runBenchmark() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("SQL Parser Benchmark: ANTLR vs JavaCC");
    System.out.println("=".repeat(80));
    System.out.println("Warmup iterations: " + WARMUP_ITERATIONS);
    System.out.println("Benchmark iterations: " + BENCHMARK_ITERATIONS);
    System.out.println("=".repeat(80) + "\n");

    Map<String, Object> queries = new LinkedHashMap<>();
    queries.put("Simple SELECT", SIMPLE_SELECT);
    queries.put("Complex SELECT (AND/OR)", COMPLEX_SELECT);
    queries.put("SQL Many Parenthesis", MANY_PARENTHESIS);
    queries.put("MATCH Query 1", MATCH_QUERY_1);
    queries.put("MATCH Query 2", MATCH_QUERY_2);
    queries.put("Mixed SQL (10 cmds)", SQL_STATEMENTS);

    Map<String, long[]> results = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : queries.entrySet()) {
      String queryName = entry.getKey();
      Object queryValue = entry.getValue();

      System.out.println("Benchmarking: " + queryName);
      System.out.println("-".repeat(40));

      long javaccTime;
      long antlrTime;

      if (queryValue instanceof String[] statements) {
        // Benchmark multiple statements
        javaccTime = benchmarkStatements(statements, "javacc");
        antlrTime = benchmarkStatements(statements, "antlr");
      } else {
        // Warmup and benchmark single query
        javaccTime = benchmarkParser((String) queryValue, "javacc");
        antlrTime = benchmarkParser((String) queryValue, "antlr");
      }

      results.put(queryName, new long[]{javaccTime, antlrTime});
      System.out.println();
    }

    // Print results table
    printResultsTable(results);
  }

  private long benchmarkParser(String query, String parserType) {
    // Set parser type
    GlobalConfiguration.SQL_PARSER_IMPLEMENTATION.setValue(parserType);

    StatementCache cache = new StatementCache(database, 0); // No caching

    // Warmup
    System.out.print("  " + parserType.toUpperCase() + " warmup...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      try {
        cache.get(query);
      } catch (Exception e) {
        System.out.println(" ERROR: " + e.getMessage());
        return -1;
      }
    }
    System.out.println(" done");

    // Benchmark
    System.out.print("  " + parserType.toUpperCase() + " benchmark...");
    long startTime = System.nanoTime();
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      try {
        cache.get(query);
      } catch (Exception e) {
        System.out.println(" ERROR: " + e.getMessage());
        return -1;
      }
    }
    long endTime = System.nanoTime();
    long totalTimeNs = endTime - startTime;
    long avgTimeUs = totalTimeNs / BENCHMARK_ITERATIONS / 1000;
    System.out.println(" done (" + avgTimeUs + " µs/parse)");

    return avgTimeUs;
  }

  private long benchmarkStatements(String[] statements, String parserType) {
    // Set parser type
    GlobalConfiguration.SQL_PARSER_IMPLEMENTATION.setValue(parserType);

    StatementCache cache = new StatementCache(database, 0); // No caching

    // Warmup
    System.out.print("  " + parserType.toUpperCase() + " warmup...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      for (String stmt : statements) {
        try {
          cache.get(stmt);
        } catch (Exception e) {
          System.out.println(" ERROR on '" + stmt + "': " + e.getMessage());
          return -1;
        }
      }
    }
    System.out.println(" done");

    // Benchmark
    System.out.print("  " + parserType.toUpperCase() + " benchmark...");
    long startTime = System.nanoTime();
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      for (String stmt : statements) {
        try {
          cache.get(stmt);
        } catch (Exception e) {
          System.out.println(" ERROR: " + e.getMessage());
          return -1;
        }
      }
    }
    long endTime = System.nanoTime();
    long totalTimeNs = endTime - startTime;
    // Average time per complete batch of statements
    long avgTimeUs = totalTimeNs / BENCHMARK_ITERATIONS / 1000;
    System.out.println(" done (" + avgTimeUs + " µs/batch of " + statements.length + " stmts)");

    return avgTimeUs;
  }

  private void printResultsTable(Map<String, long[]> results) {
    System.out.println("\n" + "=".repeat(90));
    System.out.println("RESULTS SUMMARY");
    System.out.println("=".repeat(90));

    // Header
    System.out.printf("%-30s │ %12s │ %12s │ %12s │ %10s%n",
        "Query Type", "JavaCC (µs)", "ANTLR (µs)", "Diff (µs)", "Winner");
    System.out.println("-".repeat(30) + "─┼─" + "-".repeat(12) + "─┼─" +
        "-".repeat(12) + "─┼─" + "-".repeat(12) + "─┼─" + "-".repeat(10));

    long totalJavacc = 0;
    long totalAntlr = 0;

    for (Map.Entry<String, long[]> entry : results.entrySet()) {
      String queryName = entry.getKey();
      long javaccTime = entry.getValue()[0];
      long antlrTime = entry.getValue()[1];

      if (javaccTime < 0 || antlrTime < 0) {
        System.out.printf("%-30s │ %12s │ %12s │ %12s │ %10s%n",
            queryName,
            javaccTime < 0 ? "ERROR" : javaccTime,
            antlrTime < 0 ? "ERROR" : antlrTime,
            "N/A",
            "N/A");
        continue;
      }

      totalJavacc += javaccTime;
      totalAntlr += antlrTime;

      long diff = antlrTime - javaccTime;
      String winner = javaccTime < antlrTime ? "JavaCC" :
          antlrTime < javaccTime ? "ANTLR" : "TIE";
      String diffStr = (diff >= 0 ? "+" : "") + diff;

      System.out.printf("%-30s │ %12d │ %12d │ %12s │ %10s%n",
          queryName, javaccTime, antlrTime, diffStr, winner);
    }

    // Totals
    System.out.println("-".repeat(30) + "─┼─" + "-".repeat(12) + "─┼─" +
        "-".repeat(12) + "─┼─" + "-".repeat(12) + "─┼─" + "-".repeat(10));

    long totalDiff = totalAntlr - totalJavacc;
    String totalWinner = totalJavacc < totalAntlr ? "JavaCC" :
        totalAntlr < totalJavacc ? "ANTLR" : "TIE";
    String totalDiffStr = (totalDiff >= 0 ? "+" : "") + totalDiff;

    System.out.printf("%-30s │ %12d │ %12d │ %12s │ %10s%n",
        "TOTAL", totalJavacc, totalAntlr, totalDiffStr, totalWinner);

    // Calculate percentage difference
    double pctDiff = ((double) (totalAntlr - totalJavacc) / totalJavacc) * 100;
    System.out.println("=".repeat(90));
    System.out.printf("Overall: ANTLR is %.1f%% %s than JavaCC%n",
        Math.abs(pctDiff),
        pctDiff > 0 ? "slower" : "faster");
    System.out.println("=".repeat(90) + "\n");
  }

  private static void deleteDirectory(File dir) {
    if (dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      dir.delete();
    }
  }
}
