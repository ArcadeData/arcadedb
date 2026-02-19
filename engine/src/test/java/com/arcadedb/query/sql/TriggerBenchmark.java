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
package com.arcadedb.query.sql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Benchmark comparing SQL, JavaScript, and Java trigger performance.
 * Measures the overhead of executing triggers on document operations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class TriggerBenchmark {

  private static       Database database;
  private static final int      WARMUP_ITERATIONS    = 10000;
  private static final int      BENCHMARK_ITERATIONS = 100000;

  @BeforeAll
  static void setup() {
    final String dbPath = "target/databases/trigger_benchmark";
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
    System.out.println("\n" + "=".repeat(90));
    System.out.println("Trigger Performance Benchmark: SQL vs JavaScript vs Java");
    System.out.println("=".repeat(90));
    System.out.println("Warmup iterations: " + WARMUP_ITERATIONS);
    System.out.println("Benchmark iterations: " + BENCHMARK_ITERATIONS);
    System.out.println("=".repeat(90) + "\n");

    Map<String, Long> results = new LinkedHashMap<>();

    // Test 1: Baseline (no trigger)
    System.out.println("Benchmarking: No Trigger (Baseline)");
    System.out.println("-".repeat(50));
    long baselineTime = benchmarkNoTrigger();
    results.put("No Trigger (Baseline)", baselineTime);
    System.out.println();

    // Test 2: SQL Trigger
    System.out.println("Benchmarking: SQL Trigger");
    System.out.println("-".repeat(50));
    long sqlTime = benchmarkSQLTrigger();
    results.put("SQL Trigger", sqlTime);
    System.out.println();

    // Test 3: JavaScript Trigger
    System.out.println("Benchmarking: JavaScript Trigger");
    System.out.println("-".repeat(50));
    long jsTime = benchmarkJavaScriptTrigger();
    results.put("JavaScript Trigger", jsTime);
    System.out.println();

    // Test 4: Java Trigger
    System.out.println("Benchmarking: Java Trigger");
    System.out.println("-".repeat(50));
    long javaTime = benchmarkJavaTrigger();
    results.put("Java Trigger", javaTime);
    System.out.println();

    // Print results table
    printResultsTable(results);
  }

  private long benchmarkNoTrigger() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("BaselineTest")) {
        database.getSchema().createDocumentType("BaselineTest");
      }
    });

    // Warmup
    System.out.print("  Warmup...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("BaselineTest")
            .set("name", "Test")
            .save();
      });
    }
    System.out.println(" done");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DELETE FROM BaselineTest");
    });

    // Benchmark
    System.out.print("  Benchmark...");
    long startTime = System.nanoTime();
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("BaselineTest")
            .set("name", "Test")
            .save();
      });
    }
    long endTime = System.nanoTime();
    long totalTimeNs = endTime - startTime;
    long avgTimeUs = totalTimeNs / BENCHMARK_ITERATIONS / 1000;
    System.out.println(" done (" + avgTimeUs + " µs/operation)");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DELETE FROM BaselineTest");
      database.getSchema().dropType("BaselineTest");
    });

    return avgTimeUs;
  }

  private long benchmarkSQLTrigger() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("SQLTest")) {
        database.getSchema().createDocumentType("SQLTest");
      }
      if (!database.getSchema().existsType("SQLAudit")) {
        database.getSchema().createDocumentType("SQLAudit");
      }
      database.command("sql",
          """
          CREATE TRIGGER sql_benchmark_trigger BEFORE CREATE ON TYPE SQLTest \
          EXECUTE SQL 'INSERT INTO SQLAudit SET triggered = true'""");
    });

    // Warmup
    System.out.print("  Warmup...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("SQLTest")
            .set("name", "Test")
            .save();
      });
    }
    System.out.println(" done");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DELETE FROM SQLTest");
    });

    // Benchmark
    System.out.print("  Benchmark...");
    long startTime = System.nanoTime();
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("SQLTest")
            .set("name", "Test")
            .save();
      });
    }
    long endTime = System.nanoTime();
    long totalTimeNs = endTime - startTime;
    long avgTimeUs = totalTimeNs / BENCHMARK_ITERATIONS / 1000;
    System.out.println(" done (" + avgTimeUs + " µs/operation)");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DROP TRIGGER sql_benchmark_trigger");
      database.command("sql", "DELETE FROM SQLTest");
      database.command("sql", "DELETE FROM SQLAudit");
      database.getSchema().dropType("SQLTest");
      database.getSchema().dropType("SQLAudit");
    });

    return avgTimeUs;
  }

  private long benchmarkJavaScriptTrigger() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("JSTest")) {
        database.getSchema().createDocumentType("JSTest");
      }
      if (!database.getSchema().existsType("JSAudit")) {
        database.getSchema().createDocumentType("JSAudit");
      }
      database.command("sql",
          """
          CREATE TRIGGER js_benchmark_trigger BEFORE CREATE ON TYPE JSTest \
          EXECUTE JAVASCRIPT 'database.command("sql", "INSERT INTO JSAudit SET triggered = true");'""");
    });

    // Warmup
    System.out.print("  Warmup...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("JSTest")
            .set("name", "Test")
            .save();
      });
    }
    System.out.println(" done");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DELETE FROM JSTest");
    });

    // Benchmark
    System.out.print("  Benchmark...");
    long startTime = System.nanoTime();
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("JSTest")
            .set("name", "Test")
            .save();
      });
    }
    long endTime = System.nanoTime();
    long totalTimeNs = endTime - startTime;
    long avgTimeUs = totalTimeNs / BENCHMARK_ITERATIONS / 1000;
    System.out.println(" done (" + avgTimeUs + " µs/operation)");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DROP TRIGGER js_benchmark_trigger");
      database.command("sql", "DELETE FROM JSTest");
      database.command("sql", "DELETE FROM JSAudit");
      database.getSchema().dropType("JSTest");
      database.getSchema().dropType("JSAudit");
    });

    return avgTimeUs;
  }

  private long benchmarkJavaTrigger() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("JavaTest")) {
        database.getSchema().createDocumentType("JavaTest");
      }
      if (!database.getSchema().existsType("JavaAudit")) {
        database.getSchema().createDocumentType("JavaAudit");
      }
      database.command("sql",
          """
          CREATE TRIGGER java_benchmark_trigger BEFORE CREATE ON TYPE JavaTest \
          EXECUTE JAVA 'com.arcadedb.query.sql.BenchmarkTrigger'""");
    });

    // Warmup
    System.out.print("  Warmup...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("JavaTest")
            .set("name", "Test")
            .save();
      });
    }
    System.out.println(" done");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DELETE FROM JavaTest");
    });

    // Benchmark
    System.out.print("  Benchmark...");
    long startTime = System.nanoTime();
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      database.transaction(() -> {
        database.newDocument("JavaTest")
            .set("name", "Test")
            .save();
      });
    }
    long endTime = System.nanoTime();
    long totalTimeNs = endTime - startTime;
    long avgTimeUs = totalTimeNs / BENCHMARK_ITERATIONS / 1000;
    System.out.println(" done (" + avgTimeUs + " µs/operation)");

    // Cleanup
    database.transaction(() -> {
      database.command("sql", "DROP TRIGGER java_benchmark_trigger");
      database.command("sql", "DELETE FROM JavaTest");
      database.command("sql", "DELETE FROM JavaAudit");
      database.getSchema().dropType("JavaTest");
      database.getSchema().dropType("JavaAudit");
    });

    return avgTimeUs;
  }

  private void printResultsTable(Map<String, Long> results) {
    System.out.println("\n" + "=".repeat(90));
    System.out.println("RESULTS SUMMARY");
    System.out.println("=".repeat(90));

    long baseline = results.get("No Trigger (Baseline)");

    // Header
    System.out.printf("%-25s │ %15s │ %15s │ %20s%n",
        "Trigger Type", "Avg Time (µs)", "Overhead (µs)", "Overhead (%)");
    System.out.println("-".repeat(25) + "─┼─" + "-".repeat(15) + "─┼─" +
        "-".repeat(15) + "─┼─" + "-".repeat(20));

    for (Map.Entry<String, Long> entry : results.entrySet()) {
      String triggerType = entry.getKey();
      long avgTime = entry.getValue();
      long overhead = avgTime - baseline;
      double overheadPct = baseline > 0 ? ((double) overhead / baseline) * 100 : 0;

      if (triggerType.equals("No Trigger (Baseline)")) {
        System.out.printf("%-25s │ %15d │ %15s │ %20s%n",
            triggerType, avgTime, "—", "—");
      } else {
        System.out.printf("%-25s │ %15d │ %15d │ %19.1f%%%n",
            triggerType, avgTime, overhead, overheadPct);
      }
    }

    System.out.println("=".repeat(90));

    // Performance comparison
    long sqlTime = results.get("SQL Trigger");
    long jsTime = results.get("JavaScript Trigger");
    long javaTime = results.get("Java Trigger");

    System.out.println("\nPerformance Comparison:");
    System.out.println("-".repeat(50));

    // Find fastest trigger type
    String fastest = "Java";
    long fastestTime = javaTime;
    if (sqlTime < fastestTime) {
      fastest = "SQL";
      fastestTime = sqlTime;
    }
    if (jsTime < fastestTime) {
      fastest = "JavaScript";
      fastestTime = jsTime;
    }

    System.out.printf("Fastest trigger type: %s (%d µs)%n", fastest, fastestTime);
    System.out.println();

    // Relative performance
    if (!fastest.equals("SQL")) {
      double sqlSlower = ((double) (sqlTime - fastestTime) / fastestTime) * 100;
      System.out.printf("SQL is %.1f%% slower than %s%n", sqlSlower, fastest);
    }

    if (!fastest.equals("JavaScript")) {
      double jsSlower = ((double) (jsTime - fastestTime) / fastestTime) * 100;
      System.out.printf("JavaScript is %.1f%% slower than %s%n", jsSlower, fastest);
    }

    if (!fastest.equals("Java")) {
      double javaSlower = ((double) (javaTime - fastestTime) / fastestTime) * 100;
      System.out.printf("Java is %.1f%% slower than %s%n", javaSlower, fastest);
    }

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
