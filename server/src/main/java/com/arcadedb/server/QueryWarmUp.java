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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Warms the query stack in the background while the server starts (issue #10218), so the first client request does not
 * pay for it. Started as soon as the security is up, in parallel with the load of the databases: the part that needs no
 * database runs at once, and the part that runs statements waits for the server to be {@code ONLINE}. On a fresh JVM the
 * first SQL statement loaded about 1,250 classes - the ANTLR runtime, the SQL grammar and its AST, the function and method
 * registries - and deserialized the ANTLR ATN, which made it about 60 times slower than the same statement the third time,
 * a trivial {@code SELECT 1} included.
 * <p>
 * It hashes a throwaway password the way the first login does - the JCE provider and its HMAC loop were most of what was
 * left of the first request once the query stack was warm - parses a statement that covers the common SQL clauses, which
 * needs no database, then plans and runs a trivial statement of each warmed language on one database the server holds
 * open and serializes its rows to JSON as the HTTP API does. Read-only and trivial: it touches no record, caches no
 * credential, and runs on the embedded database through the query engine itself, so it is neither replicated nor counted as
 * a client request - no tracing span, no query metric, no statistic of the database. It runs once, on a daemon thread of its
 * own that ends when it is done, at the lowest priority (a hint the operating system may ignore, so the start does share
 * its cores with it), so it delays no request that arrives first. Whatever it fails on is logged at {@code FINE} and skipped,
 * because nothing depends on it; an {@link Error} is not swallowed.
 * <p>
 * Disabled with {@link GlobalConfiguration#SERVER_QUERY_WARM_UP}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryWarmUp {
  /**
   * Parsed without a database: the grammar rules most statements go through, so the ATN states they need are deserialized
   * and the AST classes loaded.
   */
  static final String SQL_PARSE_ONLY_STATEMENT =
      "SELECT name, count(*) AS total FROM V WHERE id = 7 AND name LIKE 'a%' AND tags CONTAINS 'x' GROUP BY name ORDER BY total DESC SKIP 1 LIMIT 10";

  /** Language and statement, planned and executed on an open database when the language is registered. */
  static final List<String[]> EXECUTED_STATEMENTS = List.of(//
      new String[] { "sql", "SELECT 1" },//
      new String[] { "opencypher", "RETURN 1" });

  // A start that takes longer than this - a long recovery - gets no warm-up of the statements: nothing waits for it anyway
  private static final long MAX_WAIT_FOR_ONLINE_MS = 10 * 60 * 1000L;

  private final ArcadeDBServer server;
  private final CountDownLatch completed = new CountDownLatch(1);
  private volatile int         executedStatements;

  QueryWarmUp(final ArcadeDBServer server) {
    this.server = server;
  }

  /** Starts the warm-up on a daemon thread of its own and returns at once. */
  QueryWarmUp start() {
    final Thread thread = new Thread(this::run, "ArcadeDB Query Warm-Up");
    thread.setDaemon(true);
    thread.setPriority(Thread.MIN_PRIORITY);
    thread.start();
    return this;
  }

  /**
   * Waits for the warm-up to finish, for tests.
   *
   * @return true when it finished within the timeout
   */
  public boolean awaitCompletion(final long timeout, final TimeUnit unit) throws InterruptedException {
    return completed.await(timeout, unit);
  }

  /** How many of {@link #EXECUTED_STATEMENTS} the warm-up ran on a database, for tests. */
  public int getExecutedStatements() {
    return executedStatements;
  }

  void run() {
    final long beginTime = System.nanoTime();
    try {
      try {
        // FIRST: THE FIRST REQUEST OF ALL PAYS THE LOGIN, BEFORE IT EVEN REACHES A QUERY
        server.getSecurity().warmUpPasswordHashing();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Query warm-up: error on hashing the warm-up password", e);
      }

      try {
        new SQLAntlrParser(null).parse(SQL_PARSE_ONLY_STATEMENT);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Query warm-up: error on parsing the SQL warm-up statement", e);
      }

      final Database database = awaitOnline() ? firstOpenDatabase() : null;
      if (database != null) {
        final List<String> languages = QueryEngineManager.getInstance().getAvailableLanguages();
        for (final String[] statement : EXECUTED_STATEMENTS) {
          if (server.getStatus() != ArcadeDBServer.STATUS.ONLINE || !database.isOpen())
            break;
          if (!languages.contains(statement[0]))
            continue;

          // THE ENGINE, NOT database.query(): THAT ONE OPENS A TRACING SPAN, RECORDS arcadedb.query.duration AND COUNTS THE QUERY IN
          // THE STATISTICS OF THE DATABASE, AND A STATEMENT NOBODY ASKED FOR MUST SHOW IN NONE OF THEM
          try (final ResultSet resultSet = database.getQueryEngine(statement[0]).query(statement[1], new ContextConfiguration())) {
            final JsonSerializer serializer = JsonSerializer.createJsonSerializer();
            while (resultSet.hasNext())
              serializer.serializeResult(database, resultSet.next());
            ++executedStatements;
          } catch (final Exception e) {
            LogManager.instance()
                .log(this, Level.FINE, "Query warm-up: error on executing '%s' in %s", e, statement[1], statement[0]);
          }
        }
      }

      LogManager.instance().log(this, Level.FINE, "Query warm-up completed in %dms", null,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beginTime));
    } finally {
      completed.countDown();
    }
  }

  /**
   * Waits for the start to complete, for at most {@link #MAX_WAIT_FOR_ONLINE_MS}.
   *
   * @return true when the server is {@code ONLINE}; false when the start failed, the server is stopping, or it took longer
   */
  private boolean awaitOnline() {
    final long deadline = System.currentTimeMillis() + MAX_WAIT_FOR_ONLINE_MS;
    while (System.currentTimeMillis() < deadline) {
      final ArcadeDBServer.STATUS status = server.getStatus();
      if (status == ArcadeDBServer.STATUS.ONLINE)
        return true;
      if (status != ArcadeDBServer.STATUS.STARTING)
        return false;
      try {
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  /**
   * The embedded instance of a database the server holds open, or null when it holds none: the warm-up never opens one.
   * Embedded, so a statement of the warm-up is never forwarded to another server of a cluster.
   */
  private Database firstOpenDatabase() {
    for (final String name : server.getDatabaseNames()) {
      try {
        final ServerDatabase database = server.getDatabase(name, false, false);
        if (database != null && database.isOpen())
          return database.getEmbedded();
      } catch (final Exception e) {
        // NOT OPEN (ANY MORE): TRY THE NEXT ONE
      }
    }
    return null;
  }
}
