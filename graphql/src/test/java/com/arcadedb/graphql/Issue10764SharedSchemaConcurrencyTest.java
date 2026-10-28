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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10764: one GraphQL schema serves every request thread of a database, and the SDL registry was a plain HashMap changed in
 * place by whichever thread sent a type definition. A read-only query running meanwhile failed with "Target type not defined",
 * introspection threw ConcurrentModificationException, and a reader could see a document applied halfway. The registry is now an
 * immutable value published whole: every reader sees the SDL before a document or after it.
 * <p>
 * Asserted on outcomes, not timing: the writer sends a fixed number of documents, and every answer the readers got meanwhile must
 * be one a whole SDL state gives.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10764SharedSchemaConcurrencyTest extends AbstractGraphQLTest {
  private static final int DOCUMENTS = 400;
  private static final int READERS   = 4;

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
  void readersNeverSeeAPartialOrChangingRegistry() {
    executeTest(database -> {
      defineTypes(database);

      final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
      final AtomicBoolean writing = new AtomicBoolean(true);
      final CountDownLatch start = new CountDownLatch(1);

      final Thread writer = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < DOCUMENTS; i++)
            // THE ROOT TYPE COMES FIRST AND NAMES A TYPE DECLARED AFTER IT IN THE SAME DOCUMENT: APPLIED ONE DEFINITION AT A TIME,
            // A READER COULD SEE Query POINTING AT A TYPE NOT REGISTERED YET
            database.command("graphql", """
                type Query {
                  bookById(id: String): Book
                  bookByName(name: String): Book
                  pair: Pair%d
                }
                type Pair%d { x: String }""".formatted(i, i));
        } catch (final Throwable t) {
          failures.add(t);
        } finally {
          writing.set(false);
        }
      }, "gql-writer");

      final List<Thread> readers = new ArrayList<>();
      for (int r = 0; r < READERS; r++) {
        final int reader = r;
        readers.add(new Thread(() -> {
          try {
            start.await();
            do
              readOnce(database, reader);
            while (writing.get());
          } catch (final Throwable t) {
            failures.add(t);
          }
        }, "gql-reader-" + r));
      }

      writer.start();
      readers.forEach(Thread::start);
      start.countDown();
      join(writer);
      readers.forEach(Issue10764SharedSchemaConcurrencyTest::join);

      assertThat(failures).isEmpty();

      // AND THE LAST DOCUMENT IS WHAT IS REGISTERED
      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Query\") { fields { name type { name } } } }")) {
        final List<Result> fields = resultSet.next().getProperty("fields");
        assertThat(fields).anySatisfy(f -> {
          assertThat(f.<String>getProperty("name")).isEqualTo("pair");
          assertThat(f.<Result>getProperty("type").<String>getProperty("name")).isEqualTo("Pair" + (DOCUMENTS - 1));
        });
      }
      return null;
    });
  }

  private static void join(final Thread thread) {
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void readOnce(final Database database, final int reader) {
    switch (reader % 4) {
    case 0 -> {
      // A DATA QUERY AGAINST A TYPE DECLARED BEFORE THE WRITER STARTED: ALWAYS RESOLVED
      try (final ResultSet resultSet = database.query("graphql", "{ bookByName(name: \"Mr. brain\") { id name } }")) {
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
      }
    }
    case 1 -> {
      // THE ROOT TYPE AND THE TYPE LIST COME FROM ONE REGISTRY: WHATEVER Query.pair NAMES IS IN THE LIST
      try (final ResultSet resultSet = database.query("graphql",
          "{ __schema { queryType { fields { name type { name } } } types { name } } }")) {
        final Result schema = resultSet.next();
        final Set<String> typeNames = new HashSet<>();
        for (final Result type : schema.<List<Result>>getProperty("types"))
          typeNames.add(type.getProperty("name"));
        assertThat(typeNames).contains("Query", "Book", "Author");
        for (final Result field : schema.<Result>getProperty("queryType").<List<Result>>getProperty("fields"))
          if ("pair".equals(field.getProperty("name")))
            assertThat(typeNames).contains(field.<Result>getProperty("type").<String>getProperty("name"));
      }
    }
    case 2 -> {
      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Book\") { name fields { name } } }")) {
        final Result type = resultSet.next();
        assertThat(type.<String>getProperty("name")).isEqualTo("Book");
        assertThat(type.<List<Result>>getProperty("fields")).extracting(f -> f.<String>getProperty("name"))
            .contains("id", "name", "pageCount");
      }
    }
    default -> {
      try (final ResultSet resultSet = database.query("graphql", "{ __typename }")) {
        assertThat(resultSet.next().<String>getProperty("__typename")).isEqualTo("Query");
      }
      // A DATA QUERY WITH A SUB-SELECTION THROUGH A @relationship OF THE PRE-EXISTING SDL
      try (final ResultSet resultSet = database.query("graphql", "{ bookById(id: \"book-1\") { name authors { lastName } } }")) {
        assertThat(resultSet.next().<List<Result>>getProperty("authors")).extracting(a -> a.<String>getProperty("lastName"))
            .containsExactly("Rowling");
      }
    }
    }
  }
}
