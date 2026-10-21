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
package com.arcadedb.remote;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard for issue #10373: a remote call that reads the server's answer with {@code next()} or
 * {@code nextIfAvailable()} and no emptiness check fails with a bare {@link java.util.NoSuchElementException} (or a
 * {@link NullPointerException}) the moment the server answers nothing - which it does for a write a listener vetoed - where the
 * embedded engine raises a typed exception the caller can catch.
 * <p>
 * The question is not "did the method name start with save or delete", it is "can the server answer nothing where the caller's
 * return type cannot express nothing". Every such call is found here, in the two packages that hold the remote record layer. A
 * call passes when an {@code hasNext()} / {@code isEmpty()} check sits in the few lines before it. One without is listed in
 * {@link #AUDITED} with the reason the answer cannot be empty, so adding a new one is a decision somebody wrote down and not an
 * accident. The scan is a tripwire and not a proof: it reads text, and a check in the window is taken as the guard.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10373RemoteUnguardedNextTest {

  /** The remote record layer: the HTTP driver, then the gRPC one that extends it. */
  private static final List<Path> ROOTS = List.of(Path.of("src/main/java/com/arcadedb/remote"),
      Path.of("..", "grpc-client", "src/main/java/com/arcadedb/remote/grpc"));

  /** How many lines before a call a check still counts as the guard of that call. */
  private static final int WINDOW = 8;

  /** Below this the walk has lost its roots: the two packages hold far more reads than that. */
  private static final int EXPECTED_MINIMUM_READS = 20;

  /**
   * Reads with no emptiness check in front of them, each with why the answer cannot be empty. Keyed by file and the trimmed
   * line, so it survives edits above it and fails the moment the line itself changes.
   */
  private static final Map<String, String> AUDITED = new HashMap<>();

  static {
    final String nullChecked = "answers null for no row, and the next line tests it";
    AUDITED.put("RemoteDatabase.java: final Result result = resultSet.nextIfAvailable();", nullChecked);
    AUDITED.put("RemoteSchema.java: final Result result = resultSet.nextIfAvailable();", nullChecked);

    final String count = "count(*) over a type answers its one row, an empty type included";
    AUDITED.put("RemoteDatabase.java: (connection, response) -> createResultSet(response))).nextIfAvailable().getProperty(\"count\")).longValue();",
        count);
    AUDITED.put(
        "RemoteDatabase.java: false, (connection, response) -> createResultSet(response))).nextIfAvailable().getProperty(\"count\")).longValue();",
        count);

    AUDITED.put("RemoteSchema.java: return new RemoteBucket(result.next().getProperty(\"bucketName\"));",
        "CREATE BUCKET answers the bucket it created, and no listener can veto a schema statement");
  }

  @Test
  void everyReadOfTheServersAnswerAsksIfThereIsOneOrIsAudited() throws IOException {
    final List<String> unguarded = new ArrayList<>();
    final Map<String, Integer> seen = new HashMap<>();
    int reads = 0;

    for (final Path root : ROOTS) {
      assertThat(root).as("the scan root exists, or the guard reads nothing").isDirectory();
      try (final Stream<Path> files = Files.walk(root)) {
        for (final Path file : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
          final List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
          for (int i = 0; i < lines.size(); i++) {
            final String line = lines.get(i).trim();
            if (!isRead(line))
              continue;
            reads++;
            if (isGuarded(lines, i))
              continue;

            final String key = file.getFileName() + ": " + line;
            seen.merge(key, 1, Integer::sum);
            if (!AUDITED.containsKey(key))
              unguarded.add(file + ":" + (i + 1) + "  " + line);
          }
        }
      }
    }

    assertThat(reads).as("reads scanned").isGreaterThanOrEqualTo(EXPECTED_MINIMUM_READS);
    assertThat(unguarded).as("a next()/nextIfAvailable() on the server's answer with no hasNext()/isEmpty() in the %d lines before it: "
        + "an empty answer - a vetoed write - reaches the caller as a NoSuchElementException. Ask first and raise the typed exception the "
        + "embedded engine raises, or audit the call into AUDITED with the reason the answer is never empty", WINDOW).isEmpty();
    assertThat(seen.keySet()).as("AUDITED entries that no longer match a line: delete them").containsAll(AUDITED.keySet());
  }

  /** A call, not a mention of one in a comment. */
  private static boolean isRead(final String line) {
    if (line.startsWith("*") || line.startsWith("//") || line.startsWith("/*"))
      return false;
    return line.contains(".next()") || line.contains(".nextIfAvailable()");
  }

  private static boolean isGuarded(final List<String> lines, final int index) {
    for (int i = Math.max(0, index - WINDOW); i <= index; i++) {
      final String line = lines.get(i);
      if (line.contains("hasNext()") || line.contains("isEmpty()"))
        return true;
    }
    return false;
  }
}
