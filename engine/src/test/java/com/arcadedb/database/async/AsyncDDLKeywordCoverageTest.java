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
package com.arcadedb.database.async;

import com.arcadedb.query.sql.parser.DDLStatement;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps {@code DatabaseAsyncExecutorImpl.mayContainDDL}'s verb list in step with the SQL grammar (issue #6303,
 * item 3).
 * <p>
 * That list is the cheap half of the script classification: a dispatched {@code sqlscript} that mentions none of
 * those verbs cannot contain a DDL statement, so the parse that would prove it is skipped - which is what keeps
 * every fire-and-forget script from paying two full parses when only DDL needs the off-worker routing. Sound only
 * while the list actually covers the grammar, and a miss is quiet: the statement would go to a worker and be refused
 * there by #6281's guard, taking back exactly the operation the routing exists to give.
 * <p>
 * So the list is not maintained by hand and hope. This walks every {@link DDLStatement} subclass the parser package
 * ships and fails if one begins with a verb the filter does not know - which is the moment somebody adds it, not the
 * moment a user notices their new statement is refused when dispatched asynchronously.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AsyncDDLKeywordCoverageTest {
  @Test
  void everyDDLStatementBeginsWithAVerbTheScriptFilterKnows() throws Exception {
    final List<Class<?>> ddlStatements = ddlStatementClasses();

    // The scan itself has to be load-bearing: an empty list would make this test pass by finding nothing.
    assertThat(ddlStatements).as("the parser package must yield its DDL statements, or this test proves nothing")
        .hasSizeGreaterThan(20);

    // The check below reads the verb off the CLASS NAME, so the naming convention it rests on is itself part of what
    // has to hold: a future DDL statement named some other way would sail past the verb check while still being
    // missed by mayContainDDL at runtime. Asserted first, so that failure is loud rather than absent.
    final List<String> misnamed = new ArrayList<>();
    for (final Class<?> ddl : ddlStatements)
      if (!ddl.getSimpleName().endsWith("Statement"))
        misnamed.add(ddl.getSimpleName());

    assertThat(misnamed).as(
            "this test derives a statement's verb from its class name, so every DDLStatement has to keep the "
                + "<Verb><Noun>Statement convention - one that does not would pass the verb check below while still "
                + "being invisible to mayContainDDL")
        .isEmpty();

    final List<String> uncovered = new ArrayList<>();
    for (final Class<?> ddl : ddlStatements) {
      // Every one is named <Verb>...Statement, and the verb is the word the statement starts with.
      final String name = ddl.getSimpleName();
      if (!DatabaseAsyncExecutorImpl.mayContainDDL(name))
        uncovered.add(name);
    }

    assertThat(uncovered).as(
            "these DDL statements start with a verb DatabaseAsyncExecutorImpl.DDL_LEADING_KEYWORDS does not list, so a "
                + "script containing one would skip classification and be refused when dispatched asynchronously")
        .isEmpty();
  }

  /** A script with no DDL verb in it at all must be answered without parsing. */
  @Test
  void aScriptThatMentionsNoDDLVerbIsRejectedByTheCheapFilter() {
    assertThat(DatabaseAsyncExecutorImpl.mayContainDDL("INSERT INTO V SET id = 1; UPDATE V SET id = 2;")).isFalse();
    assertThat(DatabaseAsyncExecutorImpl.mayContainDDL("SELECT FROM V WHERE name = 'x'")).isFalse();
  }

  /** And one that does - in any case, anywhere in the text - must fall through to the parse that decides. */
  @Test
  void aScriptThatMentionsOneFallsThroughToTheParse() {
    assertThat(DatabaseAsyncExecutorImpl.mayContainDDL("insert into V set id = 1; create index on V (id) unique;"))
        .isTrue();
    assertThat(DatabaseAsyncExecutorImpl.mayContainDDL("REBUILD INDEX *;")).isTrue();
    // A false positive is allowed and costs only the parse that used to be unconditional: the parse still decides.
    assertThat(DatabaseAsyncExecutorImpl.mayContainDDL("INSERT INTO V SET note = 'please create a backup'")).isTrue();
  }

  /** Every compiled class in the parser package that is a concrete {@link DDLStatement}. */
  private static List<Class<?>> ddlStatementClasses() throws Exception {
    final File packageDir = new File(
        DDLStatement.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toPath()
        .resolve(DDLStatement.class.getPackageName().replace('.', '/')).toFile();

    final File[] files = packageDir.listFiles((dir, name) -> name.endsWith(".class") && !name.contains("$"));
    assertThat(files).as("the parser package must be on disk as classes for this scan to work").isNotNull();

    final List<Class<?>> found = new ArrayList<>();
    for (final File file : files) {
      final String simpleName = file.getName().substring(0, file.getName().length() - ".class".length());
      final Class<?> candidate;
      try {
        candidate = Class.forName(DDLStatement.class.getPackageName() + "." + simpleName);
      } catch (final Throwable e) {
        // A class that cannot be loaded on its own is not one this filter has to cover.
        continue;
      }
      if (DDLStatement.class.isAssignableFrom(candidate) && candidate != DDLStatement.class)
        found.add(candidate);
    }
    return found;
  }
}
