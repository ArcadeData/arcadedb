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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6921: {@code CREATE INDEX <name> IF NOT EXISTS} answered the guard with the index
 * already on those properties even when that index carries a DIFFERENT name. The requested name was never created,
 * so every later lookup through it - {@code SEARCH_INDEX('<name>', ...)}, {@code DROP INDEX <name>},
 * {@code REBUILD INDEX <name>} - failed on a name the statement had just reported on.
 * <p>
 * The rule these tests pin down: a name the statement WROTE is part of the request, not a label on it. A type keeps
 * one index per property set, so the requested name cannot be added next to the existing index either - the conflict
 * is reported, naming both indexes, instead of being answered by an index the caller cannot address. A statement that
 * wrote no name is unaffected: the auto-derived {@code typeName[properties]} form is not something the caller asked
 * for, so a guarded unnamed request stays the no-op it has always been.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue6921CreateIndexIfNotExistsNameTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE RgDoc");
      database.command("sql", "CREATE PROPERTY RgDoc.body STRING");
      database.command("sql", "INSERT INTO RgDoc SET body = 'TOKEN one'");
      database.command("sql", "INSERT INTO RgDoc SET body = 'TOKEN two'");
    });
  }

  /**
   * The reported case verbatim: a FULL_TEXT index named {@code rgFt} exists on the property, and a guarded statement
   * asks for {@code rgFt2} on the same property. The requested name cannot be created, so the statement must say so
   * rather than answer for {@code rgFt}.
   */
  @Test
  void guardedNamedRequestOverADifferentlyNamedIndexIsReported() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgFt2 IF NOT EXISTS ON RgDoc (body) FULL_TEXT"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgFt2")
        .hasMessageContaining("rgFt")
        .hasMessageContaining("body");

    // The refusal must leave the existing index untouched and must not have created the requested name.
    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();
    assertThat(database.getSchema().existsIndex("rgFt2")).isFalse();
  }

  /**
   * Why the name matters: ranking is reachable only through it. Before the fix the statement above reported on
   * {@code rgFt2} while this query failed, which is the mismatch the issue is about.
   */
  @Test
  void theRequestedNameIsNotUsableAfterTheRefusal() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgFt2 IF NOT EXISTS ON RgDoc (body) FULL_TEXT"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> database.query("sql", "SELECT FROM RgDoc WHERE SEARCH_INDEX('rgFt2', 'TOKEN') = true"))
        .hasMessageContaining("rgFt2");

    // ...while the index that DOES exist answers, so the refusal is about the name and nothing else.
    try (final ResultSet rs = database.query("sql", "SELECT FROM RgDoc WHERE SEARCH_INDEX('rgFt', 'TOKEN') = true")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }
  }

  /**
   * The guard still works under the index's own name: re-running the very same statement is a no-op, so the fix
   * cannot be passing the test above by refusing every guarded request.
   */
  @Test
  void guardedNamedRequestUnderItsOwnNameStaysANoOp() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    try (final ResultSet rs = database.command("sql", "CREATE INDEX rgFt IF NOT EXISTS ON RgDoc (body) FULL_TEXT")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isFalse();
      assertThat(result.<String>getProperty("name")).isEqualTo("rgFt");
    }

    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();
  }

  /**
   * A statement that wrote no name asked for no name, so the auto-derived form is not part of the request and the
   * existing index answers the guard exactly as before.
   */
  @Test
  void guardedUnnamedRequestOverANamedIndexStaysANoOp() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    try (final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON RgDoc (body) FULL_TEXT")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isFalse();
      // Reported under the name of the index that actually satisfied it, never under a name that does not exist.
      assertThat(result.<String>getProperty("name")).isEqualTo("rgFt");
    }

    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();
    assertThat(database.getSchema().existsIndex("RgDoc[body]")).isFalse();
  }

  /**
   * The mirror of the unnamed case: a named request over the auto-derived index is still a name that does not exist,
   * so it is reported too. Covers the ordinary LSM_TREE kind, to show the rule is not FULL_TEXT specific.
   */
  @Test
  void guardedNamedRequestOverAnAutoNamedIndexIsReported() {
    database.command("sql", "CREATE INDEX ON RgDoc (body) NOTUNIQUE");
    assertThat(database.getSchema().existsIndex("RgDoc[body]")).isTrue();

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgIdx IF NOT EXISTS ON RgDoc (body) NOTUNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgIdx")
        .hasMessageContaining("RgDoc[body]");

    assertThat(database.getSchema().existsIndex("rgIdx")).isFalse();
    assertThat(database.getSchema().existsIndex("RgDoc[body]")).isTrue();
  }

  /**
   * An index the type only INHERITS is the same story, and the message has to say where the index lives - dropping
   * it takes it away from the parent type, which is never something this statement does implicitly (issue #4083).
   */
  @Test
  void guardedNamedRequestOverAnInheritedIndexIsReported() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE RgChild EXTENDS RgDoc");
      database.command("sql", "CREATE INDEX rgParentIdx ON RgDoc (body) NOTUNIQUE");
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgChildIdx IF NOT EXISTS ON RgChild (body) NOTUNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgChildIdx")
        .hasMessageContaining("rgParentIdx")
        .hasMessageContaining("RgDoc");

    assertThat(database.getSchema().existsIndex("rgChildIdx")).isFalse();
    assertThat(database.getSchema().existsIndex("rgParentIdx")).isTrue();
  }

  /**
   * Without the guard the statement was already refused, but with wording that offered {@code IF NOT EXISTS} as the
   * way to make it idempotent. That advice is wrong for a name conflict - the guarded form is refused too - so the
   * name conflict has to be reported as itself on both paths.
   */
  @Test
  void unguardedNamedRequestOverADifferentlyNamedIndexNamesTheConflict() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgFt2 ON RgDoc (body) FULL_TEXT"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgFt2")
        .hasMessageContaining("rgFt")
        .hasMessageNotContaining("IF NOT EXISTS");

    assertThat(database.getSchema().existsIndex("rgFt2")).isFalse();
  }

  /**
   * The name is the LAST of the three things compared, so a request that gets both the definition and the name wrong
   * is still told about the definition - the harder problem, and the one the #5675 rule is about. Written with a
   * manual name on purpose: that is the case where the two answers compete.
   */
  @Test
  void aKindConflictIsStillReportedAsAKindConflict() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgIdx IF NOT EXISTS ON RgDoc (body) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FULL_TEXT")
        .hasMessageContaining("LSM_TREE");

    // ...and the unnamed form, where nothing competes, reports the same conflict.
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON RgDoc (body) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FULL_TEXT")
        .hasMessageContaining("LSM_TREE");
  }

  /**
   * And the requested name is created normally when the properties are free - the fix must not stand between a
   * first guarded statement and the index it asks for.
   */
  @Test
  void guardedNamedRequestOnFreePropertiesCreatesTheName() {
    try (final ResultSet rs = database.command("sql", "CREATE INDEX rgFt IF NOT EXISTS ON RgDoc (body) FULL_TEXT")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isTrue();
      assertThat(result.<String>getProperty("name")).isEqualTo("rgFt");
    }

    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();

    try (final ResultSet rs = database.query("sql", "SELECT FROM RgDoc WHERE SEARCH_INDEX('rgFt', 'TOKEN') = true")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }
  }
}
