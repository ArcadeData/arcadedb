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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10147: a {@code CREATE ... TYPE} statement is ONE schema publication, whatever it declares.
 * <p>
 * Under Raft every outermost {@code recordFileChanges} frame publishes its own {@code SCHEMA_ENTRY}, and the statement ran one frame
 * for the create and one more for each super type it linked (and the index of a UNIQUE edge type, and the CUSTOM values). A follower
 * deciding whether a transaction that had already written can be carried past the type it asked the leader for
 * ({@code RaftReplicatedDatabase.transactionStaysValidPastTheType}) counts the entries applied since, expects exactly one, and
 * refused every composite openCypher label ({@code CREATE (n:A:B)} renders {@code CREATE VERTEX TYPE `A~B` EXTENDS A, B}) with
 * a message blaming a schema change nobody made. It also saw the type appear without its super types and then gain them.
 * <p>
 * The guarantee is the invariant the counting rule depends on, over every shape the statement can take: it opens one outermost
 * session (the number of Raft entries the same code publishes, see {@link SchemaSessionCounter}) and advances
 * {@code LocalSchema.getVersion()} - which moves once per schema publication - by exactly one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10147CreateTypeOneSchemaEntryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE BaseRel");
    database.command("sql", "CREATE DOCUMENT TYPE BaseDoc");
    // A super type that declares an index: linking a sub type propagates it over the sub type's buckets, in transactions of their own
    database.command("sql", "CREATE VERTEX TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.key STRING");
    database.command("sql", "CREATE INDEX ON Indexed (key) UNIQUE");
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {
      "CREATE VERTEX TYPE Plain",
      "CREATE DOCUMENT TYPE PlainDoc",
      "CREATE EDGE TYPE Rel",
      "CREATE VERTEX TYPE SubOne EXTENDS A",
      "CREATE VERTEX TYPE `A~B` IF NOT EXISTS EXTENDS A, B",
      "CREATE VERTEX TYPE ThreeSupers EXTENDS A, B, Indexed",
      "CREATE VERTEX TYPE SubIndexed EXTENDS Indexed",
      "CREATE EDGE TYPE SubRel EXTENDS BaseRel",
      "CREATE DOCUMENT TYPE SubDoc EXTENDS BaseDoc",
      "CREATE EDGE TYPE RelUnique UNIQUE",
      "CREATE EDGE TYPE SubRelUnique EXTENDS BaseRel UNIQUE",
      "CREATE EDGE TYPE RelLight LIGHTWEIGHT",
      "CREATE VERTEX TYPE WithBuckets BUCKETS 4",
      "CREATE VERTEX TYPE WithBucketsAndSupers EXTENDS A, B BUCKETS 2",
      "CREATE VERTEX TYPE WithPageSize PAGESIZE 131072",
      "CREATE VERTEX TYPE WithCustom CUSTOM a = 1, b = 'x'",
      "CREATE VERTEX TYPE WithSupersAndCustom EXTENDS A, B CUSTOM a = 1" })
  void oneStatementIsOneSchemaEntryWhateverItDeclares(final String statement) {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> database.command("sql", statement).close());

    assertThat(sessions).as("outermost schema sessions, i.e. SCHEMA_ENTRY under Raft, of: %s", statement).isEqualTo(1);
    assertThat(schema().getVersion() - versionBefore).as("schema versions published by: %s", statement).isEqualTo(1);
  }

  /** The same statement under a name that is a variable, which is resolved when it runs. */
  @Test
  void theStatementWithAVariableNameIsOneEntryToo() {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database,
        () -> database.command("sqlscript", "LET name = 'Dynamic'; CREATE VERTEX TYPE $name EXTENDS A, B;").close());

    assertThat(sessions).isEqualTo(1);
    assertThat(schema().getVersion() - versionBefore).isEqualTo(1);
    assertThat(database.getSchema().getType("Dynamic").getSuperTypes()).extracting(DocumentType::getName)
        .containsExactlyInAnyOrder("A", "B");
  }

  /** One entry is useless if it carries less than the three of them did: the type, both links and the custom values are all there. */
  @Test
  void theSingleEntryCarriesTheTypeItsSuperTypesAndItsCustomValues() {
    database.command("sql", "CREATE VERTEX TYPE `A~B` EXTENDS A, B CUSTOM origin = 'cypher'").close();

    assertComposite();

    reopenDatabase();

    assertComposite();
    assertThat(schema().getEmbedded().toJSON().toString()).contains("A~B");
  }

  private void assertComposite() {
    final DocumentType composite = database.getSchema().getType("A~B");
    assertThat(composite.getSuperTypes()).extracting(DocumentType::getName).containsExactlyInAnyOrder("A", "B");
    assertThat(composite.getCustomValue("origin")).isEqualTo("cypher");
    assertThat(database.getSchema().getType("A").getSubTypes()).extracting(DocumentType::getName).contains("A~B");
    assertThat(database.getSchema().getType("B").getSubTypes()).extracting(DocumentType::getName).contains("A~B");
  }

  /** The inherited index is propagated by the same statement: a record of the composite type is in the super type's unique index. */
  @Test
  void theIndexOfASuperTypeCoversTheNewSubTypeAfterTheSingleEntry() {
    database.command("sql", "CREATE VERTEX TYPE SubIndexed EXTENDS Indexed").close();

    database.transaction(() -> database.newVertex("SubIndexed").set("key", "k1").save());

    assertThat(database.query("sql", "SELECT FROM Indexed WHERE key = 'k1'").stream().count()).isEqualTo(1L);
    assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Indexed").set("key", "k1").save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /** {@code IF NOT EXISTS} on a type that is there asks nothing of the schema: no entry, and nothing bumps the version. */
  @Test
  void ifNotExistsOnAnExistingTypePublishesNothing() {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database,
        () -> database.command("sql", "CREATE VERTEX TYPE A IF NOT EXISTS EXTENDS B").close());

    assertThat(sessions).isZero();
    assertThat(schema().getVersion()).isEqualTo(versionBefore);
    assertThat(database.getSchema().getType("A").getSuperTypes()).isEmpty();
  }

  /** A statement refused before it creates anything opens no session at all. */
  @Test
  void aStatementRefusedBeforeItCreatesAnythingPublishesNothing() {
    database.command("sql", "CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)").close();
    final long versionBefore = schema().getVersion();

    final SchemaSessionCounter.Count seen = new SchemaSessionCounter.Count();
    assertThatThrownBy(() -> SchemaSessionCounter.count((LocalDatabase) database, seen,
        () -> database.command("sql", "CREATE VERTEX TYPE Child EXTENDS Metrics").close())).isInstanceOf(SchemaException.class);

    assertThat(seen.sessions()).isZero();
    assertThat(schema().getVersion()).isEqualTo(versionBefore);
    assertThat(database.getSchema().existsType("Child")).isFalse();
  }

  private LocalSchema schema() {
    return ((LocalDatabase) database).getSchema().getEmbedded();
  }
}
