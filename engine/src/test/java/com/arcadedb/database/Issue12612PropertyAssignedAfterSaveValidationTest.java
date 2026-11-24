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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12612: in a transaction a {@code save()} validates the record and queues the very instance for the commit, which serializes it
 * as it is then. A property assigned between the last {@code save()} and the commit was therefore written without any validation:
 * {@code d.set("age", 30).save(); d.set("age", 500);} committed {@code age = 500} over a MAX of 120, when the same value passed to
 * {@code save()} is refused. The commit now validates every queued record a property was assigned to since its last save, so it refuses
 * what the save would have refused, and rolls the whole transaction back.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12612PropertyAssignedAfterSaveValidationTest extends TestHelper {
  private RID person;
  private RID other;
  private RID knows;

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.age INTEGER (MIN 0, MAX 120)");
    database.command("sql", "CREATE PROPERTY Person.name STRING (MANDATORY true, NOTNULL true)");
    database.command("sql", "CREATE PROPERTY Person.code STRING (REGEXP '[A-Z]{3}')");
    database.command("sql", "CREATE PROPERTY Person.ssn STRING (READONLY true)");
    database.command("sql", "CREATE INDEX ON Person (age) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE PROPERTY Address.city STRING (MANDATORY true)");
    database.command("sql", "CREATE PROPERTY Person.address EMBEDDED OF Address");
    database.command("sql", "CREATE EDGE TYPE Knows");
    database.command("sql", "CREATE PROPERTY Knows.weight INTEGER (MAX 5)");

    database.transaction(() -> {
      final MutableVertex p = database.newVertex("Person").set("age", 20, "name", "Ann", "code", "ABC", "ssn", "123").save();
      final MutableVertex o = database.newVertex("Person").set("age", 40, "name", "Bob", "code", "XYZ", "ssn", "456").save();
      person = p.getIdentity();
      other = o.getIdentity();
      knows = p.newEdge("Knows", o, "weight", 1).getIdentity();
    });
  }

  private MutableDocument load(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().modify();
  }

  private void assertPersonUnchanged() {
    final Document stored = database.lookupByRID(person, true).asDocument();
    assertThat(stored.getInteger("age")).isEqualTo(20);
    assertThat(stored.getString("name")).isEqualTo("Ann");
    assertThat(stored.getString("code")).isEqualTo("ABC");
    assertThat(stored.getString("ssn")).isEqualTo("123");
    assertThat(stored.has("note")).isFalse();
    assertThat(stored.has("address")).isFalse();
  }

  private void assertCheckDatabaseFindsNoIndexDisagreement() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result check = rs.next();
      assertThat(check.<Long>getProperty("staleIndexEntries")).isZero();
      assertThat(check.<Long>getProperty("missingIndexEntries")).isZero();
    }
  }

  /**
   * Saves the person with a valid change, then makes {@code afterSave} and commits: the commit has to be refused naming the property, and
   * nothing of the transaction, the other record it saved included, may be stored.
   */
  private void assertCommitRefuses(final String property, final Consumer<MutableDocument> afterSave) {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument d = load(person);
      d.set("note", "saved").save();
      load(other).set("age", 41).save();
      afterSave.accept(d);
    })).isInstanceOf(ValidationException.class).hasMessageContaining(property);

    assertThat(database.isTransactionActive()).isFalse();
    assertPersonUnchanged();
    assertThat(database.lookupByRID(other, true).asDocument().getInteger("age")).isEqualTo(40);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void aValueOverTheMaxAssignedAfterTheSaveIsRefusedByTheCommit() {
    assertCommitRefuses("age", d -> d.set("age", 500));

    final Index byAge = database.getSchema().getType("Person").getPolymorphicIndexByProperties("age");
    assertThat(byAge.get(new Object[] { 500 }).hasNext()).as("the refused key is not indexed").isFalse();
    assertThat(byAge.get(new Object[] { 20 }).next().getIdentity()).isEqualTo(person);
  }

  @Test
  void aValueUnderTheMinAssignedAfterTheSaveIsRefusedByTheCommit() {
    assertCommitRefuses("age", d -> d.set("age", -1));
  }

  @Test
  void aMandatoryPropertyRemovedAfterTheSaveIsRefusedByTheCommit() {
    assertCommitRefuses("name", d -> d.remove("name"));
  }

  @Test
  void aNullAssignedToANotNullPropertyAfterTheSaveIsRefusedByTheCommit() {
    assertCommitRefuses("name", d -> d.set("name", null));
  }

  @Test
  void aValueThatDoesNotMatchTheRegexpAssignedAfterTheSaveIsRefusedByTheCommit() {
    assertCommitRefuses("code", d -> d.set("code", "abc"));
  }

  @Test
  void aReadonlyPropertyChangedAfterTheSaveIsRefusedByTheCommit() {
    assertCommitRefuses("ssn", d -> d.set("ssn", "999"));
  }

  @Test
  void anEmbeddedDocumentAssignedAfterTheSaveIsValidatedByTheCommit() {
    // the embedded document misses its own MANDATORY property: validated with the record that holds it, as by the save
    assertCommitRefuses("city", d -> d.newEmbeddedDocument("Address", "address").set("street", "Main"));
  }

  @Test
  void anEdgePropertyAssignedAfterTheSaveIsRefusedByTheCommit() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableEdge e = database.lookupByRID(knows, true).asEdge().modify();
      e.set("weight", 2).save();
      e.set("weight", 99);
    })).isInstanceOf(ValidationException.class).hasMessageContaining("weight");

    assertThat(database.lookupByRID(knows, true).asEdge().getInteger("weight")).isEqualTo(1);
  }

  @Test
  void aRecordCreatedInTheTransactionAndSavedAgainIsValidatedByTheCommit() {
    final long before = database.countType("Person", false);
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableVertex v = database.newVertex("Person").set("age", 1, "name", "New").save();
      v.set("age", 2).save();
      v.set("age", 121);
    })).isInstanceOf(ValidationException.class).hasMessageContaining("age");

    assertThat(database.countType("Person", false)).isEqualTo(before);
  }

  @Test
  void aSaveRefusedOnAQueuedRecordDoesNotLetTheRefusedValueReachTheCommit() {
    // the caller handles the refusal and goes on: the refused value is still on the instance the transaction queued, and was stored
    database.begin();
    final MutableDocument d = load(person);
    d.set("note", "saved").save();
    d.set("age", 500);
    assertThatThrownBy(d::save).isInstanceOf(ValidationException.class);

    assertThatThrownBy(database::commit).isInstanceOf(ValidationException.class).hasMessageContaining("age");
    assertThat(database.isTransactionActive()).isFalse();
    assertPersonUnchanged();
  }

  @Test
  void aValidValueAssignedAfterTheSaveIsCommitted() {
    database.transaction(() -> {
      final MutableDocument d = load(person);
      d.set("age", 30).save();
      d.set("age", 31);
      d.set("code", "DEF");
      d.newEmbeddedDocument("Address", "address").set("city", "Rome");
      final MutableEdge e = database.lookupByRID(knows, true).asEdge().modify();
      e.set("weight", 2).save();
      e.set("weight", 5);
    });

    final Document stored = database.lookupByRID(person, true).asDocument();
    assertThat(stored.getInteger("age")).isEqualTo(31);
    assertThat(stored.getString("code")).isEqualTo("DEF");
    assertThat(stored.getEmbedded("address").getString("city")).isEqualTo("Rome");
    assertThat(database.lookupByRID(knows, true).asEdge().getInteger("weight")).isEqualTo(5);
    final Index byAge = database.getSchema().getType("Person").getPolymorphicIndexByProperties("age");
    assertThat(byAge.get(new Object[] { 31 }).next().getIdentity()).isEqualTo(person);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void anInvalidValuePutBackToAValidOneBeforeTheCommitIsCommitted() {
    database.transaction(() -> {
      final MutableDocument d = load(person);
      d.set("age", 30).save();
      d.set("age", 500);
      d.set("age", 32);
    });

    assertThat(database.lookupByRID(person, true).asDocument().getInteger("age")).isEqualTo(32);
  }

  @Test
  void aReadonlyPropertyAssignedItsOwnValueAfterTheSaveIsCommitted() {
    database.transaction(() -> {
      final MutableDocument d = load(person);
      d.set("age", 30).save();
      d.set("ssn", "123");
    });

    assertThat(database.lookupByRID(person, true).asDocument().getInteger("age")).isEqualTo(30);
  }

  @Test
  void aReadonlyValueWrittenUnderADeferralStaysAllowedForTheRestOfTheTransaction() {
    // the second write of a record a restore creates: the value it completes the record with is no alteration, at a later save or at
    // the commit, whatever was refused in between
    database.transaction(() -> {
      final MutableDocument d = load(person).set("ssn", "777");
      try (final DocumentValidator.ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(d, Set.of("ssn"))) {
        d.save();
      }
      assertThatThrownBy(() -> d.set("ssn", "888").save()).isInstanceOf(ValidationException.class).hasMessageContaining("ssn");
      d.set("ssn", "777");
      d.set("age", 33).save();
      d.set("age", 34);
    });

    final Document stored = database.lookupByRID(person, true).asDocument();
    assertThat(stored.getString("ssn")).isEqualTo("777");
    assertThat(stored.getInteger("age")).isEqualTo(34);
  }

  @Test
  void anotherReadonlyValueAssignedAfterADeferredWriteIsRefusedByTheCommit() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument d = load(person).set("ssn", "777");
      try (final DocumentValidator.ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(d, Set.of("ssn"))) {
        d.save();
      }
      d.set("ssn", "888");
    })).isInstanceOf(ValidationException.class).hasMessageContaining("ssn");

    assertPersonUnchanged();
  }

  @Test
  void aRecordReloadedAfterItsSaveIsCommittedAsTheStoredOne() {
    database.transaction(() -> {
      final MutableDocument d = load(person);
      d.set("age", 30).save();
      d.reload();
    });

    final int stored = database.lookupByRID(person, true).asDocument().getInteger("age");
    assertThat(stored).isIn(20, 30);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }
}
