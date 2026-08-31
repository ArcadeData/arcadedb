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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Regression/feature tests for issue #4918: {@code INSERT INTO ... CONTENT [ ... ] ON DUPLICATE KEY SKIP} lets a
 * bulk insert of a JSON array skip records that would violate a unique index instead of aborting the whole
 * batch, as plain {@code INSERT ... CONTENT [...]} still does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class InsertOnDuplicateKeySkipTest extends TestHelper {

  public InsertOnDuplicateKeySkipTest() {
    autoStartTx = true;
  }

  private void createProductTypeWithUniqueSku() {
    database.getSchema().createDocumentType("Product").createProperty("sku", Type.STRING);
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Product (sku) UNIQUE");
  }

  @Test
  void withoutTheClauseADuplicateKeyStillAbortsTheRemainderOfTheBatch() {
    createProductTypeWithUniqueSku();
    database.command("sql", "INSERT INTO Product SET sku = 'A1', name = 'first'");

    // Matches the documented baseline (issue #4918): the record BEFORE the clash is still inserted, the
    // clashing record raises, and every record AFTER it never runs at all - the whole point of the new clause.
    assertThatExceptionOfType(DuplicatedKeyException.class).isThrownBy(() -> database.command("sql",
        "INSERT INTO Product CONTENT [ {\"sku\":\"A2\",\"name\":\"second\"}, {\"sku\":\"A1\",\"name\":\"clashes\"}, {\"sku\":\"A3\",\"name\":\"never runs\"} ]"));

    assertThat(database.query("sql", "SELECT FROM Product WHERE sku = 'A3'").hasNext()).isFalse();
  }

  @Test
  void skipsARecordClashingWithAPreviouslyCommittedRecord() {
    createProductTypeWithUniqueSku();
    database.command("sql", "INSERT INTO Product SET sku = 'A1', name = 'original'");

    final ResultSet result = database.command("sql",
        "INSERT INTO Product CONTENT [ {\"sku\":\"A2\",\"name\":\"second\"}, {\"sku\":\"A1\",\"name\":\"clashes\"} ] ON DUPLICATE KEY SKIP");

    final Result first = result.next();
    assertThat(first.<Boolean>getProperty("@skipped")).isNull();
    assertThat(first.<String>getProperty("sku")).isEqualTo("A2");

    final Result second = result.next();
    assertThat(second.<Boolean>getProperty("@skipped")).isTrue();
    assertThat(second.<String>getProperty("sku")).isEqualTo("A1");
    assertThat(second.<String>getProperty("@duplicateIndex")).isEqualTo("Product[sku]");
    assertThat(second.<Object>getProperty("@existingRID")).isNotNull();
    assertThat(second.<List<Object>>getProperty("@duplicateKeys")).containsExactly("A1");
    assertThat(result.hasNext()).isFalse();
    result.close();

    // The pre-existing record was left untouched, not overwritten.
    final Result existing = database.query("sql", "SELECT FROM Product WHERE sku = 'A1'").next();
    assertThat(existing.<String>getProperty("name")).isEqualTo("original");

    assertThat(database.query("sql", "SELECT FROM Product WHERE sku = 'A2'").hasNext()).isTrue();
    assertThat(database.countType("Product", true)).isEqualTo(2);
  }

  @Test
  void skipsARecordClashingWithAnEarlierRecordInTheSameBatch() {
    createProductTypeWithUniqueSku();

    final ResultSet result = database.command("sql",
        "INSERT INTO Product CONTENT [ {\"sku\":\"B1\",\"name\":\"first\"}, {\"sku\":\"B1\",\"name\":\"dup-in-batch\"} ] ON DUPLICATE KEY SKIP");

    final Result first = result.next();
    assertThat(first.<Boolean>getProperty("@skipped")).isNull();

    final Result second = result.next();
    assertThat(second.<Boolean>getProperty("@skipped")).isTrue();
    result.close();

    assertThat(database.countType("Product", true)).isEqualTo(1);
    assertThat(database.query("sql", "SELECT FROM Product WHERE sku = 'B1'").next().<String>getProperty("name")).isEqualTo("first");
  }

  @Test
  void aNonDuplicateKeyErrorStillAbortsTheRemainderOfTheBatch() {
    database.getSchema().createDocumentType("Strict").createProperty("code", Type.STRING).setMandatory(true);

    // The mandatory-field violation is not a duplicate key: SKIP must not swallow it, so it still raises and
    // still stops any record after it, exactly like the DuplicatedKeyException baseline above.
    assertThatExceptionOfType(ValidationException.class).isThrownBy(() -> database.command("sql",
        "INSERT INTO Strict CONTENT [ {\"other\":\"missing mandatory code\"}, {\"code\":\"never runs\"} ] ON DUPLICATE KEY SKIP"));

    assertThat(database.query("sql", "SELECT FROM Strict WHERE code = 'never runs'").hasNext()).isFalse();
  }

  @Test
  void allRecordsInsertedWhenThereIsNoConflict() {
    createProductTypeWithUniqueSku();

    final ResultSet result = database.command("sql",
        "INSERT INTO Product CONTENT [ {\"sku\":\"C1\"}, {\"sku\":\"C2\"}, {\"sku\":\"C3\"} ] ON DUPLICATE KEY SKIP");

    int count = 0;
    while (result.hasNext()) {
      assertThat(result.next().<Boolean>getProperty("@skipped")).isNull();
      count++;
    }
    result.close();
    assertThat(count).isEqualTo(3);
  }

  @Test
  void supportsTheSetInsertForm() {
    // ON DUPLICATE KEY SKIP is wired into SaveElementStep, which every insert body form feeds through - not
    // just CONTENT [...]. Pins that down for the SET form explicitly.
    createProductTypeWithUniqueSku();
    database.command("sql", "INSERT INTO Product SET sku = 'E1', name = 'original'");

    final ResultSet result = database.command("sql",
        "INSERT INTO Product SET sku = 'E1', name = 'clashes' ON DUPLICATE KEY SKIP");

    final Result item = result.next();
    assertThat(item.<Boolean>getProperty("@skipped")).isTrue();
    result.close();

    assertThat(database.countType("Product", true)).isEqualTo(1);
    assertThat(database.query("sql", "SELECT FROM Product WHERE sku = 'E1'").next().<String>getProperty("name")).isEqualTo("original");
  }

  @Test
  void supportsTheFromQueryInsertForm() {
    // Same reasoning as supportsTheSetInsertForm(), for the INSERT ... FROM <query> form - selecting whole
    // elements (not a column projection): CopyDocumentStep copies an element into an unsaved in-memory document
    // and lets SaveElementStep do the one save, same as the CONTENT/SET forms. A projected SELECT is a
    // different, pre-existing code path in CopyDocumentStep that saves eagerly and bypasses SaveElementStep
    // entirely - out of scope for this clause, since fixing that is a CopyDocumentStep change unrelated to #4918.
    createProductTypeWithUniqueSku();
    database.command("sql", "INSERT INTO Product SET sku = 'D1', name = 'original'");
    database.getSchema().createDocumentType("Staging").createProperty("sku", Type.STRING);
    database.getSchema().getType("Staging").createProperty("name", Type.STRING);
    database.command("sql",
        "INSERT INTO Staging CONTENT [ {\"sku\":\"D1\",\"name\":\"clashes\"}, {\"sku\":\"D2\",\"name\":\"second\"} ]");

    final ResultSet result = database.command("sql",
        "INSERT INTO Product ON DUPLICATE KEY SKIP FROM (SELECT FROM Staging ORDER BY sku)");

    final Result first = result.next();
    assertThat(first.<Boolean>getProperty("@skipped")).isTrue();
    assertThat(first.<String>getProperty("sku")).isEqualTo("D1");

    final Result second = result.next();
    assertThat(second.<Boolean>getProperty("@skipped")).isNull();
    assertThat(second.<String>getProperty("sku")).isEqualTo("D2");
    result.close();

    assertThat(database.countType("Product", true)).isEqualTo(2);
    assertThat(database.query("sql", "SELECT FROM Product WHERE sku = 'D1'").next().<String>getProperty("name")).isEqualTo("original");
  }

  @Test
  void skipsAPartiallyNullCompositeKeyClashingWithAnExistingRecord() {
    // A composite unique index only exempts a key from the uniqueness check when EVERY component is null
    // (LSMTreeIndexAbstract.isKeyNull); a key with just ONE null component, like (John, null) here, is still a
    // real key the engine enforces uniqueness on - findDuplicateKeyConflict() must agree, or the clashing record
    // slips past the probe and aborts the batch with an uncaught DuplicatedKeyException instead of being skipped.
    database.getSchema().createDocumentType("Person").createProperty("firstName", Type.STRING);
    database.getSchema().getType("Person").createProperty("lastName", Type.STRING);
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Person (firstName, lastName) UNIQUE");
    database.command("sql", "INSERT INTO Person SET firstName = 'John', lastName = null");

    final ResultSet result = database.command("sql",
        "INSERT INTO Person CONTENT [ {\"firstName\":\"John\",\"lastName\":null} ] ON DUPLICATE KEY SKIP");

    final Result item = result.next();
    assertThat(item.<Boolean>getProperty("@skipped")).isTrue();
    assertThat(item.<String>getProperty("@duplicateIndex")).isEqualTo("Person[firstName,lastName]");
    result.close();

    assertThat(database.countType("Person", true)).isEqualTo(1);
  }

  @Test
  void anAllNullKeyIsNeverADuplicateEvenUnderNullStrategyIndex() {
    // SQL standard: NULL != NULL, so multiple all-null keys are allowed in a unique index regardless of
    // NULL_STRATEGY (engine/src/test/java/com/arcadedb/index/NullValuesIndexTest#nullStrategyIndex_uniqueAllowsMultipleNulls
    // proves this holds for a plain INSERT even under NULL_STRATEGY.INDEX). ON DUPLICATE KEY SKIP must not
    // report a "duplicate" - and so must not silently drop the row - for something a plain INSERT would accept.
    database.getSchema().createDocumentType("Ticket").createProperty("code", Type.STRING);
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Ticket (code) UNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "INSERT INTO Ticket SET code = null");

    final ResultSet result = database.command("sql",
        "INSERT INTO Ticket CONTENT [ {\"code\":null} ] ON DUPLICATE KEY SKIP");

    final Result item = result.next();
    assertThat(item.<Boolean>getProperty("@skipped")).isNull();
    result.close();

    assertThat(database.countType("Ticket", true)).isEqualTo(2);
  }

  @Test
  void duplicateAsAPropertyNameStillParsesAsAnIdentifier() {
    database.getSchema().createDocumentType("Labelled");

    final ResultSet result = database.command("sql", "INSERT INTO Labelled SET duplicate = true, skip = 'no'");
    final Result item = result.next();
    assertThat(item.<Boolean>getProperty("duplicate")).isTrue();
    assertThat(item.<String>getProperty("skip")).isEqualTo("no");
    result.close();
  }
}
