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
  void duplicateAsAPropertyNameStillParsesAsAnIdentifier() {
    database.getSchema().createDocumentType("Labelled");

    final ResultSet result = database.command("sql", "INSERT INTO Labelled SET duplicate = true, skip = 'no'");
    final Result item = result.next();
    assertThat(item.<Boolean>getProperty("duplicate")).isTrue();
    assertThat(item.<String>getProperty("skip")).isEqualTo("no");
    result.close();
  }
}
