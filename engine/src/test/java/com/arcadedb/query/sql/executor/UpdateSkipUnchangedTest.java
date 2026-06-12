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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a plain {@code UPDATE ... SET prop = value} skips the write (and the resulting MVCC
 * version bump) when the value is unchanged, mirroring the OpenCypher MERGE ON MATCH SET behavior.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UpdateSkipUnchangedTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Account").createProperty("name", Type.STRING);
      database.command("sql", "INSERT INTO Account SET name = 'ACME', branch = 'HQ', counter = 1");
    });
  }

  /** Re-asserting the same value must not lose data: the record keeps its values. */
  @Test
  void updateUnchangedValueKeepsValue() {
    database.transaction(() ->
        database.command("sql", "UPDATE Account SET name = 'ACME', branch = 'HQ' WHERE name = 'ACME'"));

    final ResultSet rs = database.query("sql", "SELECT name, branch FROM Account");
    final Result r = rs.next();
    assertThat(r.<String>getProperty("name")).isEqualTo("ACME");
    assertThat(r.<String>getProperty("branch")).isEqualTo("HQ");
  }

  /** The equality skip must not suppress genuine updates: a changed value is still written. */
  @Test
  void updateChangedValueStillWrites() {
    database.transaction(() ->
        database.command("sql", "UPDATE Account SET counter = 2 WHERE name = 'ACME'"));

    final ResultSet rs = database.query("sql", "SELECT counter FROM Account");
    assertThat(rs.next().<Number>getProperty("counter").intValue()).isEqualTo(2);
  }

  /**
   * The unchanged-skip must not be confused by a null value: setting a previously-absent property to null must make
   * it DEFINED (present with null), because getProperty() returns null for both absent and present-null. Otherwise
   * {@code SET x = null} on a record without x would be wrongly skipped, leaving x undefined.
   */
  @Test
  void updateSettingAbsentPropertyToNullMakesItDefined() {
    database.transaction(() ->
        database.command("sql", "UPDATE Account SET extra = null WHERE name = 'ACME'"));

    final ResultSet defined = database.query("sql", "SELECT FROM Account WHERE extra is defined");
    assertThat(defined.hasNext()).as("extra must be defined after SET extra = null").isTrue();
    defined.close();

    final ResultSet notDefined = database.query("sql", "SELECT FROM Account WHERE extra is not defined");
    assertThat(notDefined.hasNext()).as("extra must not be reported as not-defined").isFalse();
    notDefined.close();
  }

  /** A mix of unchanged and changed assignments still persists the changed one. */
  @Test
  void updateMixedChangedAndUnchangedWrites() {
    database.transaction(() ->
        database.command("sql", "UPDATE Account SET name = 'ACME', counter = 99 WHERE name = 'ACME'"));

    final ResultSet rs = database.query("sql", "SELECT name, counter FROM Account");
    final Result r = rs.next();
    assertThat(r.<String>getProperty("name")).isEqualTo("ACME");
    assertThat(r.<Number>getProperty("counter").intValue()).isEqualTo(99);
  }

  /**
   * Regression: re-asserting unchanged values in UPDATE ... SET must not bump the MVCC version, so two
   * concurrent UPDATEs of the same record commit without ConcurrentModificationException.
   */
  @Test
  void updateUnchangedValueDoesNotConflictConcurrently() throws Exception {
    final CyclicBarrier bothUpdated = new CyclicBarrier(2);
    final AtomicInteger conflicts = new AtomicInteger();
    final AtomicReference<Throwable> unexpected = new AtomicReference<>();

    final Runnable worker = () -> {
      try {
        database.begin();
        // Reads the shared record into this transaction; with the bug it also rewrites the
        // unchanged values, dirtying the record's page.
        database.command("sql", "UPDATE Account SET name = 'ACME', branch = 'HQ' WHERE name = 'ACME'");
        // Ensure both transactions have run the UPDATE before either commits, so a write by one
        // would invalidate the version the other read. Bounded wait so a worker that died before
        // reaching the barrier can't hang the other indefinitely.
        bothUpdated.await(15, TimeUnit.SECONDS);
        database.commit();
      } catch (final ConcurrentModificationException e) {
        conflicts.incrementAndGet();
        if (database.isTransactionActive())
          database.rollback();
      } catch (final Throwable t) {
        unexpected.compareAndSet(null, t);
        if (database.isTransactionActive())
          database.rollback();
      }
    };

    final Thread t1 = new Thread(worker);
    final Thread t2 = new Thread(worker);
    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertThat(unexpected.get()).isNull();
    assertThat(conflicts.get()).isZero();
  }
}
