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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11580: {@code COMPACT INDEX} sent to {@code POST /api/v1/command} failed with "Cannot replace compacted index
 * because a transaction is active", because the HTTP handler runs every command inside its auto-commit transaction and
 * the index swap at the end of a compaction refuses to run inside one. These tests open that transaction the way the
 * handler does ({@code begin()}, the command, {@code commit()}) and expect the statement to compact, leaving the
 * caller's transaction, and whatever it holds, untouched.
 */
class CompactIndexIssue11580Test extends TestHelper {
  private static final String TYPE_NAME = "Issue11580Doc";
  private static final String IDX_NAME  = TYPE_NAME + "[email]";

  private Object previousMinPages;

  @BeforeEach
  void loadIndexWithPagesToCompact() {
    // Background compaction off, so it cannot take the COMPACTION_SCHEDULED status from the statement under test.
    previousMinPages = database.getConfiguration().getValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("email", Type.STRING);
      // Small pages so a few hundred entries seal several pages the compactor merges.
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(2048).create();
    });

    for (int batch = 0; batch < 5; batch++) {
      final int from = batch * 200;
      database.transaction(() -> {
        for (int i = from; i < from + 200; i++)
          database.newDocument(TYPE_NAME).set("email", String.format("e%06d", i)).save();
      });
    }
  }

  @AfterEach
  void restoreCompactionSetting() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, previousMinPages);
  }

  @Test
  void compactsInsideTheAutoCommitTransaction() {
    database.begin();
    final Result result = database.command("sql", "COMPACT INDEX `" + IDX_NAME + "`").next();
    assertThat(database.isTransactionActive()).as("the caller's transaction is still open").isTrue();
    database.commit();

    assertThat(result.<Boolean>getProperty("compacted")).as("a real merge ran").isTrue();
    assertThat(result.<List<String>>getProperty("indexes")).containsExactly(IDX_NAME);
    assertEveryKeyResolves(1000);
  }

  @Test
  void compactsFromAScriptInsideTheAutoCommitTransaction() {
    database.begin();
    final Result result = database.command("sqlscript", "COMPACT INDEX `" + IDX_NAME + "`;").next();
    database.commit();

    assertThat(result.<Boolean>getProperty("compacted")).isTrue();
    assertEveryKeyResolves(1000);
  }

  @Test
  void keepsTheCallersPendingChangesForItsCommit() {
    database.begin();
    database.newDocument(TYPE_NAME).set("email", "pending").save();
    final Result result = database.command("sql", "COMPACT INDEX `" + IDX_NAME + "`").next();
    assertThat(result.<Boolean>getProperty("compacted")).isTrue();
    database.commit();

    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'pending'").hasNext()).isTrue();
    assertEveryKeyResolves(1001);
  }

  @Test
  void keepsTheCallersPendingChangesForItsRollback() {
    database.begin();
    database.newDocument(TYPE_NAME).set("email", "pending").save();
    final Result result = database.command("sql", "COMPACT INDEX `" + IDX_NAME + "`").next();
    assertThat(result.<Boolean>getProperty("compacted")).isTrue();
    database.rollback();

    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'pending'").hasNext()).isFalse();
    assertEveryKeyResolves(1000);
  }

  @Test
  void aFailureOfTheOffThreadCompactionReachesTheCallerAndKeepsItsTransaction() {
    database.getSchema().buildManualIndex("Issue11580Manual", new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
        .create();

    database.begin();
    database.newDocument(TYPE_NAME).set("email", "pending").save();
    assertThatThrownBy(() -> database.command("sql", "COMPACT INDEX `Issue11580Manual`"))
        .isInstanceOf(IndexException.class)
        .hasCauseInstanceOf(CommandExecutionException.class)
        .hasRootCauseMessage("Cannot compact index 'Issue11580Manual' because it's manual and there aren't indications of what to index");
    assertThat(database.isTransactionActive()).as("the caller's transaction survives the failure").isTrue();
    database.rollback();

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'pending'").hasNext()).isFalse();
  }

  private void assertEveryKeyResolves(final long expectedCount) {
    for (int i = 0; i < 1000; i++)
      assertThat(database.getSchema().getIndexByName(IDX_NAME).get(new Object[] { String.format("e%06d", i) }).hasNext())
          .as("key e%06d", i).isTrue();
    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME).next().<Long>getProperty("c"))
        .isEqualTo(expectedCount);
  }
}
