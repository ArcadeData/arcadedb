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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7002: {@code async().updateRecord()} never validated the document, so an asynchronous
 * update persisted a record violating a MANDATORY/NOTNULL constraint with no error reaching the caller or the error
 * callback. The synchronous update is the control.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7002AsyncUpdateValidationTest extends TestHelper {

  private static final String TYPE = "Issue7002P";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("name", Type.STRING).setMandatory(true).setNotNull(true);
    type.createProperty("v", Type.INTEGER);
  }

  @Test
  void asyncUpdateRejectsARecordViolatingItsSchemaAndReportsItToTheErrorCallback() {
    final RID rid = createRecord();

    // CONTROL: THE SYNCHRONOUS UPDATE REFUSES THE SAME EDIT
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = rid.asDocument().modify();
      doc.set("v", 2);
      doc.remove("name");
      doc.save();
    })).isInstanceOf(ValidationException.class);

    final AtomicInteger okCalls = new AtomicInteger();
    final AtomicReference<Throwable> reported = new AtomicReference<>();
    final AtomicReference<Throwable> executorWide = new AtomicReference<>();
    database.async().onError(executorWide::set);

    final MutableDocument doc = rid.asDocument().modify();
    doc.set("v", 3);
    doc.remove("name");
    database.async().updateRecord(doc, record -> okCalls.incrementAndGet(), reported::set);
    database.async().waitCompletion();

    assertThat(reported.get()).as("the violation must reach the task's error callback").isInstanceOf(ValidationException.class);
    assertThat(executorWide.get()).as("and the executor-wide onError").isInstanceOf(ValidationException.class);
    assertThat(okCalls.get()).as("a rejected update has no success to report").isZero();

    // THE DATABASE STILL HOLDS THE RECORD THE SCHEMA ALLOWS: NAME PRESENT, THE EDIT NEVER LANDED
    final Document stored = database.lookupByRID(rid, true).asDocument();
    assertThat(stored.getString("name")).isEqualTo("original");
    assertThat(stored.getInteger("v")).isEqualTo(1);
  }

  @Test
  void asyncUpdateStillAppliesAValidEdit() {
    final RID rid = createRecord();

    final AtomicReference<Throwable> reported = new AtomicReference<>();
    final MutableDocument doc = rid.asDocument().modify();
    doc.set("v", 5);
    database.async().updateRecord(doc, null, reported::set);
    database.async().waitCompletion();

    assertThat(reported.get()).isNull();
    assertThat(database.lookupByRID(rid, true).asDocument().getInteger("v")).isEqualTo(5);
  }

  private RID createRecord() {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> rid.set(database.newDocument(TYPE).set("name", "original").set("v", 1).save().getIdentity()));
    return rid.get();
  }
}
