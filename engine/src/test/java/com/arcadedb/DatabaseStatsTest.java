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
package com.arcadedb;

import com.arcadedb.database.DatabaseStats;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseStatsTest extends TestHelper {

  @Test
  void crudStatsCounters() {
    database.getSchema().createDocumentType("StatsDoc").createProperty("name", Type.STRING);

    final Map<String, Object> statsBefore = database.getStats();
    final long createBefore = (long) statsBefore.get("createRecord");
    final long readBefore = (long) statsBefore.get("readRecord");
    final long updateBefore = (long) statsBefore.get("updateRecord");
    final long deleteBefore = (long) statsBefore.get("deleteRecord");
    final long writeTxBefore = (long) statsBefore.get("writeTx");

    // CREATE
    database.begin();
    final MutableDocument doc = database.newDocument("StatsDoc");
    doc.set("name", "test");
    doc.save();
    database.commit();

    Map<String, Object> statsAfter = database.getStats();
    assertThat((long) statsAfter.get("createRecord")).isGreaterThan(createBefore);
    assertThat((long) statsAfter.get("writeTx")).isGreaterThan(writeTxBefore);

    final RID rid = doc.getIdentity();

    // READ
    final long readBeforeLookup = (long) database.getStats().get("readRecord");
    database.lookupByRID(rid, true);
    assertThat((long) database.getStats().get("readRecord")).isGreaterThan(readBeforeLookup);

    // UPDATE
    final long updateBeforeUpdate = (long) database.getStats().get("updateRecord");
    database.begin();
    final MutableDocument mutable = database.lookupByRID(rid, true).asDocument().modify();
    mutable.set("name", "updated");
    mutable.save();
    database.commit();
    assertThat((long) database.getStats().get("updateRecord")).isGreaterThan(updateBeforeUpdate);

    // DELETE
    final long deleteBeforeDelete = (long) database.getStats().get("deleteRecord");
    database.begin();
    database.deleteRecord(database.lookupByRID(rid, true));
    database.commit();
    assertThat((long) database.getStats().get("deleteRecord")).isGreaterThan(deleteBeforeDelete);
  }
}
