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
import com.arcadedb.event.BeforeRecordDeleteListener;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9837: {@code Record.delete()} and {@code Database.deleteRecord()} are {@code void}, so they throw away the one answer a
 * caller that COUNTS deletions needs: the engine's {@code false} for a delete a {@code BeforeRecordDeleteListener} vetoed.
 * {@link DatabaseInternal#deleteRecordReportingVeto(Record)} keeps it, under the same read lock {@code deleteRecord} takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9837DeleteRecordReportingVetoTest extends TestHelper {

  private RID create(final String name) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("T").set("name", name).save().getIdentity());
    return rid[0];
  }

  private long count() {
    return database.countType("T", false);
  }

  @Test
  void answersTrueWhenTheRecordIsDeleted() {
    database.getSchema().createDocumentType("T");
    final RID rid = create("a");

    final boolean[] deleted = new boolean[1];
    database.transaction(() -> deleted[0] = ((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)));

    assertThat(deleted[0]).isTrue();
    assertThat(count()).isZero();
  }

  @Test
  void answersFalseAndKeepsTheRecordWhenAListenerVetoesTheDelete() {
    database.getSchema().createDocumentType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> false);
    final RID rid = create("a");

    final boolean[] deleted = {true};
    database.transaction(() -> deleted[0] = ((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)));

    assertThat(deleted[0]).isFalse();
    assertThat(count()).isEqualTo(1L);
  }

  @Test
  void aVetoOfOneRecordDoesNotStopTheNext() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordDeleteListener) record -> !"keep".equals(record.asDocument().getString("name")));
    final RID keep = create("keep");
    final RID drop = create("drop");

    final int[] deleted = new int[1];
    database.transaction(() -> {
      for (final RID rid : new RID[] { keep, drop })
        if (((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)))
          deleted[0]++;
    });

    assertThat(deleted[0]).isEqualTo(1);
    assertThat(count()).isEqualTo(1L);
  }

  @Test
  void anyOtherRefusalIsThrownAsDeleteRecordDoes() {
    database.getSchema().createDocumentType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      throw new IllegalStateException("refused");
    });
    final RID rid = create("a");

    assertThatThrownBy(() -> database.transaction(
        () -> ((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)))).isInstanceOf(
        IllegalStateException.class).hasMessageContaining("refused");
    assertThat(count()).isEqualTo(1L);
  }
}
