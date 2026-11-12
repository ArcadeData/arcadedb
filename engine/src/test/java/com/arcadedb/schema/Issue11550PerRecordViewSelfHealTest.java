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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A per-record materialized view heals itself after a crash (issue #11550): its pending delta and recovery mark live in memory
 * only, so a commit whose pass had not run when the process died is missing from a view persisted VALID. The open that replays
 * the WAL hands every per-record view the recovery full pass, on the scheduler thread, without a manual REFRESH.
 */
class Issue11550PerRecordViewSelfHealTest extends TestHelper {
  private static final String QUERY = "SELECT name, age FROM Person WHERE age >= 18";

  @Test
  void aCommitWhosePassDiedWithTheProcessReachesTheViewAfterTheReopen() {
    createAdultsView();
    final MaterializedViewImpl view = view();
    assertThat(view.isPerRecord()).isTrue();

    // Own the refresh, as a pass still running would: the commit below only appends its RIDs to the pending delta and asks the
    // owner for a further pass, which the crash then takes down with the delta
    assertThat(view.tryBeginRefresh()).isTrue();
    database.transaction(() -> {
      database.command("sql", "UPDATE Person SET age = 99 WHERE name = 'a3'");
      database.newDocument("Person").set("name", "late").set("age", 40).save();
      database.command("sql", "DELETE FROM Person WHERE name = 'a5'");
    });
    assertThat(viewRows()).as("the pass of the commit has not run").doesNotContain("a3/99", "late/40").contains("a5/23");
    assertThat(view.getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());

    ((DatabaseInternal) database).kill();
    database.close();
    database = factory.open();

    // No REFRESH: the open handed the view its recovery full pass
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view().getStatus()) && viewRows().containsAll(List.of("a3/99", "late/40")));
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    assertThat(viewRows()).doesNotContain("a5/23");

    // And the view keeps its per-record maintenance afterwards
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 77 WHERE name = 'a7'"));
    assertThat(viewRows()).contains("a7/77");
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
  }

  @Test
  void aViewPersistedNonValidConvergesAtTheNextOpenWithoutATrigger() {
    createAdultsView();
    final MaterializedViewImpl view = view();

    // A failed pass leaves the view non-VALID, persisted, and missing the records it drained: simulated by a commit whose pass
    // never ran, followed by the status the failure persists
    assertThat(view.tryBeginRefresh()).isTrue();
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 98 WHERE name = 'a2'"));
    view.drainPendingDelta();
    view.setStatus(MaterializedViewStatus.ERROR);
    MaterializedViewRefresher.persistStatus(database, view.getName(), view.getStatus());
    view.endRefresh();
    assertThat(viewRows()).doesNotContain("a2/98");

    reopenDatabase();

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view().getStatus()) && viewRows().contains("a2/98"));
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));

    // The VALID the recovery pass ended in was persisted
    reopenDatabase();
    assertThat(view().getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
  }

  @Test
  void aCleanReopenOfAValidViewRunsNoPass() {
    createAdultsView();

    reopenDatabase();

    // A recovery asked at the open says STALE before the open returns: a clean close leaves nothing owed, so nothing is asked
    assertThat(view().getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
    assertThat(view().owesRecoveryPass()).isFalse();
    assertThat(view().getRefreshCount()).isZero();
  }

  @Test
  void theLeaderRecoveryAsksEveryPerRecordView() {
    createAdultsView();
    database.transaction(() -> database.getSchema().buildMaterializedView().withName("Manual").withQuery(QUERY)
        .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create());

    // What a node that just took the leadership asks: the per-record view only, the manual one is not maintained on commit
    assertThat(((LocalSchema) database.getSchema()).recoverPerRecordMaterializedViews()).isEqualTo(1);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view().getStatus()) && !view().owesRecoveryPass());
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
  }

  private void createAdultsView() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Person");
      type.createProperty("name", Type.STRING);
      type.createProperty("age", Type.INTEGER);
    });
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("Person").set("name", "a" + i).set("age", 18 + (i % 40)).save();
    });
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("Adults")
        .withQuery(QUERY)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());
  }

  private MaterializedViewImpl view() {
    return (MaterializedViewImpl) database.getSchema().getMaterializedView("Adults");
  }

  private List<String> viewRows() {
    return queryRows("SELECT name, age FROM Adults");
  }

  private List<String> queryRows(final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("age"));
      }
    }
    return rows;
  }
}
