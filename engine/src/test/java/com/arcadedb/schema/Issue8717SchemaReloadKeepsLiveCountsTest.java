/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8717: a follower applying a replicated schema entry re-reads the schema through
 * {@code loadIncremental()} (or the {@code load()} fallback), and that used to re-apply the per-bucket record counts
 * of {@code statistics.json} on top of the live counters. The file is written only by a graceful close, so every
 * insert since then vanished from {@code count(*)}, although the records were intact.
 * <p>
 * The stored counts are a start-up shortcut: they are applied when the schema is first loaded, and never again on a
 * live database. A full reload, which rebuilds the bucket instances, carries each live counter over to the new instance.
 */
class Issue8717SchemaReloadKeepsLiveCountsTest extends TestHelper {

  private static final String TYPE     = "Counted";
  private static final int    INITIAL  = 25;
  private static final int    INSERTED = 17;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE, 1);
    insert(INITIAL);
    assertThat(bucket().count()).isEqualTo(INITIAL);
  }

  @Test
  void anIncrementalSchemaReloadKeepsTheCountsOfInsertsMadeSinceTheLastClose() throws Exception {
    // A graceful close writes statistics.json with INITIAL, the state the follower had at its last restart.
    reopenDatabase();
    assertThat(bucket().getCachedRecordCount()).isEqualTo(INITIAL);

    insert(INSERTED);
    assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);

    schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

    assertThat(bucket().getCachedRecordCount()).isEqualTo(INITIAL + INSERTED);
    assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
  }

  @Test
  void aFullSchemaReloadKeepsTheCountsOfInsertsMadeSinceTheLastClose() throws Exception {
    reopenDatabase();
    insert(INSERTED);
    assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);

    schema().load(ComponentFile.MODE.READ_WRITE, true);

    // The full load rebuilds the bucket instance: it inherits the live counter rather than the stale file value, and
    // is not left unknown either, which would cost a rescan of every bucket on the next count(*).
    assertThat(bucket().getCachedRecordCount()).isEqualTo(INITIAL + INSERTED);
    assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
    // Nor are the stale page hints of the close applied to the rebuilt instance: it starts empty and regathers.
    assertThat(bucket().getStatistics().getJSONArray("pages").length()).isZero();
  }

  @Test
  void aReloadDoesNotResurrectAStaleCountOverAnUnknownCounter() throws Exception {
    // After a crash recovery the counter is unknown (-1) and the file still holds the older close's value.
    reopenDatabase();
    insert(INSERTED);
    bucket().setCachedRecordCount(-1);

    schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

    assertThat(bucket().getCachedRecordCount()).isEqualTo(-1L);
    assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
  }

  @Test
  void anIncrementalSchemaReloadKeepsTheLivePageFreeSpaceHints() throws Exception {
    reopenDatabase();
    // Stand-in for the hints this bucket built since the close: statistics.json still carries the ones of the close.
    final JSONArray live = new JSONArray().put(new JSONObject().put("id", 0).put("free", 1234));
    bucket().setPageStatistics(live);

    schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

    assertThat(bucket().getStatistics().getJSONArray("pages").toString()).isEqualTo(live.toString());
  }

  private void insert(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
  }

  private LocalSchema schema() {
    return (LocalSchema) database.getSchema().getEmbedded();
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
  }
}
