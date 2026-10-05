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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9253: an insert into a NOTUNIQUE_HASH index decoded the size of every entry of every full
 * overflow page it walked past, so a load got slower with every record already indexed. A page now remembers, in the
 * header, that it holds no dead space, and the walk moves past it with one free-space check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9253HashIndexOverflowWalkTest extends TestHelper {
  private static final int KEYS = 100;

  @Test
  void fullOverflowPagesAreMarkedAsHavingNoDeadSpace() throws IOException {
    createType();
    load(30_000, 0);

    assertThat(countPagesMarkedWithoutDeadSpace()).as("pages known to hold no dead space after the load").isGreaterThan(0);
    verifyCounts(30_000, new int[KEYS]);
  }

  @Test
  void deletesAndReinsertsAfterTheMarkKeepTheIndexConsistent() {
    createType();
    final List<RID> rids = load(20_000, 0);

    // removing records leaves holes in pages that were already marked: the mark must go, or the holes are never reused
    final Random rnd = new Random(9253);
    Collections.shuffle(rids, rnd);
    final int[] deleted = new int[KEYS];
    database.begin();
    for (int i = 0; i < rids.size() / 2; i++) {
      final RID rid = rids.get(i);
      deleted[((Number) rid.asDocument().get("g")).intValue()]++;
      rid.asDocument().delete();
      if ((i + 1) % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    load(20_000, 20_000);
    verifyCounts(40_000, deleted);
  }

  @Test
  void pagesWithoutTheMarkAreCheckedAndMarkedAgain() throws IOException {
    createType();
    load(20_000, 0);
    final int marked = countPagesMarkedWithoutDeadSpace();
    assertThat(marked).isGreaterThan(0);

    // files written before the mark existed have it clear on every page
    clearMarks();
    assertThat(countPagesMarkedWithoutDeadSpace()).isZero();

    load(20_000, 20_000);
    assertThat(countPagesMarkedWithoutDeadSpace()).isGreaterThan(0);
    verifyCounts(40_000, new int[KEYS]);
  }

  private void clearMarks() {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.transaction(() -> {
      for (final IndexInternal sub : ((TypeIndex) db.getSchema().getIndexByName("H[g]")).getIndexesOnBuckets()) {
        final HashIndexBucket bucket = ((HashIndex) sub).bucket;
        final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId())).getPageSize();
        for (int p = 2; p < bucket.getTotalPages(); p++)
          try {
            final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, bucket.getFileId(), p), pageSize, false);
            final int depthAndFlag = page.readShort(HashIndexBucket.BUCKET_LOCAL_DEPTH) & 0xFFFF;
            page.writeShort(HashIndexBucket.BUCKET_LOCAL_DEPTH, (short) (depthAndFlag & HashIndexBucket.LOCAL_DEPTH_MASK));
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
      }
    });
  }

  private void createType() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE H");
      database.command("sql", "CREATE PROPERTY H.g LONG");
      database.command("sql", "CREATE INDEX ON H (g) NOTUNIQUE_HASH");
    });
  }

  private List<RID> load(final int records, final int from) {
    final List<RID> rids = new ArrayList<>(records);
    database.begin();
    for (int i = 0; i < records; i++) {
      final var doc = database.newDocument("H").set("g", (long) ((from + i) % KEYS));
      doc.save();
      rids.add(doc.getIdentity());
      if ((i + 1) % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    return rids;
  }

  private int countPagesMarkedWithoutDeadSpace() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    int marked = 0;
    for (final IndexInternal sub : ((TypeIndex) db.getSchema().getIndexByName("H[g]")).getIndexesOnBuckets()) {
      final HashIndexBucket bucket = ((HashIndex) sub).bucket;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId())).getPageSize();
      // pages 0 and 1 are the metadata and the directory
      for (int p = 2; p < bucket.getTotalPages(); p++) {
        final BasePage page = db.getPageManager().getImmutablePage(new PageId(db, bucket.getFileId(), p), pageSize, false, false);
        if ((page.readShort(HashIndexBucket.BUCKET_LOCAL_DEPTH) & HashIndexBucket.NO_DEAD_SPACE_FLAG) != 0)
          marked++;
      }
    }
    return marked;
  }

  private void verifyCounts(final int records, final int[] deleted) {
    long total = 0;
    for (int g = 0; g < KEYS; g++) {
      final long viaIndex = count("SELECT count(*) AS c FROM H WHERE g = " + g);
      final long viaScan = count("SELECT count(*) AS c FROM H WHERE g + 0 = " + g);
      assertThat(viaIndex).as("index count for g=" + g).isEqualTo(viaScan);
      total += viaIndex;
    }
    assertThat(total).isEqualTo(count("SELECT count(*) AS c FROM H"));
    long expectedTotal = records;
    for (final int d : deleted)
      expectedTotal -= d;
    assertThat(total).isEqualTo(expectedTotal);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
