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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6175: the continuation chunks of a multi-page record are placed by an allocator that PRIORITIZES the page
 * the previous chunk landed on and then scans the free-space statistics from the lowest page id up - so a chain
 * routinely comes back to the page holding its own HEAD chunk. That page is poisoned on arrival (a continuation chunk
 * is written through inline record-table writes no slot image accounts for), and it is precisely the page #6129 built
 * {@code SLOT_KIND_FIRST_CHUNK}, the chain fingerprint and the region re-derivation to keep mergeable.
 * <p>
 * <b>Measured before changing anything</b>, by logging every continuation-chunk placement of the fixture below (one
 * 9-byte record grown to 200 KB on a page it shares with one neighbour, page size 64 KB):
 * <pre>
 *   chunk 1 -> page 1 (new)     remaining 204811
 *   chunk 2 -> page 2 (new)     remaining 147490
 *   chunk 3 -> page 0  &lt;-- the head chunk's own page, from the statistics scan at half the requested size
 *   chunk 4 -> page 3 (new)     remaining  32874
 * </pre>
 * One spill, one poisoned head page, and the concurrent update of an unrelated record on it turned into a hard
 * conflict. The shape is not exotic: it is what a bucket of mostly-small records with a few large ones looks like,
 * which is also the shape where the merge matters most.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6175ChunkOnHeadPageTest extends BucketPageLayoutTestSupport {
  private static final String BIG = "b".repeat(200 * 1024);

  /**
   * The issue itself: the record spills while sharing its page with another record that a concurrent transaction
   * rewrites. Nothing about that neighbour has anything to do with the chain, so the commit must merge - it did not,
   * because chunk 3 of the chain came home to page 0 and poisoned it.
   */
  @Test
  void aChainThatComesBackToTheHeadChunksPageDoesNotCostItsMerge() throws InterruptedException {
    final RID[] spilling = new RID[1];
    final RID[] neighbour = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Chain", 1).createProperty("v", Type.STRING);
      spilling[0] = database.newDocument("Chain").set("v", "p").save().getIdentity();
      neighbour[0] = database.newDocument("Chain").set("v", "n").save().getIdentity();
    });

    // The premise, proved rather than assumed: with the merge switched OFF the very same interleaving must FAIL,
    // which is what shows the two records really do share a page.
    database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
    try {
      assertThat(spillSurvives(spilling[0], neighbour[0], "a".repeat(200 * 1024), "without the merge"))
          .as("the spilling record and the neighbour must share a page").isFalse();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
    }

    assertThat(spillSurvives(spilling[0], neighbour[0], BIG, "rewritten"))
        .as("the spill must still merge with a concurrent write to another record of the page").isTrue();

    database.transaction(() -> {
      assertThat(spilling[0].asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("rewritten");
    });
    checkDatabase();
  }

  /**
   * The same for an UPDATE of a record that is already a chunk chain, which allocates its extra chunks through the
   * identical call in {@code updateMultiPageRecord}: growing the chain must not poison the head chunk's page either.
   */
  @Test
  void growingAnExistingChainDoesNotCostTheHeadChunksPageEither() throws InterruptedException {
    final RID[] spilling = new RID[1];
    final RID[] neighbour = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Growing", 1).createProperty("v", Type.STRING);
      spilling[0] = database.newDocument("Growing").set("v", "p").save().getIdentity();
      neighbour[0] = database.newDocument("Growing").set("v", "n").save().getIdentity();
    });

    // Already a chunk chain before the contended update, so the growth below goes through updateMultiPageRecord.
    database.transaction(() -> spilling[0].asDocument(true).modify().set("v", "s".repeat(100 * 1024)).save());

    final String grown = "g".repeat(300 * 1024);
    assertThat(spillSurvives(spilling[0], neighbour[0], grown, "rewritten"))
        .as("growing the chain must still merge with a concurrent write to another record of the page").isTrue();

    database.transaction(() -> {
      assertThat(spilling[0].asDocument(true).getString("v")).isEqualTo(grown);
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("rewritten");
    });
    checkDatabase();
  }

  /**
   * What the exclusion must NOT become: a bucket that allocates a fresh page for every continuation chunk. Only the
   * ONE page holding this record's head chunk is off limits, and only while its slot is tracked, so the pages of
   * other records - and of this record's own earlier chunks - are reused exactly as before. Rewriting one chunked
   * record over and over must therefore leave the bucket's page count where it was.
   */
  @Test
  void onlyTheHeadChunksOwnPageIsOffLimits() {
    final RID[] spilling = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Bounded", 1).createProperty("v", Type.STRING);
      spilling[0] = database.newDocument("Bounded").set("v", "p").save().getIdentity();
      database.newDocument("Bounded").set("v", "n").save();
    });

    database.transaction(() -> spilling[0].asDocument(true).modify().set("v", BIG).save());

    final long pagesAfterSpill = (Long) bucketStats("Bounded").get("totalPages");

    for (int i = 0; i < 10; i++) {
      final String value = "v" + i + "-" + "x".repeat(200 * 1024);
      database.transaction(() -> spilling[0].asDocument(true).modify().set("v", value).save());
    }

    final Map<String, Object> layout = bucketStats("Bounded");
    assertThat((Long) layout.get("totalPages"))
        .as("rewriting one chunked record must reuse its chain's pages, not grow the bucket: " + layout)
        .isEqualTo(pagesAfterSpill);
    checkDatabase();
  }

  /**
   * Grows a record until it has to spill out of its page and, before committing, has another thread commit a change
   * to a record sharing that page. Returns true when our commit went through, i.e. the merge absorbed the version
   * bump.
   */
  private boolean spillSurvives(final RID spilling, final RID neighbour, final String value, final String neighbourValue)
      throws InterruptedException {
    database.begin();
    spilling.asDocument(true).modify().set("v", value).save();

    final Thread concurrent = new Thread(
        () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
    concurrent.start();
    concurrent.join();

    try {
      database.commit();
      return true;
    } catch (final ConcurrentModificationException e) {
      return false;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }
}
