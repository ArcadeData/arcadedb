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
 * Issue #6149: the record PLACEHOLDER is the pre-chunk spill mechanism. Since #332 a record that outgrows its page
 * becomes a chunk chain instead, and the single site that still produces a placeholder pointer fires only when the
 * spilling record's slot cannot host a chunk header - 14 bytes. Those 14 bytes are normally there for the asking: the
 * branch is reached because the page cannot host the record's FULL new size, which says nothing about whether it can
 * spare the five-odd bytes that separate a 9-byte slot from a chunk header. The in-page shift that
 * {@code growRecordInPage} already performs provides them.
 * <p>
 * That matters because the placeholder is the one record shape the disjoint-slot merge cannot replay (the pointer
 * rewrite changes two pages at once) and the one behind the silent lost update of #6141.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6149PlaceholderPrefersChunksTest extends BucketPageLayoutTestSupport {
  /**
   * The issue itself: a 9-byte record on a page that cannot host its new 20 KB value, but has kilobytes of free tail.
   * Before the fix the slot became a placeholder POINTER; now it is grown by the handful of bytes a chunk header
   * needs and the record spills into a chunk chain like every other record of its size.
   */
  @Test
  void aSmallRecordSpillingWithRoomLeftInThePageBecomesAChunkChain() {
    final String big = "b".repeat(20 * 1024);

    final RID[] tiny = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Holder", 1).createProperty("v", Type.STRING);
      // Written FIRST so it is never the last record of the page: a last record would be grown into the free tail.
      tiny[0] = database.newDocument("Holder").set("v", "p").save().getIdentity();
    });
    fillFirstPage("Holder");

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());

    final Map<String, Object> layout = bucketStats("Holder");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("no placeholder must be produced any more: " + layout)
        .isZero();
    assertThat((Long) layout.get("totalSurrogateRecords")).as("and therefore no placeholder content either: " + layout)
        .isZero();
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must have spilled into chunks: " + layout)
        .isEqualTo(1L);

    database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(big));
    checkDatabase();
  }

  /**
   * The record keeps its RID across the spill, and the neighbours the in-page shift moved keep theirs and their
   * content: the shift only changes offsets, which are recomputed from the page's own slot table.
   */
  @Test
  void theShiftThatMakesRoomForTheChunkHeaderPreservesEveryOtherRecord() {
    final String big = "b".repeat(40 * 1024);

    final RID[] tiny = new RID[1];
    final RID[] neighbours = new RID[8];
    final String[] neighbourValues = new String[neighbours.length];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Neighbours", 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument("Neighbours").set("v", "p").save().getIdentity();
      for (int i = 0; i < neighbours.length; i++) {
        neighbourValues[i] = "n" + i + "-" + "y".repeat(16 * (i + 1));
        neighbours[i] = database.newDocument("Neighbours").set("v", neighbourValues[i]).save().getIdentity();
      }
    });
    fillFirstPage("Neighbours");

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());

    database.transaction(() -> {
      assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(big);
      for (int i = 0; i < neighbours.length; i++)
        assertThat(neighbours[i].asDocument(true).getString("v")).as("neighbour " + i + " must be intact")
            .isEqualTo(neighbourValues[i]);
    });

    assertThat((Long) bucketStats("Neighbours").get("totalPlaceholderRecords")).isZero();
    checkDatabase();
  }

  /**
   * The spill keeps working when it is repeated: every small record of a full page is grown past what the page can
   * host, in turn, so each one has to find the bytes for its own chunk header in what is left of the free tail.
   */
  @Test
  void everyRecordOfAPageCanSpillInTurn() {
    final int records = 24;
    final RID[] rids = new RID[records];

    database.transaction(() -> {
      database.getSchema().createDocumentType("AllSpill", 1).createProperty("v", Type.STRING);
      for (int i = 0; i < records; i++)
        rids[i] = database.newDocument("AllSpill").set("v", "r" + i).save().getIdentity();
    });
    fillFirstPage("AllSpill");

    for (int i = 0; i < records; i++) {
      final int index = i;
      database.transaction(() -> rids[index].asDocument(true).modify().set("v", payloadOf(index)).save());
    }

    final Map<String, Object> layout = bucketStats("AllSpill");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("every spill must have found its chunk header: " + layout)
        .isZero();

    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        assertThat(rids[i].asDocument(true).getString("v")).as("record " + i).isEqualTo(payloadOf(i));
    });
    checkDatabase();
  }

  /**
   * The fallback, on the only page shape that still needs it: one with NO free tail at all, which no insert can
   * produce (the allocator always leaves a spare margin) but a spill can - the head chunk of a record that outgrows
   * its page takes its slot's footprint PLUS the whole tail, ending the page exactly at its maximum content size.
   * A 9-byte record on such a page has nowhere to find a chunk header, so it must still become a placeholder.
   * <p>
   * This is what covers the refusal arm of the enlargement test, {@code missing >= freeTailInPage}, at
   * {@code freeTailInPage == 0}. The neighbouring values (a tail that EXISTS but is smaller than the five-odd
   * bytes needed) run the identical comparison with different operands, and cannot be built through the public
   * API: a tail of 1..7 bytes needs byte-exact control of the page geometry, while inserts always leave at least
   * {@code SPARE_SPACE_FOR_GROWTH} (32) bytes and a spill always takes the tail down to exactly 0. A test that
   * guessed at those bytes would assert its own arithmetic rather than the engine's.
   */
  @Test
  void aSmallRecordSpillingOutOfASealedPageStillFallsBackToAPlaceholder() {
    final String big = "b".repeat(20 * 1024);

    final RID[] tiny = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Sealed", 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument("Sealed").set("v", "p").save().getIdentity();
    });
    sealFirstPage("Sealed");

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());

    final Map<String, Object> layout = bucketStats("Sealed");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("a page with no free tail leaves no other option: " + layout)
        .isEqualTo(1L);
    assertThat((Long) layout.get("totalSurrogateRecords")).as("with its content record on another page: " + layout)
        .isEqualTo(1L);

    database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(big));
    checkDatabase();
  }

  /**
   * A placeholder that already exists - the ones every database written before this change contains - is migrated to
   * a chunk chain the first time its content record can no longer absorb an update, because the slot rebuild goes
   * through the very same spill branch. The readers stay, but the stored shape converges on chunks as data is
   * rewritten.
   */
  @Test
  void anExistingPlaceholderIsRebuiltAsAChunkChainWhenTheContentCannotAbsorbTheUpdate() {
    final String big = "b".repeat(20 * 1024);
    final String huge = "h".repeat(200 * 1024);

    final RID[] tiny = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Legacy", 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument("Legacy").set("v", "p").save().getIdentity();
    });
    final RID sealing = sealFirstPage("Legacy");

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());
    assertThat((Long) bucketStats("Legacy").get("totalPlaceholderRecords")).isEqualTo(1L);

    // Free the tail of page 0 again by deleting the record that sealed it, so the rebuild has room for a chunk header.
    database.transaction(() -> database.deleteRecord(sealing.asDocument(true)));

    // 200 KB fits no page at all, so the content record cannot grow: the slot is rebuilt from scratch.
    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", huge).save());

    final Map<String, Object> layout = bucketStats("Legacy");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the rebuilt slot must be a chunk head: " + layout).isZero();
    assertThat((Long) layout.get("totalSurrogateRecords")).as("and the old content record must be gone: " + layout).isZero();
    assertThat((Long) layout.get("totalMultiPageRecords")).isEqualTo(1L);

    database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(huge));
    checkDatabase();
  }

  /**
   * The spill that had to enlarge its slot is still a single-slot write, so the disjoint-slot merge (#5279, #6129)
   * has to replay it on a newer committed version of the page - shift included, since the room the head chunk needs
   * is not on the committed page either. That closes the last gap the placeholder used to leave: this very write was
   * the one shape no merge could absorb, because the pointer rewrite changed two pages at once.
   */
  @Test
  void aSpillThatHadToEnlargeItsSlotIsStillReplayedOnACommittedPage() throws Exception {
    final RID[] tiny = new RID[1];
    final RID[] neighbour = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Merged", 1).createProperty("v", Type.STRING);
      // 9 bytes: too small for a chunk header, which is exactly what used to make this a placeholder.
      tiny[0] = database.newDocument("Merged").set("v", "p").save().getIdentity();
      neighbour[0] = database.newDocument("Merged").set("v", "n").save().getIdentity();
    });
    fillFirstPage("Merged");

    // The premise, proved rather than assumed: with the merge switched OFF the very same interleaving must FAIL,
    // which is what shows the two records really do share a page.
    database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
    try {
      assertThat(spillSurvives(tiny[0], neighbour[0], "a".repeat(200 * 1024), "neighbour without the merge"))//
          .as("the spilling record and the neighbour must share a page").isFalse();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
    }

    final String spilled = "b".repeat(200 * 1024);
    assertThat(spillSurvives(tiny[0], neighbour[0], spilled, "neighbour rewritten")).isTrue();

    final Map<String, Object> layout = bucketStats("Merged");
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must have spilled into chunks: " + layout)
        .isEqualTo(1L);
    assertThat((Long) layout.get("totalPlaceholderRecords")).isZero();

    database.transaction(() -> {
      assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(spilled);
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour rewritten");
    });
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

  private static String payloadOf(final int index) {
    return "p" + index + "-" + "z".repeat(4 * 1024 + index);
  }

}
