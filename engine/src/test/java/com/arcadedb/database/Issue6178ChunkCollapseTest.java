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
 * Issue #6178: once a record had spilled it stayed a chunk chain for the rest of its life. However far it shrank
 * back, {@code updateMultiPageRecord} freed the continuation chunks and left a chain of exactly one chunk - a
 * FIRST_CHUNK marker, a 4-byte size, an 8-byte next pointer that is 0, and the content - costing 13 bytes per record
 * for ever, routing every read and write through the chunk path, and making {@code check()} report a multi-page
 * record that is not one any more.
 * <p>
 * The transition back is the mirror of {@code SLOT_KIND_RECORD_SPILLED_TO_CHUNK}, which exists for exactly the
 * opposite one, and is bounded by the same rule #6163 sizes the head chunk with: the region the slot owns on its own
 * page. A record that no longer fits it stays a chain, because shrinking is not licence to take a neighbour's bytes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6178ChunkCollapseTest extends BucketPageLayoutTestSupport {
  /**
   * The common shape after #6163: a record that spilled as the LAST record of its page owns the whole free tail as
   * its region, so it can shrink a long way back and still fit. It must come back as a plain record, not as a chain
   * of one chunk.
   */
  @Test
  void aRecordShrinkingBackIntoItsRegionBecomesAPlainRecordAgain() {
    database.transaction(() -> database.getSchema().createDocumentType("Shrinking", 1).createProperty("v", Type.STRING));

    final RID last = fillFirstPage("Shrinking");

    // Spills as the page's LAST record: its head chunk takes the record's own footprint plus the whole free tail.
    database.transaction(() -> last.asDocument(true).modify().set("v", "s".repeat(70 * 1024)).save());
    assertThat((Long) bucketStats("Shrinking").get("totalMultiPageRecords")).as("the record must have spilled")
        .isEqualTo(1L);

    final String small = "s".repeat(1024);
    database.transaction(() -> last.asDocument(true).modify().set("v", small).save());

    final Map<String, Object> layout = bucketStats("Shrinking");
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the record no longer needs a chain: " + layout).isZero();
    assertThat((Long) layout.get("totalChunks")).as("and no chunk must be left behind: " + layout).isZero();

    database.transaction(() -> assertThat(last.asDocument(true).getString("v")).isEqualTo(small));
    checkDatabase();
  }

  /**
   * The other end of the same rule: a record whose slot sits in the MIDDLE of its page owns only the handful of bytes
   * its own footprint covers - 14 of them when the spill had to enlarge the slot to reach a chunk header - so it
   * collapses only when it shrinks back inside those, and stays a chain for anything larger.
   */
  @Test
  void aRecordInTheMiddleOfItsPageCollapsesOnlyWithinItsOwnFootprint() {
    final RID[] tiny = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Middle", 1).createProperty("v", Type.STRING);
      // Written FIRST so it is never the last record of the page, hence never given the free tail.
      tiny[0] = database.newDocument("Middle").set("v", "p").save().getIdentity();
    });
    fillFirstPage("Middle");

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());
    assertThat((Long) bucketStats("Middle").get("totalMultiPageRecords")).isEqualTo(1L);

    // Still far larger than the 14 bytes the slot owns: the record stays a chunk chain.
    final String stillBig = "m".repeat(20 * 1024);
    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", stillBig).save());

    Map<String, Object> layout = bucketStats("Middle");
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("a record that does not fit its own slot must stay a chain: " + layout).isEqualTo(1L);
    database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(stillBig));

    // Back to what it was when it was created: now it fits, and the chain goes.
    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "p").save());

    layout = bucketStats("Middle");
    assertThat((Long) layout.get("totalMultiPageRecords")).as("back inside its slot, it is a plain record: " + layout)
        .isZero();
    assertThat((Long) layout.get("totalChunks")).as("with the whole chain freed: " + layout).isZero();

    database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo("p"));
    checkDatabase();
  }

  /**
   * The collapse is a single-slot write like the spill it undoes, so it has to be replayable: a concurrent update of
   * another record on the same page must still merge instead of failing the transaction.
   * <p>
   * This is also where the collapse meets #6175, and the reason no separate test is needed for the pair. Freeing the
   * chain poisons every page it visits, so a chain that had come home to the head chunk's own page would poison the
   * very page this replay needs - which is exactly what #6175 stops. Measured on this fixture: with the exclusion
   * disabled the chain runs 1, 2, 0, 3 (chunk 3 lands on the head page); with it, 1, 2, 3, 4. Verified reaching
   * {@code rebaseRecordOnPage} with {@code SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD} rather than merely committing, so the
   * new replay is what this passes through and not some other path.
   */
  @Test
  void theCollapseIsReplayedOnACommittedPage() throws Exception {
    final RID[] collapsing = new RID[1];
    final RID[] neighbour = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Merged", 1).createProperty("v", Type.STRING);
      collapsing[0] = database.newDocument("Merged").set("v", "p").save().getIdentity();
      neighbour[0] = database.newDocument("Merged").set("v", "n").save().getIdentity();
    });

    database.transaction(() -> collapsing[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());
    assertThat((Long) bucketStats("Merged").get("totalMultiPageRecords")).isEqualTo(1L);

    // The premise, proved rather than assumed: with the merge switched OFF the same interleaving must FAIL, which is
    // what shows the two records really do share a page.
    database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
    try {
      assertThat(collapseSurvives(collapsing[0], neighbour[0], "q", "without the merge"))
          .as("the collapsing record and the neighbour must share a page").isFalse();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
    }

    // Re-spilled by the rolled-back attempt? No - the failed transaction changed nothing. Collapse it for real.
    assertThat(collapseSurvives(collapsing[0], neighbour[0], "p", "rewritten")).isTrue();

    final Map<String, Object> layout = bucketStats("Merged");
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the collapse must have gone through: " + layout).isZero();

    database.transaction(() -> {
      assertThat(collapsing[0].asDocument(true).getString("v")).isEqualTo("p");
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("rewritten");
    });
    checkDatabase();
  }

  /**
   * A concurrent update of the SAME record is still a conflict, collapse or no collapse: the chain this transaction
   * is about to free is the one it read, and the pre-image plus the chain fingerprint have to say so.
   */
  @Test
  void aConcurrentUpdateOfTheSameRecordStillConflicts() throws Exception {
    final RID[] collapsing = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Contended", 1).createProperty("v", Type.STRING);
      collapsing[0] = database.newDocument("Contended").set("v", "p").save().getIdentity();
      database.newDocument("Contended").set("v", "n").save();
    });

    database.transaction(() -> collapsing[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());

    database.begin();
    collapsing[0].asDocument(true).modify().set("v", "p").save();

    final Thread concurrent = new Thread(
        () -> database.transaction(() -> collapsing[0].asDocument(true).modify().set("v", "c".repeat(200 * 1024)).save()));
    concurrent.start();
    concurrent.join();

    try {
      database.commit();
      throw new AssertionError("two transactions rewriting the same record must conflict");
    } catch (final ConcurrentModificationException expected) {
      // THE CONTRACT
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    database.transaction(() -> assertThat(collapsing[0].asDocument(true).getString("v")).isEqualTo("c".repeat(200 * 1024)));
    checkDatabase();
  }

  /**
   * The other head chunk, and the last transition between the two shapes that was missing (#6286): the CONTENT record
   * of a placeholder, which {@code createRecordInternal} spills into a chain of its own whenever no page can host it
   * whole. It used to keep that chain however far it shrank, because the collapse could only write a plain POSITIVE
   * size marker and a positive marker is exactly what tells {@code scan()} that a slot holds a document of its own.
   * Writing the NEGATED size the shape is recognised by is all that was needed: the chain goes, and the content stays
   * reachable through its pointer and through nothing else.
   */
  @Test
  void aPlaceholderContentShrinkingBackIntoItsRegionBecomesAContentRecord() {
    final RID[] placeholder = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Surrogate", 1).createProperty("v", Type.STRING);
      placeholder[0] = database.newDocument("Surrogate").set("v", "p").save().getIdentity();
    });
    // A page with no free tail at all is the only shape that still produces a placeholder pointer (#6149).
    sealFirstPage("Surrogate");

    // 200 KB fits no page whole, so the content record created behind the pointer spills into a chain itself.
    final String huge = "h".repeat(200 * 1024);
    database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", huge).save());

    Map<String, Object> layout = bucketStats("Surrogate");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the fixture must really be a placeholder: " + layout)
        .isEqualTo(1L);
    // The placeholder's content is a chain of its own - which is what makes this fixture the one this test needs, and
    // not merely a placeholder with a plain content record. Since #6196 that chain is counted as the SURROGATE it is,
    // next to the one multi-page record the bucket really has: the record that sealed page 0 when it spilled.
    assertThat((Long) layout.get("totalSurrogateRecords"))
        .as("the placeholder's content must itself have spilled into a chain: " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("and it is not a multi-page record of its own: " + layout).isEqualTo(1L);

    final long recordsBefore = countRecords("Surrogate");
    // Exactly one since #6196 gave the head chunk of a content record a marker of its own: before that,
    // createRecordInternal wrote FIRST_CHUNK where the negative size marker that hides a content record belonged, and
    // a scan handed these bytes out twice - once through the pointer, once as a document in their own right.
    final long copiesBefore = countRecordsHolding("Surrogate", huge);
    assertThat(copiesBefore).as("a chunked placeholder content is scanned once, through its pointer").isEqualTo(1L);

    final long chunksBefore = (Long) layout.get("totalChunks");
    assertThat(chunksBefore).as("the content chain must really have continuation chunks: " + layout).isPositive();

    // Back to a handful of bytes: the content record's own chain collapses into the negated-size shape.
    database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", "p").save());

    layout = bucketStats("Surrogate");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer must still be a pointer: " + layout)
        .isEqualTo(1L);
    assertThat((Long) layout.get("totalSurrogateRecords"))
        .as("its content is still a surrogate, now a plain one: " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("and the only multi-page record left is the one that sealed page 0: " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalChunks")).as("the content record's own chain must be gone: " + layout)
        .isLessThan(chunksBefore);
    assertThat(countRecords("Surrogate")).as("and the collapse must not have added a record of its own")
        .isEqualTo(recordsBefore);
    // What the negated marker is worth: a plain POSITIVE size marker is precisely what makes a slot a document in its
    // own right - the duplication #6196 removed, which a collapse writing the wrong sign would reintroduce on the one
    // record shape that reaches this branch behind a pointer.
    assertThat(countRecordsHolding("Surrogate", "p")).as("the collapse must not multiply the placeholder's content")
        .isEqualTo(copiesBefore);

    database.transaction(() -> assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo("p"));

    // And it survives the round trip: grown back past its page it becomes a chain again, and shrunk again it collapses
    // again, all through the same pointer.
    database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", huge).save());
    database.transaction(() -> assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo(huge));
    database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", "p").save());
    database.transaction(() -> assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo("p"));
    assertThat(countRecordsHolding("Surrogate", "p")).as("still exactly one copy after the round trip")
        .isEqualTo(copiesBefore);

    checkDatabase();
  }

  /**
   * Shrinks {@code collapsing} back inside its slot and, before committing, has another thread commit a change to a
   * record sharing that page. Returns true when our commit went through.
   */
  private boolean collapseSurvives(final RID collapsing, final RID neighbour, final String value,
      final String neighbourValue) throws InterruptedException {
    database.begin();
    collapsing.asDocument(true).modify().set("v", value).save();

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
