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

import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10700: a record growing on a page whose free tail is gone was refused for ever when the bytes the page could
 * give back were HOLES - what the shrinking updates of the same transaction leave behind until the commit packs the
 * page - rather than a record able to move off-page. The growth measured the page by its free tail only, so a page with
 * tens of KB of dead bytes refused a 9-byte pointer, and every retry rebuilt the same page. A growth that does not fit
 * the free tail now packs the page's holes first, and is then judged against the page as the commit would leave it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10700GrowthReclaimsPageHolesTest extends BucketPageLayoutTestSupport {
  private static final String FILLER = "f".repeat(8 * 1024);
  private static final String SEAL   = "s".repeat(70 * 1024);
  private static final String BIG    = "b".repeat(20 * 1024);

  /**
   * The reporter's shape: every filler of a sealed page is shrunk to an empty document and one empty document grows to
   * 20 KB, all in ONE transaction. The page holds ~40 KB of holes, so the growth fits inside it once they are packed:
   * the transaction must commit, the grown record must stay a plain in-page record, and nothing may move off-page.
   */
  @Test
  void aGrowthFitsTheHolesTheSameTransactionLeft() {
    final Layout layout = sealedPageWithEmpties("Holes10700");
    final List<RID> empties = layout.empties();
    final RID sealer = layout.sealer();
    final List<RID> fillers = layout.fillers();
    assertThat(fillers).as("the premise: page 0 holds fillers to shrink").isNotEmpty();
    final long countBefore = countRecordsFromCounter("Holes10700");

    database.transaction(() -> {
      for (final RID filler : fillers)
        emptied(filler);
      empties.getFirst().asDocument(true).modify().set("v", BIG).save();
    });

    assertThat((Long) bucketStats("Holes10700").get("totalPlaceholderRecords"))
        .as("the holes were enough: no record may have moved behind a pointer").isZero();
    assertThat((Long) bucketStats("Holes10700").get("totalMultiPageRecords"))
        .as("the grown record fits the packed page, so only the sealer is a chunk chain").isEqualTo(1L);
    assertThat(countRecordsFromCounter("Holes10700")).isEqualTo(countBefore);
    assertThat(countRecords("Holes10700")).isEqualTo(countBefore);
    checkDatabase();

    database.transaction(() -> {
      assertThat(empties.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
      for (int i = 1; i < empties.size(); i++)
        assertThat(empties.get(i).asDocument(true).has("v")).isFalse();
      for (final RID filler : fillers)
        assertThat(filler.asDocument(true).has("v")).isFalse();
      assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEAL);
    });

    reopenDatabase();
    database.transaction(
        () -> assertThat(database.lookupByRID(empties.getFirst(), true).asDocument().getString("v")).isEqualTo(BIG));
    checkDatabase();
  }

  /**
   * The holes are smaller than the growth, so the record still has to spill - but they are far more than the few bytes
   * a chunk header needs, so it spills into a chunk chain of its own instead of borrowing a pointer's bytes from a
   * neighbour: no live record is moved off its page when packing the page was enough.
   */
  @Test
  void holesSmallerThanTheGrowthStillSpareANeighbourTheMove() {
    final Layout layout = sealedPageWithEmpties("Spill10700");
    final List<RID> empties = layout.empties();
    final RID sealer = layout.sealer();
    final List<RID> fillers = layout.fillers();
    final long countBefore = countRecordsFromCounter("Spill10700");

    database.transaction(() -> {
      emptied(fillers.getFirst());
      empties.getFirst().asDocument(true).modify().set("v", BIG).save();
    });

    assertThat((Long) bucketStats("Spill10700").get("totalPlaceholderRecords"))
        .as("a hole of 8 KB hosts a chunk header: no neighbour may have moved behind a pointer").isZero();
    assertThat((Long) bucketStats("Spill10700").get("totalMultiPageRecords"))
        .as("the grown record spilled into a chunk chain next to the sealer").isEqualTo(2L);
    assertThat(countRecordsFromCounter("Spill10700")).isEqualTo(countBefore);
    assertThat(countRecords("Spill10700")).isEqualTo(countBefore);
    checkDatabase();

    database.transaction(() -> {
      assertThat(empties.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(fillers.getFirst().asDocument(true).has("v")).isFalse();
      for (int i = 1; i < fillers.size(); i++)
        assertThat(fillers.get(i).asDocument(true).getString("v")).isEqualTo(FILLER);
      assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEAL);
    });
  }

  /**
   * Records DELETED in the transaction leave holes too, and their slots must not be handed out again by the same
   * transaction: packing the page for the growth must not shorten the slot table under a slot the transaction freed,
   * or the content record the spill creates could reuse a RID this transaction deleted.
   */
  @Test
  void deletedNeighboursLeaveHolesWhoseSlotsAreNotReused() {
    final Layout layout = sealedPageWithEmpties("Deleted10700");
    final List<RID> empties = layout.empties();
    final RID sealer = layout.sealer();
    final List<RID> fillers = layout.fillers();
    final long countBefore = countRecordsFromCounter("Deleted10700");

    database.transaction(() -> {
      for (final RID filler : fillers)
        database.deleteRecord(filler.asDocument(true));
      // Grows past every hole: it has to spill, and the chunks it creates go through the allocator
      empties.getFirst().asDocument(true).modify().set("v", "x".repeat(200 * 1024)).save();
    });

    assertThat(countRecordsFromCounter("Deleted10700")).isEqualTo(countBefore - fillers.size());
    assertThat(countRecords("Deleted10700")).isEqualTo(countBefore - fillers.size());
    checkDatabase();
    database.transaction(() -> {
      assertThat(empties.getFirst().asDocument(true).getString("v")).isEqualTo("x".repeat(200 * 1024));
      for (final RID filler : fillers)
        assertThat(database.existsRecord(filler)).as("deleted " + filler).isFalse();
      assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEAL);
    });
  }

  /** Page 0 of a single-bucket type: three empty documents (6-8 byte slots), the 8 KB fillers, the sealing record. */
  private record Layout(List<RID> empties, RID sealer, List<RID> fillers) {
  }

  private Layout sealedPageWithEmpties(final String typeName) {
    final List<RID> empties = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
      for (int i = 0; i < 3; i++)
        empties.add(database.newDocument(typeName).save().getIdentity());
    });
    final RID sealer = sealFirstPage(typeName);
    return new Layout(empties, sealer, fillersOfPageZero(empties.getLast(), sealer));
  }

  /** Shrinks {@code rid} to an empty document in the caller's transaction, leaving a hole where its content was. */
  private static void emptied(final RID rid) {
    final MutableDocument document = rid.asDocument(true).modify();
    document.remove("v");
    document.save();
  }

  /** The 8 KB fillers {@code sealFirstPage} put on page 0 between {@code after} and the record that sealed it. */
  private static List<RID> fillersOfPageZero(final RID after, final RID sealer) {
    final List<RID> fillers = new ArrayList<>();
    for (long position = after.getPosition() + 1; position < sealer.getPosition(); position++)
      fillers.add(new RID(after.getBucketId(), position));
    return fillers;
  }
}
