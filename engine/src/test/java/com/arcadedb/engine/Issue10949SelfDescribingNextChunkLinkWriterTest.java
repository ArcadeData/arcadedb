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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10949: step 2 of the rollout of the self-describing next-chunk link {@code -(position + 1)} (#10948). A
 * database-scoped setting, OFF by default, makes the link writers store the new form instead of the absolute position.
 * Every reader decodes both forms since #10853, so a chain written with the setting ON must read, update, delete and
 * check clean, and a chain holding both forms must behave the same.
 */
class Issue10949SelfDescribingNextChunkLinkWriterTest extends BucketPageLayoutTestSupport {

  @Test
  void theSettingIsOffByDefault() {
    assertThat(GlobalConfiguration.BUCKET_SELF_DESCRIBING_CHUNK_LINKS.getDefValue()).isEqualTo(false);
    assertThat(GlobalConfiguration.BUCKET_SELF_DESCRIBING_CHUNK_LINKS.getScope()).isEqualTo(GlobalConfiguration.SCOPE.DATABASE);
  }

  @Test
  void withTheSettingOffEveryLinkIsTheLegacyAbsolutePosition() {
    final RID[] rids = createChunkedRecords("Off");
    for (final RID rid : rids) {
      final List<Long> links = storedLinksOf(rid);
      assertThat(links).as("the fixture needs continuation chunks").isNotEmpty();
      assertThat(links).as("legacy links of " + rid).allMatch(link -> link > 0L);
    }
    assertCleanCheck("Off");
  }

  @Test
  void withTheSettingOnEveryNewLinkIsSelfDescribingAndTheChainRoundTrips() {
    enableSelfDescribingLinks(true);
    final RID[] rids = createChunkedRecords("On");

    for (final RID rid : rids) {
      final List<Long> links = storedLinksOf(rid);
      assertThat(links).as("the fixture needs continuation chunks").isNotEmpty();
      assertThat(links).as("self-describing links of " + rid).allMatch(link -> link < 0L);
    }
    assertPayloads(rids, 'x');
    assertCleanCheck("On");

    // GROW (extends the chain), SHRINK (still a chain), COLLAPSE (back into a plain record), then a same-size rewrite
    final String grown = "g".repeat(200_000);
    final String shrunk = "s".repeat(12_000);
    database.transaction(() -> rids[0].asDocument(true).modify().set("payload", grown).save());
    database.transaction(() -> rids[1].asDocument(true).modify().set("payload", shrunk).save());
    database.transaction(() -> rids[2].asDocument(true).modify().set("payload", "tiny").save());
    database.transaction(() -> {
      assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(grown);
      assertThat(rids[1].asDocument(true).getString("payload")).isEqualTo(shrunk);
      assertThat(rids[2].asDocument(true).getString("payload")).isEqualTo("tiny");
    });
    assertThat(storedLinksOf(rids[0])).as("the extension is linked in the new form").isNotEmpty().allMatch(link -> link < 0L);
    assertCleanCheck("On");

    // DELETE: every chunk, reached through the new links, is freed
    database.transaction(() -> {
      for (int i = 3; i < rids.length; i++)
        rids[i].asDocument(true).delete();
    });
    database.transaction(() -> rids[0].asDocument(true).delete());
    assertCleanCheck("On");
  }

  @Test
  void aRecordCreatedAsAChainWithTheSettingOnIsLinkedInTheNewForm() {
    enableSelfDescribingLinks(true);
    final RID[] record = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Created", 1).createProperty("v", Type.STRING);
      record[0] = database.newDocument("Created").set("v", "x".repeat(150_000)).save().getIdentity();
    });

    assertThat(storedLinksOf(record[0])).isNotEmpty().allMatch(link -> link < 0L);
    database.transaction(() -> assertThat(record[0].asDocument(true).getString("v")).isEqualTo("x".repeat(150_000)));
    assertCleanCheck("Created");
  }

  @Test
  void aChainHoldingBothFormsBehavesLikeAnyOther() {
    final RID[] rids = createChunkedRecords("Mixed");
    final int legacyLinks = storedLinksOf(rids[0]).size();

    // Flip the setting on a database that already holds legacy chains, then extend one: new tail, old head
    enableSelfDescribingLinks(true);
    final String grown = "g".repeat(200_000);
    database.transaction(() -> rids[0].asDocument(true).modify().set("payload", grown).save());

    final List<Long> links = storedLinksOf(rids[0]);
    assertThat(links.size()).isGreaterThan(legacyLinks);
    assertThat(links).as("the links the extension wrote are new, the ones it kept are legacy").anyMatch(link -> link > 0L)
        .anyMatch(link -> link < 0L);

    database.transaction(() -> assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(grown));
    assertCleanCheck("Mixed");

    // Back to OFF: a rewrite of the mixed chain still reads and checks clean, then deletes whole
    enableSelfDescribingLinks(false);
    final String regrown = "h".repeat(250_000);
    database.transaction(() -> rids[0].asDocument(true).modify().set("payload", regrown).save());
    database.transaction(() -> assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(regrown));
    assertCleanCheck("Mixed");
    database.transaction(() -> rids[0].asDocument(true).delete());
    assertCleanCheck("Mixed");
  }

  @Test
  void position0IsLinkableOnlyInTheNewForm() {
    // OFF: the #10701 refusal stays
    assertThatThrownBy(() -> LocalBucket.nextChunkLink(null, "b", 0, 0, 2048, false)).isInstanceOf(DatabaseOperationException.class);
    assertThat(LocalBucket.nextChunkLink(null, "b", 3, 7, 2048, false)).isEqualTo(3L * 2048 + 7);

    // ON: position 0 can be named, every other position decodes back to itself
    assertThat(LocalBucket.nextChunkLink(null, "b", 0, 0, 2048, true)).isEqualTo(-1L);
    assertThat(LocalBucket.decodeNextChunkLink(LocalBucket.nextChunkLink(null, "b", 0, 0, 2048, true))).isZero();
    assertThat(LocalBucket.decodeNextChunkLink(LocalBucket.nextChunkLink(null, "b", 3, 7, 2048, true))).isEqualTo(3L * 2048 + 7);
  }

  private void enableSelfDescribingLinks(final boolean enabled) {
    ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.BUCKET_SELF_DESCRIBING_CHUNK_LINKS, enabled);
  }

  /** The next-chunk link fields of the chain headed at {@code head}, as stored, head chunk first, end of chain excluded. */
  private List<Long> storedLinksOf(final RID head) {
    final List<Long> links = new ArrayList<>();
    database.transaction(() -> {
      RID current = head;
      while (true) {
        final RID at = current;
        final long link = onSlot(at, page -> {
          final int offset = recordOffsetOf(page, at);
          return page.readLong((int) (offset + page.readNumberAndSize(offset)[1]) + Binary.INT_SERIALIZED_SIZE);
        });
        if (link == 0L)
          return;
        links.add(link);
        current = new RID(head.getBucketId(), LocalBucket.decodeNextChunkLink(link));
      }
    });
    return links;
  }

  private void assertPayloads(final RID[] rids, final char filler) {
    database.transaction(() -> {
      for (int i = 0; i < rids.length; i++)
        assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(payload(i, filler));
    });
  }

  private void assertCleanCheck(final String typeName) {
    assertThat((Long) bucketStats(typeName).get("totalErrors")).isZero();
    assertThat((Long) bucketStats(typeName).get("orphanedChunks")).isZero();
    checkDatabase();
  }
}
