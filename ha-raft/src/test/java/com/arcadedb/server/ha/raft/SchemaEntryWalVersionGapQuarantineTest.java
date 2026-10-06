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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The WAL a schema entry carries is applied page by page like a transaction entry, and a page more than one version
 * ahead of the local copy is the same divergence there: an intermediate write never reached this node. It used to be
 * applied with {@code ignoreErrors=true}, so the page was skipped and every other page of the same transaction was
 * applied - a transaction half on this node, with records pointing at content that never landed and nothing to say
 * so. It must quarantine the database for a resync instead, as a transaction entry does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaEntryWalVersionGapQuarantineTest {
  private static final String DB = "schemaWalGap";

  @TempDir
  private Path          serverDir;
  private LocalDatabase database;

  @BeforeEach
  void setUp() {
    database = (LocalDatabase) new DatabaseFactory(serverDir.resolve(DB).toString()).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Applied", 1);
      database.getSchema().createDocumentType("Behind", 1);
      database.newDocument("Applied").set("v", 1).save();
      database.newDocument("Behind").set("v", 1).save();
    });
  }

  @AfterEach
  void tearDown() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aVersionGapInASchemaEntryWalQuarantinesInsteadOfApplyingHalfTheTransaction() throws Exception {
    final PageId applied = firstPageOf("Applied");
    final PageId behind = firstPageOf("Behind");
    final long appliedVersionBefore = page(applied).getVersion();

    // One buffered transaction: the first page is the next version, the second one skips a version
    final byte[] wal = walOf(pageDelta(applied, (int) appliedVersionBefore + 1), pageDelta(behind, (int) page(behind).getVersion() + 2));

    final AtomicInteger gaps = new AtomicInteger();
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = gaps;
    final DatabaseBoundStateMachine stateMachine = new DatabaseBoundStateMachine(database);

    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(), Collections.emptyMap(),
        List.of(wal), Collections.emptyList());
    assertThatThrownBy(() -> stateMachine.applySchemaEntry(RaftLogEntryCodec.decode(entry), 42L, false))
        .as("a gap is a resync condition, not a page to skip")
        .isInstanceOf(ReplicationException.class);

    assertThat(gaps.get()).isEqualTo(1);
    assertThat(stateMachine.isDatabaseDiverged(DB)).as("the database must be quarantined for a resync").isTrue();
  }

  private PageId firstPageOf(final String type) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(type).getBuckets(false).get(0);
    return new PageId(database, bucket.getFileId(), 0);
  }

  private ImmutablePage page(final PageId pageId) throws Exception {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(pageId.getFileId());
    return database.getPageManager().getImmutablePage(pageId, bucket.getPageSize(), false, false);
  }

  /** A page segment rewriting its first 8 content bytes with themselves, at the given version. */
  private byte[] pageDelta(final PageId pageId, final int version) throws Exception {
    final ImmutablePage current = page(pageId);
    final byte[] content = new byte[8];
    current.readByteArray(0, content);
    final ByteBuffer buffer = ByteBuffer.allocate(6 * Integer.BYTES + content.length);
    buffer.putInt(pageId.getFileId());
    buffer.putInt(pageId.getPageNumber());
    buffer.putInt(BasePage.PAGE_HEADER_SIZE);
    buffer.putInt(BasePage.PAGE_HEADER_SIZE + content.length - 1);
    buffer.putInt(version);
    buffer.putInt(current.getContentSize());
    buffer.put(content);
    return buffer.array();
  }

  private static byte[] walOf(final byte[]... pages) {
    int size = 2 * Long.BYTES + 2 * Integer.BYTES;
    for (final byte[] page : pages)
      size += page.length;
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.putLong(1L);
    buffer.putLong(System.currentTimeMillis());
    buffer.putInt(pages.length);
    buffer.putInt(size);
    for (final byte[] page : pages)
      buffer.put(page);
    return buffer.array();
  }

  /** A state machine with no server attached, whose only database is the one this test opened. */
  private static class DatabaseBoundStateMachine extends ArcadeStateMachine {
    private final DatabaseInternal database;

    DatabaseBoundStateMachine(final DatabaseInternal database) {
      this.database = database;
    }

    @Override
    DatabaseInternal databaseFor(final String databaseName) {
      return database;
    }
  }
}
