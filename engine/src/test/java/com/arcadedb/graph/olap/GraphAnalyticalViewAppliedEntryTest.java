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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12250, through the real apply: an entry that reaches the database as pages (a follower applying the replicated log, the
 * leader applying a commit a follower made in its own transaction) goes through
 * {@code TransactionManager.applyChanges()}, which tells the graph analytical views when the entry carried content the database
 * did not have. {@link GraphAnalyticalViewReplicatedCommitTest} drives the registry with synthetic entries; this one pins the
 * wiring, and the rule that decides when it notifies: a page written at a version newer than the one on disk does (a page the
 * bucket did not hold yet included), an entry replayed at the version already applied (the torn-write repair) or older than it
 * does not, because it changes nothing a view serves and a view that rebuilt since would otherwise go STALE again for nothing. An
 * entry whose apply fails after publishing a page does too: the pages are written one by one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewAppliedEntryTest extends TestHelper {
  private RID vertexRid;
  private RID documentRid;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("V").save();
      vertexRid = hub.getIdentity();
      for (int i = 0; i < 10; i++)
        hub.newEdge("E", database.newVertex("V").save());
      documentRid = database.newDocument("Doc").save().getIdentity();
    });
  }

  @Test
  void anEntryAppliedWithNewContentMakesTheViewStaleBeforeTheApplyReturns() throws IOException {
    final GraphAnalyticalView view = createView();
    final int vertexBucket = vertexRid.getBucketId();
    final int documentBucket = documentRid.getBucketId();

    // A page of a document type's bucket changes nothing the view serves
    assertThat(apply(entry(documentBucket, pageVersion(documentBucket) + 1))).isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    assertThat(apply(entry(vertexBucket, pageVersion(vertexBucket) + 1))).isTrue();
    assertThat(view.getStatus()).as("STALE as soon as the apply returned, before anything queried the view")
        .isEqualTo(GraphAnalyticalView.Status.STALE);

    // A query takes the record path, and asks for the rebuild that serves the next one
    assertThat(edges()).isEqualTo(10);
    awaitReady(view);
    assertThat(view.getEdgeCount()).isEqualTo(10);
  }

  @Test
  void anEntryAlreadyAppliedDoesNotMakeTheViewStaleAgain() throws IOException {
    final GraphAnalyticalView view = createView();
    final int vertexBucket = vertexRid.getBucketId();
    final int base = pageVersion(vertexBucket);
    final WALFile.WALTransaction entry = entry(vertexBucket, base + 1);

    assertThat(apply(entry)).isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    assertThat(edges()).isEqualTo(10);
    awaitReady(view);

    // The same entry again, at the version the page holds now: written once more, as a torn page is repaired, with no new content
    assertThat(apply(entry)).as("the pages are written again").isTrue();
    assertThat(view.getStatus()).as("nothing new to tell the view").isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.isReady()).isTrue();

    // An entry older than the page is skipped
    assertThat(apply(entry(vertexBucket, base))).as("superseded: nothing written").isFalse();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    // The next entry on the page is new content again
    assertThat(apply(entry(vertexBucket, base + 2))).isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
  }

  /** The first version of a page the bucket does not hold yet is newer than anything the database has for it: a bucket that grows. */
  @Test
  void anEntryAppendingAPageToABucketMakesTheViewStale() {
    final GraphAnalyticalView view = createView();
    final int vertexBucket = vertexRid.getBucketId();
    final int pageNumber = ((PaginatedComponent) database.getSchema().getFileById(vertexBucket)).getTotalPages();

    assertThat(apply(entry(newPage(vertexBucket, pageNumber)))).isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
  }

  /** The pages of an entry are written one by one: one that stops part-way may have published some, and the views are told. */
  @Test
  void anApplyThatFailsAfterPublishingAPageStillMakesTheViewStale() throws IOException {
    final GraphAnalyticalView view = createView();
    final int vertexBucket = vertexRid.getBucketId();
    final int documentBucket = documentRid.getBucketId();
    final int vertexVersion = pageVersion(vertexBucket);
    // The first page applies, the second one is two versions ahead of what the database holds: the apply stops there
    final WALFile.WALTransaction entry = entry(existingPage(vertexBucket, vertexVersion + 1),
        existingPage(documentBucket, pageVersion(documentBucket) + 2));

    assertThatThrownBy(() -> apply(entry)).isInstanceOf(WALVersionGapException.class);

    assertThat(pageVersion(vertexBucket)).as("the first page was published").isEqualTo(vertexVersion + 1);
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    assertThat(edges()).isEqualTo(10);
    awaitReady(view);
  }

  private GraphAnalyticalView createView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private static void awaitReady(final GraphAnalyticalView view) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> view.getStatus() == GraphAnalyticalView.Status.READY);
  }

  private boolean apply(final WALFile.WALTransaction entry) {
    return ((DatabaseInternal) database).getTransactionManager().applyChanges(entry, Collections.emptyMap(), false);
  }

  private long edges() {
    try (final ResultSet rs = database.query("sql", "SELECT sum(out('E').size()) AS c FROM V")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private ImmutablePage firstPage(final int bucketId) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    return db.getPageManager().getImmutablePage(new PageId(db, bucketId, 0), pageSize, false, true);
  }

  private int pageVersion(final int bucketId) throws IOException {
    return (int) firstPage(bucketId).getVersion();
  }

  /** A one-page entry for page 0 of the bucket at {@code version}, see {@link #existingPage}. */
  private WALFile.WALTransaction entry(final int bucketId, final int version) throws IOException {
    return entry(existingPage(bucketId, version));
  }

  private static WALFile.WALTransaction entry(final WALFile.WALPage... pages) {
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = 1L;
    tx.timestamp = System.currentTimeMillis();
    tx.pages = pages;
    return tx;
  }

  /**
   * Page 0 of the bucket at {@code version}, rewriting the first bytes of its content with their current value: what the page
   * holds does not change, only its version does.
   */
  private WALFile.WALPage existingPage(final int bucketId, final int version) throws IOException {
    final ImmutablePage page = firstPage(bucketId);
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), BasePage.PAGE_HEADER_SIZE, content, 0, content.length);
    return walPage(bucketId, 0, version, page.getContentSize(), content);
  }

  /** A page the bucket does not hold yet, at its first version, empty: no records. */
  private static WALFile.WALPage newPage(final int bucketId, final int pageNumber) {
    return walPage(bucketId, pageNumber, 1, 11, new byte[11]);
  }

  private static WALFile.WALPage walPage(final int bucketId, final int pageNumber, final int version, final int contentSize,
      final byte[] content) {
    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = bucketId;
    walPage.pageNumber = pageNumber;
    walPage.currentPageVersion = version;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + content.length - 1;
    walPage.currentPageSize = contentSize;
    walPage.currentContent = new Binary(content);
    return walPage;
  }
}
