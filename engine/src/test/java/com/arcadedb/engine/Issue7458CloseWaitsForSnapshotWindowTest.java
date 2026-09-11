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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7458: a database close or drop must wait for every open point-in-time snapshot window
 * on that database, instead of closing the files underneath the backup that is reading them. Before the fix the
 * close went ahead, the window's page reads failed on the closed channels, the window was invalidated as FAILED and
 * the backup's retry loop fell back to the frozen-files path against a closed database, which threw - a loud
 * failure and no corrupt archive, but a regression in availability against the read lock that used to make the
 * close simply wait.
 * <p>
 * The property is two-sided: the close waits for the windows already open, and no NEW window can open on a database
 * whose close is waiting - otherwise a stream of backups could postpone the close forever.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7458CloseWaitsForSnapshotWindowTest extends TestHelper {
  private static final String TYPE = "Doc";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("id", Integer.class);
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void closeWaitsUntilTheWindowIsReleased() throws Exception {
    assertCloseWaitsForTheWindow(false);
  }

  @Test
  void dropWaitsUntilTheWindowIsReleased() throws Exception {
    assertCloseWaitsForTheWindow(true);
  }

  @Test
  void noNewWindowOpensWhileACloseIsWaiting() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final PageSnapshot first = pageManager.openSnapshot(db);
    final Thread closer = new Thread(db::close, "closer");
    try {
      closer.start();

      // THE CLOSE HAS NO WAY TO SIGNAL THAT IT IS WAITING, SO PROBE FOR ITS EFFECT: ONCE THE CLOSE IS WAITING, A NEW
      // WINDOW IS REFUSED. UNTIL THEN A WINDOW STILL OPENS (AND IS RELEASED AT ONCE, SO THE CLOSE IS NOT DELAYED)
      PageSnapshotException refusal = null;
      final long deadline = System.currentTimeMillis() + 30_000;
      while (refusal == null && System.currentTimeMillis() < deadline) {
        try (final PageSnapshot probe = pageManager.openSnapshot(db)) {
          assertThat(probe.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
        } catch (final PageSnapshotException e) {
          refusal = e;
        }
        Thread.sleep(10);
      }

      assertThat(refusal).as("a window must be refused once the close is waiting").isNotNull();
      assertThat(refusal.getReason()).isEqualTo(PageSnapshotException.Reason.CLOSING);
      assertThat(closer.isAlive()).as("the close must still be waiting for the first window").isTrue();
      assertThat(db.isOpen()).isTrue();
    } finally {
      first.close();
      closer.join(30_000);
    }

    assertThat(closer.isAlive()).isFalse();
    assertThat(db.isOpen()).isFalse();
  }

  /**
   * A window leaves the registry as the FIRST step of its close and is fully released (shadow closed, retained files
   * deleted) only at the end of it. The close must count it until the end: this drives the two steps by hand, so the
   * gap between them is as wide as the test wants it.
   */
  @Test
  void closeWaitsForAReleaseStillInProgress() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final PageSnapshot snapshot = pageManager.openSnapshot(db);
    // THE FIRST STEP OF PageSnapshot.close(): OUT OF THE REGISTRY, NOT RELEASED YET
    pageManager.unregisterSnapshot(snapshot);
    assertThat(pageManager.isSnapshotWindowOpen(db)).isFalse();

    final Thread closer = new Thread(db::close, "closer");
    try {
      closer.start();
      closer.join(1_000);
      assertThat(closer.isAlive()).as("the close must wait for the release, not only for the unregistration").isTrue();
      assertThat(db.isOpen()).isTrue();
    } finally {
      // THE LAST STEP OF PageSnapshot.close()
      pageManager.snapshotReleased(snapshot);
      closer.join(30_000);
    }

    assertThat(closer.isAlive()).isFalse();
    assertThat(db.isOpen()).isFalse();
    // THE REAL CLOSE FINDS NOTHING LEFT TO UNREGISTER OR TO WAKE
    snapshot.close();
  }

  @Test
  void windowOnAClosedDatabaseIsRefused() {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.close();

    // THE ACCESSOR ON THE DATABASE REFUSES A CLOSED ONE; THE PAGE MANAGER IS JVM-WIDE AND ANSWERS FOR IT
    assertThatThrownBy(() -> PageManager.INSTANCE.openSnapshot(db)).isInstanceOf(PageSnapshotException.class)
        .satisfies(e -> assertThat(((PageSnapshotException) e).getReason()).isEqualTo(PageSnapshotException.Reason.CLOSING));
    assertThat(PageManager.INSTANCE.isSnapshotWindowOpen(db)).isFalse();
  }

  private void assertCloseWaitsForTheWindow(final boolean drop) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final PageSnapshot snapshot = pageManager.openSnapshot(db);
    assertThat(snapshot.getFiles()).isNotEmpty();

    final CountDownLatch started = new CountDownLatch(1);
    final AtomicReference<Throwable> closerFailure = new AtomicReference<>();
    final Thread closer = new Thread(() -> {
      started.countDown();
      try {
        if (drop)
          db.drop();
        else
          db.close();
      } catch (final Throwable e) {
        closerFailure.set(e);
      }
    }, drop ? "dropper" : "closer");

    try {
      closer.start();
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

      // GIVE THE CLOSE EVERY CHANCE TO GO AHEAD: IT MUST NOT
      closer.join(1_000);
      assertThat(closer.isAlive()).as("the " + closer.getName() + " must wait for the open window").isTrue();
      assertThat(db.isOpen()).as("the database must stay open while the window is").isTrue();
      assertThat(pageManager.isSnapshotWindowOpen(db)).isTrue();

      // THE WINDOW STILL SERVES ITS t0 IMAGE WHILE THE CLOSE IS WAITING - THIS IS THE BACKUP FINISHING ITS READ
      long bytes = 0;
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        try (final InputStream in = snapshot.newInputStream(file.fileId())) {
          final byte[] buffer = new byte[8192];
          for (int read; (read = in.read(buffer)) > 0; )
            bytes += read;
        }
      assertThat(bytes).isPositive();
      snapshot.checkValid();
      assertThat(closer.isAlive()).isTrue();
    } finally {
      snapshot.close();
      closer.join(30_000);
    }

    assertThat(closer.isAlive()).as("the " + closer.getName() + " must complete once the window is released").isFalse();
    assertThat(closerFailure.get()).isNull();
    assertThat(db.isOpen()).isFalse();
    assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.CLOSED);
  }
}
