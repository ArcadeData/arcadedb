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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * #6598 item 1: {@link DatabaseRID#asVertex(boolean)} used to catch every {@code Exception} out of
 * {@code lookupByRID} and rewrap it as {@link RecordNotFoundException}, so a permission denial or a retryable
 * conflict was reported to the caller as "record not found" - a healthy, present record that simply could not be
 * reached for a reason that had nothing to do with it being missing.
 * <p>
 * Its sibling {@link RID#asVertex(boolean)} was never widened this way: it only ever caught {@link ClassCastException}
 * (a RID that names a record which is not a vertex). {@link DatabaseRID#asVertex(boolean)} must behave the same.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6598DatabaseRidAsVertexBroadCatchTest {

  // ---------------------------------------------------------------------------------------------------------
  // Unit level: pins the catch contract directly against a stubbed BasicDatabase, independent of which engine
  // code path happens to raise each exception today.
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aSecurityExceptionFromLookupSurvivesAsVertexUnwrapped() {
    final BasicDatabase database = mock(BasicDatabase.class);
    final DatabaseRID rid = new DatabaseRID(database, 0, 0L);
    final SecurityException denial = new SecurityException("User 'test' is not allowed to readRecord");
    when(database.lookupByRID(any(RID.class), anyBoolean())).thenThrow(denial);

    final Throwable thrown = catchThrowable(() -> rid.asVertex(true));

    assertThat(thrown).as("a permission denial must not be reported as a missing record")
        .isInstanceOf(SecurityException.class)
        .isNotInstanceOf(RecordNotFoundException.class)
        .isSameAs(denial);
  }

  @Test
  void aRetryableConflictFromLookupSurvivesAsVertexUnwrapped() {
    final BasicDatabase database = mock(BasicDatabase.class);
    final DatabaseRID rid = new DatabaseRID(database, 0, 0L);
    final ConcurrentModificationException conflict = new ConcurrentModificationException("retry me");
    when(database.lookupByRID(any(RID.class), anyBoolean())).thenThrow(conflict);

    final Throwable thrown = catchThrowable(() -> rid.asVertex(true));

    assertThat(thrown).as("a retryable conflict must keep being retryable, not become a permanent not-found")
        .isInstanceOf(NeedRetryException.class)
        .isNotInstanceOf(RecordNotFoundException.class)
        .isSameAs(conflict);
  }

  @Test
  void aRecordNotFoundExceptionFromLookupIsRethrownAsIs() {
    final BasicDatabase database = mock(BasicDatabase.class);
    final DatabaseRID rid = new DatabaseRID(database, 0, 0L);
    final RecordNotFoundException notFound = new RecordNotFoundException("Record " + rid + " not found", rid);
    when(database.lookupByRID(any(RID.class), anyBoolean())).thenThrow(notFound);

    final Throwable thrown = catchThrowable(() -> rid.asVertex(true));

    assertThat(thrown).isSameAs(notFound);
  }

  @Test
  void aRecordThatIsNotAVertexStillReportsAsNotFound() {
    // A ClassCastException is the one case asVertex() is meant to translate: the record exists, but is not a
    // Vertex. Preserved exactly as RID.asVertex(boolean) already behaves.
    final BasicDatabase database = mock(BasicDatabase.class);
    final DatabaseRID rid = new DatabaseRID(database, 0, 0L);
    final Document notAVertex = mock(Document.class);
    when(database.lookupByRID(any(RID.class), anyBoolean())).thenAnswer(invocation -> notAVertex);

    final Throwable thrown = catchThrowable(() -> rid.asVertex(true));

    assertThat(thrown).isInstanceOf(RecordNotFoundException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(rid);
  }

  // ---------------------------------------------------------------------------------------------------------
  // End-to-end: the exact scenario reported in #6598 - a principal granted deleteRecord but not readRecord,
  // reaching the vertex through DatabaseRID.asVertex() (the shortcut ordinary caller code uses) rather than
  // lookupByRID directly (which #6586's test used specifically to avoid this bug).
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aPermissionDenialReachedThroughAsVertexIsNotReportedAsRecordNotFound() {
    final String path = "target/databases/Issue6598PermissionProbe";

    final DatabaseFactory factory = new DatabaseFactory(path).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    final Database restricted = factory.create();
    try {
      restricted.transaction(() -> restricted.getSchema().createVertexType("Guarded", 1));

      final RID[] guarded = new RID[1];
      restricted.transaction(() -> {
        final MutableVertex v = restricted.newVertex("Guarded");
        v.save();
        guarded[0] = v.getIdentity();
      });
      assertThat(guarded[0]).as("BaseRecord.upgradeRID must hand back a DatabaseRID for this test to mean anything")
          .isInstanceOf(DatabaseRID.class);

      final Set<SecurityDatabaseUser.ACCESS> granted = EnumSet.of(SecurityDatabaseUser.ACCESS.DELETE_RECORD);
      DatabaseContext.INSTANCE.getContext(restricted.getDatabasePath()).setCurrentUser(userGranting(granted));

      final Throwable thrown = catchThrowable(
          () -> restricted.transaction(() -> guarded[0].asVertex(), false, 1));

      assertThat(thrown).as("a healthy record denied by permissions must not be reported as missing: %s", thrown)
          .isInstanceOf(SecurityException.class)
          .isNotInstanceOf(RecordNotFoundException.class);
    } finally {
      DatabaseContext.INSTANCE.getContext(restricted.getDatabasePath()).setCurrentUser(null);
      restricted.drop();
      factory.close();
    }
  }

  /** A principal that allows the database wholesale and exactly {@code granted} on every file. */
  private static SecurityDatabaseUser userGranting(final Set<SecurityDatabaseUser.ACCESS> granted) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return granted.contains(access);
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
