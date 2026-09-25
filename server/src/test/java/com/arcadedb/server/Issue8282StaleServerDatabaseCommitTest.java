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
package com.arcadedb.server;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8282: a {@link ServerDatabase} resolved before the HA wrap holds the plain {@link LocalDatabase}. After the
 * wrap the server's registry holds a new {@code ServerDatabase} around the replicated wrapper, but the old one - the
 * Postgres, MongoDB, Bolt and gRPC executors keep theirs for the whole connection - used to commit straight through
 * {@code LocalDatabase.commit()}, which never replicates.
 * <p>
 * The wrapper is a {@link Proxy} installed through {@code LocalDatabase.setWrappedDatabaseInstance()}, the call
 * {@code RaftReplicatedDatabase}'s constructor makes. It counts the commits it is asked for and runs them on the real
 * database, which is enough to tell "went through the wrapper" from "went around it".
 */
class Issue8282StaleServerDatabaseCommitTest extends TestHelper {

  private static final String TYPE = "Issue8282Doc";

  @Override
  protected void beginTest() {
    // Registers the property name in the dictionary now: a first use inside a test would add a commit of its own
    // (Dictionary.getIdByName() runs a transaction() through the wrapper) to the count asserted below.
    database.getSchema().createDocumentType(TYPE).createProperty("name", Type.STRING);
  }

  @AfterEach
  void removeWrapper() {
    if (database != null && database.isOpen())
      local().setWrappedDatabaseInstance(local());
  }

  private LocalDatabase local() {
    return (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
  }

  private DatabaseInternal installWrapper(final AtomicInteger commits) {
    final LocalDatabase real = local();
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("commit".equals(method.getName()) && method.getParameterCount() == 0)
            commits.incrementAndGet();
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    real.setWrappedDatabaseInstance(wrapper);
    return wrapper;
  }

  @Test
  void aHandleResolvedBeforeTheWrapCommitsThroughTheWrapper() {
    final ServerDatabase staleHandle = new ServerDatabase(null, local());

    final AtomicInteger commits = new AtomicInteger();
    installWrapper(commits);

    for (int i = 0; i < 3; i++) {
      staleHandle.begin();
      staleHandle.newDocument(TYPE).set("name", "row-" + i).save();
      staleHandle.commit();
    }

    assertThat(commits.get()).as("each commit on the stale handle must reach the current wrapper").isEqualTo(3);
    assertThat(database.countType(TYPE, true)).isEqualTo(3L);
  }

  @Test
  void aHandleResolvedAfterTheWrapStillCommitsThroughTheWrapperOnce() {
    final AtomicInteger commits = new AtomicInteger();
    final ServerDatabase freshHandle = new ServerDatabase(null, installWrapper(commits));

    freshHandle.begin();
    freshHandle.newDocument(TYPE).set("name", "fresh").save();
    freshHandle.commit();

    assertThat(commits.get()).isEqualTo(1);
    assertThat(database.countType(TYPE, true)).isEqualTo(1L);
  }

  @Test
  void aHandleBuiltAroundAReplacedWrapperCommitsThroughTheCurrentOne() {
    // rewrapDatabases() on a plugin restart installs a NEW wrapper on the same LocalDatabase; a handle built around
    // the previous one must follow, not commit through a wrapper the server has discarded.
    final AtomicInteger oldCommits = new AtomicInteger();
    final ServerDatabase handleOnOldWrapper = new ServerDatabase(null, installWrapper(oldCommits));
    final AtomicInteger newCommits = new AtomicInteger();
    installWrapper(newCommits);

    handleOnOldWrapper.begin();
    handleOnOldWrapper.newDocument(TYPE).set("name", "rewrapped").save();
    handleOnOldWrapper.commit();

    assertThat(newCommits.get()).as("the commit must reach the wrapper installed last").isEqualTo(1);
    assertThat(oldCommits.get()).as("and not the one it replaced").isZero();
    assertThat(database.countType(TYPE, true)).isEqualTo(1L);
  }

  @Test
  void aHandleOnAnUnwrappedDatabaseCommitsLocally() {
    final ServerDatabase handle = new ServerDatabase(null, local());

    handle.begin();
    handle.newDocument(TYPE).set("name", "standalone").save();
    handle.commit();

    assertThat(database.countType(TYPE, true)).isEqualTo(1L);
  }
}
