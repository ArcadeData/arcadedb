/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9763: {@code ALTER DATABASE} undoes its in-memory change when the save fails, because under Raft HA the save
 * is what replicates the setting. A save that fails with {@link TransactionCommittedRemotelyException} is the exception
 * to that: the change IS committed cluster-wide and every node installs it, this one included, so undoing it here
 * would leave this node alone on the previous value.
 */
class Issue9763CommittedRemotelyKeepsSettingTest {
  private static final String DB_PATH = "target/databases/Issue9763CommittedRemotelyKeepsSettingTest";

  private CommittedRemotelyDatabase database;

  /** A database whose configuration save reports "committed cluster-wide, not confirmed locally", as HA can. */
  private static final class CommittedRemotelyDatabase extends LocalDatabase {
    private volatile boolean failSave;

    private CommittedRemotelyDatabase(final String path) {
      super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    }

    @Override
    public void saveConfiguration() throws IOException {
      if (failSave)
        throw new TransactionCommittedRemotelyException("committed by the majority, not confirmed by every peer");
      super.saveConfiguration();
    }
  }

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    database = new CommittedRemotelyDatabase(DB_PATH);
    database.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aSettingCommittedRemotelyStaysInForce() {
    database.failSave = true;

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(TransactionCommittedRemotelyException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
  }
}
