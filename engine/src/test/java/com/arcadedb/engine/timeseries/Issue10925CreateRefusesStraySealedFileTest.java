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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10925: CREATE TIMESERIES TYPE adopted any sealed file already sitting at its name, so a type created one statement ago
 * answered rows that belonged to a type dropped before it. The create now refuses it, as the rename already did.
 */
class Issue10925CreateRefusesStraySealedFileTest extends TestHelper {

  private static final String CREATE = "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1";

  @Test
  void createRefusesASealedFileLeftAtItsName() throws Exception {
    final File sealed = new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0));
    Files.write(sealed.toPath(), new byte[] { 1, 2, 3 });

    assertThatThrownBy(() -> database.command("sql", CREATE)).hasMessageContaining(sealed.getName());

    assertThat(database.getSchema().existsType("Ticker")).isFalse();
    assertThat(sealed).as("the stray file is left untouched").exists();
  }

  @Test
  void createSucceedsOnceTheStrayFileIsGone() throws Exception {
    final File sealed = new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0));
    Files.write(sealed.toPath(), new byte[] { 1, 2, 3 });
    assertThatThrownBy(() -> database.command("sql", CREATE)).isInstanceOf(SchemaException.class)
        .hasMessageContaining(sealed.getName());

    assertThat(sealed.delete()).isTrue();
    database.command("sql", CREATE);

    assertThat(database.getSchema().existsType("Ticker")).isTrue();
  }

  @Test
  void dropThenRecreateIsNotRefused() throws Exception {
    database.command("sql", CREATE);
    database.command("sql", "DROP TYPE Ticker");
    assertThat(new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0))).doesNotExist();

    database.command("sql", CREATE);

    assertThat(database.getSchema().existsType("Ticker")).isTrue();
  }

  @Test
  void ifNotExistsOnAnExistingTypeIsANoOp() throws Exception {
    database.command("sql", CREATE);

    database.command("sql", CREATE.replace("Ticker", "Ticker IF NOT EXISTS"));

    assertThat(database.getSchema().existsType("Ticker")).isTrue();
  }

  @Test
  void embeddedBuilderRefusesToo() throws Exception {
    final File sealed = new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0));
    Files.write(sealed.toPath(), new byte[] { 1, 2, 3 });

    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("Ticker").withTimestamp("ts")
        .withTag("name", Type.STRING).withField("f1", Type.DOUBLE).withShards(1).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining(sealed.getName());
    assertThat(database.getSchema().existsType("Ticker")).isFalse();
  }
}
