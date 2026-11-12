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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11518: an HA node rechecks its copy for the #11258 damages after a schema reload, not only at
 * startup. A full reload builds a new {@link Dictionary} instance, which counts the header page the open wrote for an emptied
 * file as committed, so the "names were lost" fact the open recorded must survive the reload or the recheck reads it as gone.
 */
class Issue11518DamageSurvivesSchemaReloadTest extends TestHelper {

  @Test
  void anEmptiedDictionaryIsStillReportedAfterAFullSchemaReload() throws IOException {
    database.getSchema().createDocumentType("Account").createProperty("acctId", Long.class);
    database.close();

    emptyTheDictionaryFile();
    database = factory.open();
    assertThat(schema().isDictionaryLostForExistingTypes()).isTrue();

    schema().load(ComponentFile.MODE.READ_WRITE, true);

    assertThat(schema().getDictionary().wasHeaderPageRecreated()).as("a new dictionary instance after the reload").isTrue();
    assertThat(schema().isDictionaryLostForExistingTypes()).isTrue();
  }

  @Test
  void aHealthyDictionaryIsNotReportedAfterAFullSchemaReload() throws IOException {
    database.getSchema().createDocumentType("Account").createProperty("acctId", Long.class);

    schema().load(ComponentFile.MODE.READ_WRITE, true);

    assertThat(schema().isDictionaryLostForExistingTypes()).isFalse();
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }

  private void emptyTheDictionaryFile() throws IOException {
    final File[] dictionaryFiles = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith("." + Dictionary.DICT_EXT));
    assertThat(dictionaryFiles).as("the fixture has a dictionary file").hasSize(1);
    try (final RandomAccessFile raf = new RandomAccessFile(dictionaryFiles[0], "rw")) {
      raf.setLength(0);
    }
  }
}
