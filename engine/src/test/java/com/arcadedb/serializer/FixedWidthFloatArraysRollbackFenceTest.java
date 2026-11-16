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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12234: a build before 26.11.1 cannot decode the fixed-width float and double arrays (binary types 36 and 37) and,
 * opening a database that holds them, reads them as null and drops them on the record's next update. Every build since 26.8.1
 * refuses at open a dictionary whose file is labelled with a format version above 1, so the first fixed-width value a database
 * holds relabels its dictionary to {@link Dictionary#FIXED_WIDTH_FLOAT_ARRAYS_VERSION} first: such a build then refuses the
 * database instead of losing data.
 */
class FixedWidthFloatArraysRollbackFenceTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("emb", Type.ARRAY_OF_FLOATS);
      database.getSchema().getType("Doc").createProperty("dbl", Type.ARRAY_OF_DOUBLES);
    });
  }

  @Test
  void aDatabaseWithoutFloatArraysKeepsDictionaryVersion1() {
    database.transaction(() -> database.newDocument("Doc").set("name", "plain").save());
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(1);
  }

  @Test
  void theFirstFixedWidthArrayRelabelsTheDictionaryBeforeTheRecordIsCommitted() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").set("emb", new float[] { 1F, 2.5F, -3F }).set("dbl",
          new double[] { 0.1, 0.2 });
      doc.save();
      rid[0] = doc.getIdentity();
      // THE LABEL IS ON DISK BEFORE THE COMMIT THAT WRITES THE VALUE: A CRASH RIGHT AFTER THE COMMIT CANNOT LEAVE THE NEW FORM
      // IN A DATABASE AN OLDER BUILD WOULD STILL OPEN
      assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    });

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    final Document read = database.lookupByRID(rid[0], true).asDocument();
    assertThat((float[]) read.get("emb")).containsExactly(1F, 2.5F, -3F);
    assertThat((double[]) read.get("dbl")).containsExactly(0.1, 0.2);

    // THE RELABELLED DICTIONARY KEEPS WORKING: NEW NAMES, AND A REOPEN AFTER THEM
    database.transaction(() -> database.newDocument("Doc").set("anotherName", 1).set("emb", new float[] { 4F }).save());
    reopenDatabase();
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc WHERE anotherName = 1").nextIfAvailable().<Long>getProperty("c"))
        .isEqualTo(1L);
  }

  @Test
  void withTheFixedWidthFormDisabledTheDictionaryStaysVersion1() {
    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` false");
    database.transaction(() -> database.newDocument("Doc").set("emb", new float[] { 1F, 2F }).save());
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(1);
    assertThat((float[]) database.query("sql", "SELECT emb FROM Doc").nextIfAvailable().getProperty("emb")).containsExactly(1F, 2F);
  }

  /**
   * A database written by 26.11.x holds the fixed-width form under a version 1 dictionary, as does a replica that received the
   * records as pages. The first read of such a value relabels the dictionary too.
   */
  @Test
  void readingAFixedWidthArrayRelabelsADictionaryStillAtVersion1() {
    database.transaction(() -> database.newDocument("Doc").set("emb", new float[] { 7F, 8F }).save());
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);

    // TURN THE CLOCK BACK: THE SAME FILES WITH THE DICTIONARY LABELLED AS 26.11.1 LEFT IT
    database.close();
    final File current = dictionaryFile();
    assertThat(current.renameTo(new File(current.getParentFile(), current.getName().replaceFirst("\\.v\\d+\\.", ".v1.")))).isTrue();
    database = factory.open();
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    assertThat((float[]) database.query("sql", "SELECT emb FROM Doc").nextIfAvailable().getProperty("emb")).containsExactly(7F, 8F);
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
  }

  private File dictionaryFile() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, fileName) -> fileName.endsWith("." + Dictionary.DICT_EXT));
    assertThat(files).hasSize(1);
    return files[0];
  }

  /** The format version an opening build reads, which lives in the file name: {@code dictionary.<id>.<pageSize>.v<N>.dict}. */
  private int dictionaryFileVersion() {
    final String name = dictionaryFile().getName();
    final String prefix = name.substring(0, name.lastIndexOf('.'));
    return Integer.parseInt(prefix.substring(prefix.lastIndexOf('.') + 2));
  }
}
