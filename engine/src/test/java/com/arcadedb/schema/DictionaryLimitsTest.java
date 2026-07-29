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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.DatabaseMetadataException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the boundaries of the schema dictionary: it lives in one page, it never reclaims an entry, and it has to say so when it
 * runs out of room instead of leaking a raw page-boundary error.
 */
class DictionaryLimitsTest extends TestHelper {
  /**
   * Names are padded so a handful of thousands of them, rather than tens of thousands, exhaust the page.
   */
  private static String name(final int i) {
    return ("p" + i).concat("x".repeat(100));
  }

  @Test
  void runningOutOfPageSpaceIsReportedAsADictionaryError() {
    final Dictionary dictionary = database.getSchema().getDictionary();

    int inserted = 0;
    DatabaseMetadataException failure = null;
    try {
      for (int i = 0; i < 100_000; ++i) {
        dictionary.getIdByName(name(i), true);
        ++inserted;
      }
    } catch (final DatabaseMetadataException e) {
      failure = e;
    }

    assertThat(failure).as("the dictionary is bounded by its single page and must eventually refuse a name").isNotNull();
    assertThat(failure.getMessage()).contains("No space left in dictionary file");
    assertThat(failure.getMessage()).contains("items=" + inserted);

    // THE WHOLE PAGE MINUS THE PAGE HEADER AND THE LEGACY COUNTER IS USABLE, AND THE REFUSAL HAPPENS ONLY AT THE VERY END
    assertThat(dictionary.getAvailableSpace()).isLessThan(102 + 1);

    // THE FAILED INSERT ROLLED BACK CLEANLY: EVERYTHING ACCEPTED SO FAR IS STILL RESOLVABLE IN BOTH DIRECTIONS
    assertThat(dictionary.getDictionaryMap()).hasSize(inserted);
    for (int i = 0; i < inserted; ++i) {
      final int id = dictionary.getIdByName(name(i), false);
      assertThat(id).isEqualTo(i);
      assertThat(dictionary.getNameById(id)).isEqualTo(name(i));
    }
  }

  @Test
  void reloadRebuildsBothDirectionsFromThePage() throws Exception {
    final Dictionary dictionary = database.getSchema().getDictionary();

    final int total = 2_000;
    for (int i = 0; i < total; ++i)
      dictionary.getIdByName(name(i), true);

    dictionary.reload();

    assertThat(dictionary.getDictionaryMap()).hasSize(total);
    for (int i = 0; i < total; ++i) {
      assertThat(dictionary.getIdByName(name(i), false)).isEqualTo(i);
      assertThat(dictionary.getNameById(i)).isEqualTo(name(i));
    }
    assertThat(dictionary.getIdByName("neverSeen", false)).isEqualTo(-1);
  }

  @Test
  void renamingAnItemToItsOwnNameIsANoOp() {
    final Dictionary dictionary = database.getSchema().getDictionary();

    database.transaction(() -> {
      dictionary.getIdByName("sameName", true);
      // BEFORE THE GUARD THIS SPUN FOREVER: EVERY indexOf() FOUND THE ENTRY THE PREVIOUS set() HAD JUST REWRITTEN
      dictionary.updateName("sameName", "sameName");
    });

    assertThat(dictionary.getIdByName("sameName", false)).isNotEqualTo(-1);
    assertThat(dictionary.getNameById(dictionary.getIdByName("sameName", false))).isEqualTo("sameName");
  }

  @Test
  void entriesAreNeverReclaimed() {
    final Dictionary dictionary = database.getSchema().getDictionary();

    database.getSchema().createDocumentType("Ephemeral").createProperty("onlyHere", Type.STRING);
    final int size = dictionary.getDictionaryMap().size();

    database.getSchema().dropType("Ephemeral");

    // DROPPING THE TYPE GIVES NOTHING BACK: THE ID SPACE IS APPEND-ONLY BY DESIGN, BECAUSE STORED RECORDS REFERENCE IT BY ID
    assertThat(dictionary.getDictionaryMap()).hasSize(size);
    assertThat(dictionary.getIdByName("onlyHere", false)).isNotEqualTo(-1);
    assertThat(dictionary.getIdByName("Ephemeral", false)).isNotEqualTo(-1);
  }
}
