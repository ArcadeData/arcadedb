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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the boundaries of the schema dictionary: entries are never reclaimed, and both directions of the mapping survive a
 * reload. Page rollover has its own test, {@link com.arcadedb.engine.DictionaryMultiPageTest}.
 */
class DictionaryLimitsTest extends TestHelper {
  /**
   * Names are padded so a few thousand of them, rather than tens of thousands, cross a page boundary.
   */
  private static String name(final int i) {
    return ("p" + i).concat("x".repeat(100));
  }

  /**
   * Filling far past one page used to end in "no space left in dictionary file". It has to just keep working now, with every id
   * still resolvable in both directions.
   * <p>
   * Sized by what it has to prove, not by a round number: every name costs its own nested transaction, so 3,000 padded names
   * cross four pages and keep the test well under a second even on a slow CI runner. Twenty thousand of them proved nothing
   * more and only bought 20,000 WAL commits.
   */
  @Test
  void fillingWellPastOnePageKeepsWorking() {
    final Dictionary dictionary = database.getSchema().getDictionary();

    final int total = 3_000;
    for (int i = 0; i < total; ++i)
      dictionary.getIdByName(name(i), true);

    assertThat((long) total * 100).as("the fixture has to be bigger than a single page").isGreaterThan(dictionary.getPageSize());
    assertThat(dictionary.getTotalPages()).as("several pages, not just a rollover").isGreaterThanOrEqualTo(4);
    assertThat(dictionary.getDictionaryMap()).hasSize(total);

    for (int i = 0; i < total; ++i) {
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
