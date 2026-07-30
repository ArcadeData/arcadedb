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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PR #5560 let the schema dictionary spill past page 0. Dictionary pages replicate as raw pages through
 * {@code TransactionManager.applyChanges}, and the post-apply reload has to walk every page rather than page 0, or a follower
 * ends up with an in-RAM dictionary missing every name past the first page and fails each record that references one with
 * "Dictionary item with id N is not valid".
 * <p>
 * That path was covered only by a unit test feeding a hand-built {@code WALFile.WALPage} to {@code applyChanges}. This is the
 * real thing: a leader crosses the page boundary, and every follower has to resolve the names both ways and read the records
 * back. It is the scenario behind the upgrade-ordering requirement in {@code docs/5560-dictionary-multipage.md}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5560DictionaryRolloverReplicationIT extends BaseRaftHATest {
  /**
   * Padded so a few hundred names cross a page instead of a few hundred thousand. Long, but a legal identifier: only type and
   * property names ever enter the dictionary, and the point is to reach the boundary quickly rather than to look typical.
   * <p>
   * The sizing targets the default page size: {@link Dictionary#DEF_PAGE_SIZE} is 65,536 bytes per page, a little less once the
   * page and dictionary headers come off the top, while {@link #PROPERTIES} names of this length are about 200,000 bytes. The
   * fixture therefore crosses the boundary around three times over rather than only just. That margin is what keeps the
   * {@code getTotalPages() > 1} guard below honest if the default ever changes; should it grow past the total written here, the
   * guard fails loudly rather than passing without a rollover.
   */
  private static final int NAME_LENGTH            = 500;
  private static final int TYPE_RECORDS           = 5;
  private static final int PROPERTIES_PER_RECORD  = 80;
  /**
   * Derived, never chosen: the index arithmetic below walks {@code record * PROPERTIES_PER_RECORD + p}, so a count that is not a
   * multiple of the per-record figure would leave gaps that the follower loop would then report as missing names.
   */
  private static final int PROPERTIES             = TYPE_RECORDS * PROPERTIES_PER_RECORD;

  private static String propertyName(final int i) {
    final String prefix = "p" + i + "_";
    return prefix + "x".repeat(NAME_LENGTH - prefix.length());
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Deliberately empty. The base fixture builds several types with properties, indexes and records, and every one of those names
   * lands in the dictionary before the test starts. This test opens by asserting the dictionary is still a single page, so that
   * the rollover it goes on to measure is unambiguously produced by its own writes.
   */
  @Override
  protected void populateDatabase() {
  }

  @Test
  @Tag("slow")
  void aDictionaryThatRollsOverOnTheLeaderIsUsableOnEveryFollower() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final Dictionary leaderDictionary = leader.getSchema().getDictionary();
    assertThat(leaderDictionary.getTotalPages()).as("the fixture has to start from a single page").isEqualTo(1);

    leader.transaction(() -> leader.getSchema().createDocumentType("Wide"));

    // ENOUGH DISTINCT PROPERTY NAMES TO PUSH THE DICTIONARY OFF PAGE 0. EACH ONE ENTERS THE DICTIONARY THE FIRST TIME IT IS
    // SERIALISED, SO THE ROLLOVER HAPPENS AS A SIDE EFFECT OF ORDINARY WRITES, WHICH IS HOW IT HAPPENS IN PRODUCTION
    for (int r = 0; r < TYPE_RECORDS; ++r) {
      final int record = r;
      leader.transaction(() -> {
        final var doc = leader.newDocument("Wide");
        doc.set("recordId", record);
        for (int p = 0; p < PROPERTIES_PER_RECORD; ++p) {
          final int index = record * PROPERTIES_PER_RECORD + p;
          doc.set(propertyName(index), "value_" + index);
        }
        doc.save();
      });
    }

    assertThat(leaderDictionary.getTotalPages()).as("the leader's dictionary has to have crossed a page boundary")
        .isGreaterThan(1);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database server = getServerDatabase(serverIndex, getDatabaseName());
      final Dictionary dictionary = server.getSchema().getDictionary();

      assertThat(dictionary.getTotalPages()).as("the dictionary must span several pages on server %d", serverIndex)
          .isGreaterThan(1);

      // BOTH DIRECTIONS, FOR NAMES ON PAGE 0 AND FOR NAMES PAST IT. A FOLLOWER THAT STOPPED AT PAGE 0 HOLDS NEITHER MAPPING FOR A
      // NAME PAST THE BOUNDARY, SO BOTH BREAK TOGETHER AND THE MISSING-ID ASSERTION BELOW IS SIMPLY THE ONE REACHED FIRST - IT IS
      // WHERE THE PINNED-PAGE RUN TRIPS. RESOLVING THE ID IS WHAT A WRITE NEEDS; RESOLVING THE NAME IS WHAT EVERY READ OF THESE
      // RECORDS HITS, WHICH IS WHY THE RECORD READ-BACK FURTHER DOWN IS THE USER-VISIBLE END OF THE SAME FAILURE
      for (int p = 0; p < PROPERTIES; ++p) {
        final String name = propertyName(p);
        final int id = dictionary.getIdByName(name, false);
        assertThat(id).as("property #%d must be in the dictionary of server %d", p, serverIndex).isNotEqualTo(-1);
        assertThat(dictionary.getNameById(id)).as("id %d must resolve on server %d", id, serverIndex).isEqualTo(name);
      }

      // AND THE RECORDS THEMSELVES, WHICH IS THE USER-VISIBLE SYMPTOM: THE PROPERTY NAMES ARE STORED AS DICTIONARY IDS INSIDE
      // EACH ONE, SO DESERIALISING THEM IS WHAT TURNS A MISSING PAGE INTO "Dictionary item with id N is not valid"
      assertThat(server.countType("Wide", false)).as("every record must replicate to server %d", serverIndex)
          .isEqualTo(TYPE_RECORDS);

      try (final ResultSet rs = server.query("sql", "SELECT FROM Wide ORDER BY recordId")) {
        int seen = 0;
        while (rs.hasNext()) {
          final var row = rs.next();
          final int record = row.getProperty("recordId");
          for (int p = 0; p < PROPERTIES_PER_RECORD; ++p) {
            final int index = record * PROPERTIES_PER_RECORD + p;
            assertThat(row.<String>getProperty(propertyName(index)))
                .as("record %d property %d must read back on server %d", record, index, serverIndex)
                .isEqualTo("value_" + index);
          }
          ++seen;
        }
        assertThat(seen).as("every record must be readable on server %d", serverIndex).isEqualTo(TYPE_RECORDS);
      }
    });
  }
}
