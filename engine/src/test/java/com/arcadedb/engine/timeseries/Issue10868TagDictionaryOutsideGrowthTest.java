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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10868: one intern on a dictionary whose pages grew from outside the instance - an HA follower receiving them through the
 * Raft WAL, then promoted - disabled its self-heal for good. {@code publish} set {@code entryCount} to the on-disk count while the
 * map held nothing between what it had loaded and the first id it appended, and {@code resolveMiss} refused to reload an id at or
 * below that count, so every id in the gap read back as null; a value only the missed pages held was interned a second time.
 * <p>
 * The writer is the dictionary registered with the schema; the "promoted" instance is a second view over the same file, the
 * stand-in the existing self-heal tests use for a node whose pages grew without its map.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10868TagDictionaryOutsideGrowthTest extends TestHelper {

  @Test
  void anInternOnAStaleInstanceKeepsEveryIdResolvableAndInternsNothingTwice() throws Exception {
    final TimeSeriesTagDictionary writer = createDictionary("tags_promoted");
    writer.internAll(values("early", 3));

    final TimeSeriesTagDictionary promoted = new TimeSeriesTagDictionary((DatabaseInternal) database, "tags_promoted",
        writer.getComponentFile());
    assertThat(promoted.getById(3)).isEqualTo("early2");
    assertThat(promoted.size()).isEqualTo(3);

    // pages grow from outside the promoted instance: ids 4..43
    writer.internAll(values("wal", 40));
    assertThat(writer.size()).isEqualTo(43);

    // the promoted node interns a value of its own before reading any of those ids
    final int own = promoted.intern("promoted-wrote-this");
    assertThat(own).as("the next id after everything the file holds").isEqualTo(44);

    // every id the pages hold is resolvable, the ones the intern jumped over included
    for (int id = 1; id <= 43; id++)
      assertThat(promoted.getById(id)).as("id %d", id).isEqualTo(writer.getById(id));
    assertThat(promoted.getById(44)).isEqualTo("promoted-wrote-this");

    // a value the missed pages hold keeps its id: interned again, it is not appended a second time
    final int wal16 = writer.getId("wal16");
    assertThat(promoted.getId("wal16")).isEqualTo(wal16);
    assertThat(promoted.intern("wal16")).isEqualTo(wal16);
    assertThat(promoted.size()).isEqualTo(44);
  }

  @Test
  void aStaleInstanceInterningAValueTheFileAlreadyHoldsGetsItsIdBack() throws Exception {
    final TimeSeriesTagDictionary writer = createDictionary("tags_duplicate");
    writer.internAll(values("a", 2));

    final TimeSeriesTagDictionary promoted = new TimeSeriesTagDictionary((DatabaseInternal) database, "tags_duplicate",
        writer.getComponentFile());
    assertThat(promoted.getById(2)).isEqualTo("a1");

    writer.internAll(values("b", 5));

    // the first thing the stale instance does is intern a batch that mixes values the file holds with a new one
    promoted.internAll(List.of("b3", "fresh", "b0"));
    assertThat(promoted.getId("b3")).isEqualTo(writer.getId("b3"));
    assertThat(promoted.getId("b0")).isEqualTo(writer.getId("b0"));
    assertThat(promoted.getId("fresh")).isEqualTo(8);
    assertThat(promoted.size()).as("one new entry, no duplicate").isEqualTo(8);

    // and the file agrees: a fresh view rebuilt from the pages holds each value once
    final TimeSeriesTagDictionary fresh = new TimeSeriesTagDictionary((DatabaseInternal) database, "tags_duplicate",
        writer.getComponentFile());
    fresh.load();
    assertThat(fresh.size()).isEqualTo(8);
    assertThat(fresh.checkIntegrity()).isEmpty();
  }

  private TimeSeriesTagDictionary createDictionary(final String name) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final TimeSeriesTagDictionary dict = new TimeSeriesTagDictionary(db, name, db.getDatabasePath() + "/" + name);
    ((LocalSchema) db.getSchema()).registerFile(dict);
    dict.initHeaderPage();
    return dict;
  }

  private static List<String> values(final String prefix, final int count) {
    final List<String> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      values.add(prefix + i);
    return values;
  }
}
