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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12685: {@code count()} and {@code exists()} of a native select agree with the rows {@code documents()} returns when the
 * select has a {@code skip}. {@code count()} used to count a record again when it was one of the skipped ones and a second index
 * cursor delivered it (an {@code or} of two indexed leaves), and {@code exists()} ignored the skip.
 */
class SelectCountSkipTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("T");
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");
      database.newDocument("T").set("a", 1, "b", "x").save(); // matches both leaves of the or
      database.newDocument("T").set("a", 1, "b", "y").save();
      database.newDocument("T").set("a", 2, "b", "x").save();
      database.newDocument("T").set("a", 3, "b", "z").save(); // matches neither
    });
  }

  private SelectCompiled orOfTwoIndexedLeaves(final int skip) {
    return database.select().fromType("T").where().property("a").eq().value(1).or().property("b").eq().value("x").skip(skip).compile();
  }

  @Test
  void countOfAnOrOverTwoIndexesAgreesWithTheRowsUnderSkip() {
    for (int skip = 0; skip <= 4; skip++) {
      final SelectCompiled select = orOfTwoIndexedLeaves(skip);
      final int rows = select.documents().toList().size();
      assertThat(rows).as("rows with skip " + skip).isEqualTo(Math.max(0, 3 - skip));
      assertThat(select.count()).as("count with skip " + skip).isEqualTo(rows);
    }
  }

  @Test
  void countOfAnOrOverTwoIndexesAgreesWithTheRowsUnderSkipAndLimit() {
    for (int skip = 0; skip <= 3; skip++)
      for (int limit = 0; limit <= 3; limit++) {
        final SelectCompiled select = database.select().fromType("T").where().property("a").eq().value(1).or().property("b").eq()
            .value("x").skip(skip).limit(limit).compile();
        assertThat(select.count()).as("count with skip " + skip + " limit " + limit)
            .isEqualTo(select.documents().toList().size());
      }
  }

  @Test
  void countOfAnInListOverOneIndexAgreesWithTheRowsUnderSkip() {
    for (int skip = 0; skip <= 3; skip++) {
      final SelectCompiled select = database.select().fromType("T").where().property("a").in().value(List.of(1, 2)).skip(skip)
          .compile();
      assertThat(select.count()).as("count with skip " + skip).isEqualTo(select.documents().toList().size());
    }
  }

  @Test
  void existsHonoursTheSkip() {
    for (int skip = 0; skip <= 4; skip++) {
      final SelectCompiled select = orOfTwoIndexedLeaves(skip);
      assertThat(select.exists()).as("exists with skip " + skip).isEqualTo(skip < 3);
    }
  }

  @Test
  void existsHonoursTheSkipOnAScan() {
    // no index on c: a plain scan answers
    database.getSchema().getType("T").createProperty("c", Type.INTEGER);
    database.transaction(() -> {
      database.command("sql", "update T set c = 7 where a = 1");
    });
    assertThat(database.select().fromType("T").where().property("c").eq().value(7).skip(1).compile().exists()).isTrue();
    assertThat(database.select().fromType("T").where().property("c").eq().value(7).skip(2).compile().exists()).isFalse();
    assertThat(database.select().fromType("T").where().property("c").eq().value(7).skip(1).limit(0).compile().exists()).isFalse();
  }

  @Test
  void existsWithoutSkipStillAnswersOnTheFirstMatch() {
    assertThat(database.select().fromType("T").where().property("a").eq().value(3).compile().exists()).isTrue();
    assertThat(database.select().fromType("T").where().property("a").eq().value(4).compile().exists()).isFalse();
    assertThat(database.select().fromType("T").compile().exists()).isTrue();
  }
}
