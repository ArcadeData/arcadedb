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
import com.arcadedb.database.Document;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12692: a parallel native select with an {@code orderBy} and a {@code limit} returned an arbitrary {@code limit}-sized
 * subset of the matches, sorted, because the producers stopped once {@code limit} records were enqueued while the consumer sorts
 * the whole result before it applies the limit.
 */
class SelectParallelOrderByLimitTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T", 3).createProperty("v", Type.INTEGER);
      for (int i = 0; i < 300; i++)
        database.newDocument("T").set("v", i).save();
    });
  }

  private List<Integer> values(final SelectCompiled select) {
    final List<Integer> result = new ArrayList<>();
    final SelectIterator<Document> iterator = select.documents();
    while (iterator.hasNext())
      result.add(iterator.next().getInteger("v"));
    return result;
  }

  @Test
  void descendingOrderWithALimitIsTheTopOfTheWholeResult() {
    for (int run = 0; run < 5; run++)
      assertThat(values(database.select().fromType("T").orderBy("v", false).limit(5).compile().parallel()))
          .containsExactly(299, 298, 297, 296, 295);
  }

  @Test
  void ascendingOrderWithAWhereAndALimitIsTheBottomOfTheMatches() {
    for (int run = 0; run < 5; run++)
      assertThat(values(database.select().fromType("T").where().property("v").ge().value(100).compile().parallel())).hasSize(200);

    for (int run = 0; run < 5; run++)
      assertThat(values(
          database.select().fromType("T").where().property("v").ge().value(100).orderBy("v", true).limit(3).compile().parallel()))
          .containsExactly(100, 101, 102);
  }

  @Test
  void orderWithSkipAndLimitAgreesWithTheSequentialRun() {
    final List<Integer> sequential = values(database.select().fromType("T").orderBy("v", false).skip(2).limit(4).compile());
    assertThat(sequential).containsExactly(297, 296, 295, 294);
    for (int run = 0; run < 5; run++)
      assertThat(values(database.select().fromType("T").orderBy("v", false).skip(2).limit(4).compile().parallel()))
          .isEqualTo(sequential);
  }

  @Test
  void aLimitWithoutAnOrderStillStopsTheProducers() {
    for (int run = 0; run < 5; run++)
      assertThat(values(database.select().fromType("T").limit(7).compile().parallel())).hasSize(7).doesNotHaveDuplicates();
  }
}
