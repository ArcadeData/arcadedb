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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests of the transient scan hash behind issue #8695.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ScanPropertyHashIndexTest extends TestHelper {

  private ScanPropertyHashIndex build(final String property) {
    @SuppressWarnings("unchecked") final Iterator<Identifiable> scan = (Iterator<Identifiable>) (Object) database.iterateType("T", true);
    return new ScanPropertyHashIndex(scan, property, WorkGuard.forCommandDeadline(null));
  }

  private int count(final Iterator<Identifiable> it) {
    int n = 0;
    while (it.hasNext()) {
      it.next();
      n++;
    }
    return n;
  }

  @Test
  void textAndIntegralKeysAreExactAndOtherTypesAreKeptAside() {
    database.getSchema().createVertexType("T");
    database.transaction(() -> {
      database.newVertex("T").set("k", "a").save();
      database.newVertex("T").set("k", "a").save();
      database.newVertex("T").set("k", 7).save();
      database.newVertex("T").set("k", 7L).save();
      database.newVertex("T").set("k", 7.0d).save();
      database.newVertex("T").set("x", 1).save();
    });
    final ScanPropertyHashIndex index = build("k");
    assertThat(count(index.candidates("a"))).isEqualTo(2);
    assertThat(count(index.candidates("zz"))).isZero();
    // two integral matches plus the Double 7.0, which can equal 7 by value and is always offered
    assertThat(count(index.candidates(7L))).isEqualTo(3);
    assertThat(ScanPropertyHashIndex.isSupported(7.5d)).isFalse();
    assertThat(ScanPropertyHashIndex.isSupported(null)).isFalse();
  }

  @Test
  void numericLookupDeclinesWhenTooManyNonIntegralValuesAreStored() {
    database.getSchema().createVertexType("T");
    database.transaction(() -> {
      for (int i = 0; i <= ScanPropertyHashIndex.MAX_NON_INTEGRAL; i++)
        database.newVertex("T").set("k", i + 0.5d).save();
      database.newVertex("T").set("k", "a").save();
    });
    final ScanPropertyHashIndex index = build("k");
    assertThat(index.candidates(3L)).isNull();
    assertThat(count(index.candidates("a"))).isEqualTo(1);
  }

  @Test
  void aKeyWithFewDistinctValuesIsNotSelective() {
    database.getSchema().createVertexType("T");
    database.transaction(() -> {
      for (int i = 0; i < ScanPropertyHashIndex.MIN_RECORDS_TO_JUDGE + 10; i++)
        database.newVertex("T").set("kind", "K" + (i % 3)).set("id", "i" + i).save();
    });
    assertThat(build("kind").isSelective()).isFalse();
    assertThat(build("id").isSelective()).isTrue();
  }
}
