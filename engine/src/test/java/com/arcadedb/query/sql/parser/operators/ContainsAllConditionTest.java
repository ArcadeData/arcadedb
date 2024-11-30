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
package com.arcadedb.query.sql.parser.operators;

import com.arcadedb.query.sql.parser.ContainsAllCondition;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli-(at)-arcadedata.com)
 */
public class ContainsAllConditionTest {
  @Test
  public void test() {
    final ContainsAllCondition op = new ContainsAllCondition(-1);

    assertThat(op.execute(null, null)).isFalse();
    assertThat(op.execute(null, "foo")).isFalse();

    final List<Object> left = new ArrayList<>();
    assertThat(op.execute(left, "foo")).isFalse();
    assertThat(op.execute(left, null)).isFalse();

    left.add("foo");
    left.add("bar");

    assertThat(op.execute(left, "foo")).isTrue();
    assertThat(op.execute(left, "bar")).isTrue();
    assertThat(op.execute(left, "fooz")).isFalse();

    left.add(null);
    assertThat(op.execute(left, null)).isTrue();

    final List<Object> right = new ArrayList<>();
    left.add("foo");
    left.add("bar");

    assertThat(op.execute(left, right)).isTrue();
  }

  @Test
  public void testIterable() {
    final Iterable<Object> left = new Iterable<>() {
      private final List<Integer> ls = Arrays.asList(3, 1, 2);

      @Override
      public Iterator iterator() {
        return ls.iterator();
      }
    };

    final Iterable<Object> right = new Iterable<>() {
      private final List<Integer> ls = Arrays.asList(2, 3);

      @Override
      public Iterator iterator() {
        return ls.iterator();
      }
    };

    final ContainsAllCondition op = new ContainsAllCondition(-1);
    assertThat(op.execute(left, right)).isFalse();
  }

  @Test
  public void issue1785() {
    final ContainsAllCondition op = new ContainsAllCondition(-1);

    final List<Object> nullList = new ArrayList<>();
    nullList.add(null);

    assertThat(op.execute(nullList, nullList)).isTrue();
  }
}
