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

import com.arcadedb.query.sql.parser.ContainsValueOperator;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/** @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com) */
public class ContainsValueOperatorTest {
  @Test
  public void test() {
    final ContainsValueOperator op = new ContainsValueOperator(-1);

    assertThat(op.execute(null, null, null)).isFalse();
    assertThat(op.execute(null, null, "foo")).isFalse();

    final Map<Object, Object> originMap = new HashMap<Object, Object>();
    assertThat(op.execute(null, originMap, "bar")).isFalse();
    assertThat(op.execute(null, originMap, null)).isFalse();

    originMap.put("foo", "bar");
    originMap.put(1, "baz");
    originMap.put(2, 12);

    assertThat(op.execute(null, originMap, "bar")).isTrue();
    assertThat(op.execute(null, originMap, "baz")).isTrue();
    assertThat(op.execute(null, originMap, 12)).isTrue();
    assertThat(op.execute(null, originMap, "asdfafsd")).isFalse();
  }
}
