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

import com.arcadedb.query.sql.parser.InOperator;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class InOperatorTest {
  @Test
  public void test() {
    final InOperator op = new InOperator(-1);

    assertThat(op.execute(null, null, null)).isFalse();
    assertThat(op.execute(null, null, "foo")).isFalse();
    assertThat(op.execute(null, "foo", null)).isFalse();
    assertThat(op.execute(null, "foo", "foo")).isFalse();

    final List<Object> list1 = new ArrayList<Object>();
    assertThat(op.execute(null, "foo", list1)).isFalse();
    assertThat(op.execute(null, null, list1)).isFalse();
    assertThat(op.execute(null, list1, list1)).isTrue();

    list1.add("a");
    list1.add(1);

    assertThat(op.execute(null, "foo", list1)).isFalse();
    assertThat(op.execute(null, "a", list1)).isTrue();
    assertThat(op.execute(null, 1, list1)).isTrue();
  }

  @Test
  public void issue1785() {
    final InOperator op = new InOperator(-1);

    final List<Object> nullList = new ArrayList<>();
    nullList.add(null);

    assertThat(op.execute(null, null, nullList)).isTrue();
    assertThat(op.execute(null, nullList, nullList)).isTrue();
  }
}
