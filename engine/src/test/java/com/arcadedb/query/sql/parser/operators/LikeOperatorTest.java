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

import com.arcadedb.query.sql.executor.QueryHelper;
import com.arcadedb.query.sql.parser.LikeOperator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class LikeOperatorTest {
  @Test
  public void test() {
    final LikeOperator op = new LikeOperator(-1);
    assertThat(op.execute(null, "foobar", "%ooba%")).isTrue();
    assertThat(op.execute(null, "foobar", "%oo%")).isTrue();
    assertThat(op.execute(null, "foobar", "oo%")).isFalse();
    assertThat(op.execute(null, "foobar", "%oo")).isFalse();
    assertThat(op.execute(null, "foobar", "%fff%")).isFalse();
    assertThat(op.execute(null, "foobar", "foobar")).isTrue();
    assertThat(op.execute(null, "100%", "100\\%")).isTrue();
    assertThat(op.execute(null, "100%", "100%")).isTrue();
    assertThat(op.execute(null, "", "")).isTrue();
    assertThat(op.execute(null, "100?", "100\\?")).isTrue();
    assertThat(op.execute(null, "100?", "100?")).isTrue();
    assertThat(op.execute(null, "abc\ndef", "%e%")).isTrue();
  }

  @Test
  public void replaceSpecialCharacters() {
    assertThat(QueryHelper.convertForRegExp("\\[]{}()|*+$^.?%")).isEqualTo("(?s)\\\\\\[\\]\\{\\}\\(\\)\\|\\*\\+\\$\\^\\...*");
  }
}
