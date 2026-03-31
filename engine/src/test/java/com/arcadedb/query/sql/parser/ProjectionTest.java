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
package com.arcadedb.query.sql.parser;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Created by luigidellaquila on 02/07/15.
 */
class ProjectionTest {

  @Test
  void isExpand() throws Exception {
    final SelectStatement stm = (SelectStatement) new SQLAntlrParser(null).parse("select expand(foo)  from V");
    assertThat(stm.getProjection().isExpand()).isTrue();

    final SelectStatement stm2 = (SelectStatement) new SQLAntlrParser(null).parse("select foo  from V");
    assertThat(stm2.getProjection().isExpand()).isFalse();

    final SelectStatement stm3 = (SelectStatement) new SQLAntlrParser(null).parse("select expand  from V");
    assertThat(stm3.getProjection().isExpand()).isFalse();
  }

  @Test
  void validate() throws Exception {
    final SelectStatement stm = (SelectStatement) new SQLAntlrParser(null).parse("select expand(foo)  from V");
    stm.getProjection().validate();

    try {
      final SelectStatement stmInvalid = (SelectStatement) new SQLAntlrParser(null).parse("select expand(foo), bar  from V");
      stmInvalid.getProjection().validate(); // this should throw
      fail("Expected validate() to throw");
    } catch (final CommandSQLParsingException ex) {
      // expected
    }
  }
}
