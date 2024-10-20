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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Created by luigidellaquila on 02/07/15.
 */
public class ProjectionTest {

  @Test
  public void testIsExpand() throws ParseException {
    final SqlParser parser = getParserFor("select expand(foo)  from V");
    final SelectStatement stm = (SelectStatement) parser.Parse();
    assertThat(stm.getProjection().isExpand()).isTrue();

    final SqlParser parser2 = getParserFor("select foo  from V");
    final SelectStatement stm2 = (SelectStatement) parser2.Parse();
    assertThat(stm2.getProjection().isExpand()).isFalse();

    final SqlParser parser3 = getParserFor("select expand  from V");
    final SelectStatement stm3 = (SelectStatement) parser3.Parse();
    assertThat(stm3.getProjection().isExpand()).isFalse();
  }

  @Test
  public void testValidate() throws ParseException {
    final SqlParser parser = getParserFor("select expand(foo)  from V");
    final SelectStatement stm = (SelectStatement) parser.Parse();
    stm.getProjection().validate();

    try {
      getParserFor("select expand(foo), bar  from V").Parse();

      fail("");
    } catch (final CommandSQLParsingException ex) {

    } catch (final Exception x) {
      fail("");
    }
  }

  protected SqlParser getParserFor(final String string) {
    final InputStream is = new ByteArrayInputStream(string.getBytes());
    final SqlParser osql = new SqlParser(null, is);
    return osql;
  }
}
