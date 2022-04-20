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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Created by luigidellaquila on 02/07/15.
 */
public class ProjectionTest {

  @Test
  public void testIsExpand() throws ParseException {
    SqlParser parser = getParserFor("select expand(foo)  from V");
    SelectStatement stm = (SelectStatement) parser.parse();
    Assertions.assertTrue(stm.getProjection().isExpand());

    SqlParser parser2 = getParserFor("select foo  from V");
    SelectStatement stm2 = (SelectStatement) parser2.parse();
    Assertions.assertFalse(stm2.getProjection().isExpand());

    SqlParser parser3 = getParserFor("select expand  from V");
    SelectStatement stm3 = (SelectStatement) parser3.parse();
    Assertions.assertFalse(stm3.getProjection().isExpand());
  }

  @Test
  public void testValidate() throws ParseException {
    SqlParser parser = getParserFor("select expand(foo)  from V");
    SelectStatement stm = (SelectStatement) parser.parse();
    stm.getProjection().validate();

    try {
      getParserFor("select expand(foo), bar  from V").parse();

      Assertions.fail();
    } catch (CommandSQLParsingException ex) {

    } catch (Exception x) {
      Assertions.fail();
    }
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(null, is);
    return osql;
  }
}
