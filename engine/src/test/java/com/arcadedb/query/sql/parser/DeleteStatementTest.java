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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.fail;

public class DeleteStatementTest extends TestHelper {

  public DeleteStatementTest() {
    autoStartTx = true;
  }

  @Test
  public void deleteFromSubqueryWithWhereTest() {
    database.command("sql", "create document type Foo");
    database.command("sql", "create document type Bar");
    final MutableDocument doc1 = database.newDocument("Foo").set("k", "key1");
    final MutableDocument doc2 = database.newDocument("Foo").set("k", "key2");
    final MutableDocument doc3 = database.newDocument("Foo").set("k", "key3");

    doc1.save();
    doc2.save();
    doc3.save();

    List<Document> list = new ArrayList<>();
    list.add(doc1);
    list.add(doc2);
    list.add(doc3);
    final MutableDocument bar = database.newDocument("Bar").set("arr", list);
    bar.save();

    database.command("sql", "delete from (select expand(arr) from Bar) where k = 'key2'");

    ResultSet result = database.query("sql", "select from Foo");
    Assertions.assertNotNull(result);
    Assertions.assertEquals(result.countEntries(), 2);
    for (ResultSet it = result; it.hasNext(); ) {
      Document doc = it.next().toElement();
      Assertions.assertNotEquals(doc.getString("k"), "key2");
    }
    database.commit();
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(null, is);
    return osql;
  }

  protected SimpleNode checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

}
