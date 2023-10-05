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

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.fail;

public class InsertStatementTest {

  protected SimpleNode checkRightSyntax(final String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(final String query, final boolean isCorrect) {
    final SqlParser osql = getParserFor(query);
    try {
      final SimpleNode result = osql.Parse();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (final Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testSimpleInsert() {
    checkRightSyntax("insert into Foo (a) values (1)");
    checkRightSyntax("insert into Foo (a) values ('1')");
    checkRightSyntax("insert into Foo (a) values (\"1\")");

    checkRightSyntax("insert into Foo (a,b) values (1, 2)");
    checkRightSyntax("insert into Foo (a,b) values ('1', '2')");
    checkRightSyntax("insert into Foo (a,b) values (\"1\", \"2\")");
  }

  @Test
  public void testInsertIntoBucket() {
    checkRightSyntax(
        "insert into bucket:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
    checkRightSyntax(
        "insert into BUCKET:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");

    checkRightSyntax(
        "insert into Foo bucket foo1 (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
    checkRightSyntax(
        "insert into Foo BUCKET foo1 (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
  }

  @Test
  public void testInsertSelectTimeout() {
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 ");
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 return");
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 exception");
  }

  @Test
  public void testInsertInsert() {
    checkRightSyntax("insert into foo set bar = (insert into foo set a = 'foo') ");
  }

  @Test
  public void testInsertEmbeddedDocs() {
    checkRightSyntax(
        "INSERT INTO Activity SET user = #14:1, story = #18:2, `like` = { \n" + "      count: 0, \n" + "      latest: [], \n"
            + "      '@type': 'document', \n" + "      '@type': 'Like'\n" + "    }");

    checkRightSyntax(
        "INSERT INTO Activity SET user = #14:1, story = #18:2, `like` = { \n" + "      count: 0, \n" + "      latest: [], \n"
            + "      '@type': 'document', \n" + "      '@type': 'Like'\n" + "    }");
  }

  @Test
  public void testJsonEscaping() {
    // issue #5911
    checkRightSyntax("insert into Bookmark content {\"data\""
        + ":\"DPl62xXzEqG3tIPv7jYYWK34IG4bwTUNk0UUnhYHOluUdPiMQOLSz3V\\/GraBuzbEbjDARS6X1wUh53Dh3\\/hFpSXVy74iw4K7\\/WvwtyvdDJ51\\/6qg8RgPyL8qByNXnqxLviMaZk+UZCNmJ+wPJ+\\/Jphtb\\/cNPw5HmbTIA2VxOq"
        + "1OybZIuJaTRVD5tO8sVpMqJTa4IFjMb69vlIYpWctEYByp7gtBCRQOsBeLydnoW+DUOeG1jDyrMmA4hi5M+ctwdn9Vb5wqTjWw=\",\"isRead\":\"N\",\"id\":\"52013784-4e32-4e9b-9676-1814ca1256fb\",\"isPrivate\":\"F\",\"is"
        + "Shared\":0}");
  }

  @Test
  public void testJsonArrayEscaping() {
    // issue #5911
    checkRightSyntax("insert into Bookmark content [{\"data\""
        + ":\"DPl62xXzEqG3tIPv7jYYWK34IG4bwTUNk0UUnhYHOluUdPiMQOLSz3V\\/GraBuzbEbjDARS6X1wUh53Dh3\\/hFpSXVy74iw4K7\\/WvwtyvdDJ51\\/6qg8RgPyL8qByNXnqxLviMaZk+UZCNmJ+wPJ+\\/Jphtb\\/cNPw5HmbTIA2VxOq"
        + "1OybZIuJaTRVD5tO8sVpMqJTa4IFjMb69vlIYpWctEYByp7gtBCRQOsBeLydnoW+DUOeG1jDyrMmA4hi5M+ctwdn9Vb5wqTjWw=\",\"isRead\":\"N\",\"id\":\"52013784-4e32-4e9b-9676-1814ca1256fb\",\"isPrivate\":\"F\",\"is"
        + "Shared\":0}, {}]");
  }

  @Test
  public void testSlashInQuery() {
    checkRightSyntax("insert into test content {\"node_id\": \"MFmqvmht//sYYWB8=\"}");
    checkRightSyntax("insert into test content { \"node_id\": \"MFmqvmht\\/\\/GYsYYWB8=\"}");
  }

  @Test
  public void testInsertSelectNoTarget() {
    checkRightSyntax("insert into Bookmark from select #12:0");
    checkRightSyntax("insert into Bookmark from select expand($foo)");
    checkRightSyntax("insert into Bookmark from (select #12:0)");
    checkRightSyntax("insert into Bookmark from (select expand($foo))");
  }

  @Test
  public void testInsertJsonNewLines() {
    checkRightSyntax("INSERT INTO doc CONTENT {\n" + //
        "\"head\" : {\n" + //
        "  \"vars\" : [ \"item\", \"itemLabel\" ]\n" + //
        "},\n" + //
        "\"results\" : {\n" + //
        "  \"bindings\" : [ {\n" + //
        "    \"item\" : {\n" + //
        "          \"type\" : \"uri\",\n" + //
        "              \"value\" : \"http://www.wikidata.org/entity/Q113997665\"\n" + //
        "        },\n" + //
        "        \"itemLabel\" : {\n" + //
        "          \"xml:lang\" : \"en\",\n" + //
        "              \"type\" : \"literal\",\n" + //
        "              \"value\" : \"ArcadeDB\"\n" + //
        "        }\n" + //
        "      }, {\n" + //
        "        \"item\" : {\n" + //
        "          \"type\" : \"uri\",\n" + //
        "              \"value\" : \"http://www.wikidata.org/entity/Q808716\"\n" + //
        "        },\n" + //
        "        \"itemLabel\" : {\n" + //
        "          \"xml:lang\" : \"en\",\n" + //
        "              \"type\" : \"literal\",\n" + //
        "              \"value\" : \"OrientDB\"\n" + //
        "        }\n" + //
        "      } ]\n" + //
        "    }\n" + //
        "}");
  }

  protected SqlParser getParserFor(final String string) {
    final InputStream is = new ByteArrayInputStream(string.getBytes());
    return new SqlParser(null, is);
  }
}
