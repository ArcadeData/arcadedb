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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9992: the REGEXP gate refused a pattern on a LINK while the write path matched one anyway, against the
 * {@code toString()} of whatever object the link arrived as, so the same link passed as a RID ({@code #1:0}) and failed as
 * the record it points to ({@code #1:0[n=0]}). A LINK is now matched as the RID it points to, by the write path, the check of
 * stored records and the declaration alike, so they cannot disagree. A type whose values have no text at all (a LIST) refuses
 * the write of a pattern a schema written before the rule still carries, saying what the declaration would have said,
 * instead of matching it against a bracketed dump.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9992LinkRegexpTextFormTest extends TestHelper {

  @Test
  void aLinkIsMatchedAsItsRidWhateverObjectItArrivedAs() {
    database.command("sql", "CREATE VERTEX TYPE V9992");
    database.command("sql", "CREATE DOCUMENT TYPE T9992");

    final Vertex[] targets = new Vertex[2];
    database.transaction(() -> {
      targets[0] = database.newVertex("V9992").set("n", 0).save();
      targets[1] = database.newVertex("V9992").set("n", 1).save();
    });
    final RID first = targets[0].getIdentity();
    // the pattern names the first link exactly: '#3:0' matches it, its own record writes itself as '#3:0[n=0]'
    database.command("sql", "CREATE PROPERTY T9992.l LINK (regexp '" + first + "')");

    // the same link, as a RID and as the record: both pass
    database.transaction(() -> database.newDocument("T9992").set("l", first).save());
    database.transaction(() -> database.newDocument("T9992").set("l", targets[0]).save());
    database.transaction(() -> database.newDocument("T9992").set("l", first.asVertex()).save());
    // and as the text a JSON or HTTP client sends
    database.transaction(() -> database.newDocument("T9992").set("l", first.toString()).save());

    // the other link, both ways: both fail
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T9992").set("l", targets[1].getIdentity()).save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T9992").set("l", targets[1]).save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");

    // SQL writes the link as the record a subquery selects
    database.transaction(() -> database.command("sql", "INSERT INTO T9992 SET l = (SELECT FROM V9992 WHERE n = 0)").close());
    assertThat(database.countType("T9992", false)).isEqualTo(5);
  }

  @Test
  void aPatternOnALinkIsJudgedAgainstTheShapeOfARid() {
    database.command("sql", "CREATE DOCUMENT TYPE L9992");
    // what the issue's legacy schema matched against: the text of a record, which a link is never written as any more
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY L9992.l LINK (regexp '#[0-9]+:[0-9]+\\\\[.*\\\\]')"))
        .hasMessageContaining("matches no text a value of LINK");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY L9992.l LINK (regexp '[A-Z]+')"))
        .hasMessageContaining("matches no text a value of LINK");
    database.command("sql", "CREATE PROPERTY L9992.l LINK (regexp '#12:.*')");
    assertThat(database.getSchema().getType("L9992").getProperty("l").getRegexp()).isEqualTo("#12:.*");
  }

  @Test
  void aPatternFromASchemaWrittenBeforeTheRuleIsMatchedTheSameWayOnALink() throws IOException {
    database.command("sql", "CREATE VERTEX TYPE W9992");
    database.command("sql", "CREATE DOCUMENT TYPE S9992");
    database.command("sql", "CREATE PROPERTY S9992.l LINK");
    database.command("sql", "CREATE PROPERTY S9992.tags LIST");
    database.command("sql", "CREATE PROPERTY S9992.s STRING");
    database.close();

    // patterns no declaration accepts today: the text of a selected record on the LINK, and one on a type with no text
    final File schemaFile = new File(getDatabasePath(), "schema.json");
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject properties = root.getJSONObject("types").getJSONObject("S9992").getJSONObject("properties");
    properties.getJSONObject("l").put("regexp", "#[0-9]+:[0-9]+\\[.*\\]");
    properties.getJSONObject("tags").put("regexp", "\\[.*\\]");
    // and one that does not even compile
    properties.getJSONObject("s").put("regexp", "[a-");
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

    database = factory.open();
    final Vertex[] saved = new Vertex[1];
    database.transaction(() -> saved[0] = database.newVertex("W9992").set("n", 0).save());
    final Vertex target = saved[0];

    // the same link fails the same way, as a RID and as the record whose toString() the pattern was written for
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("S9992").set("l", target.getIdentity()).save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("S9992").set("l", target).save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");

    // a LIST has no text: the write says so and names the remedy, rather than matching the pattern against '[a, b]'
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument document = database.newDocument("S9992");
      document.set("tags", List.of("a", "b"));
      document.save();
    })).isInstanceOf(ValidationException.class).hasMessageContaining("is not applicable to type LIST")
        .hasMessageContaining("REGEXP null");

    // a pattern that does not compile says so, with the remedy, rather than escaping as a PatternSyntaxException
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("S9992").set("s", "abc").save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("is not a valid regular expression")
        .hasMessageContaining("REGEXP null");

    // the remedy works, and so does a write without the property
    database.transaction(() -> database.newDocument("S9992").save());
    database.command("sql", "ALTER PROPERTY S9992.tags REGEXP null");
    database.command("sql", "ALTER PROPERTY S9992.l REGEXP null");
    database.transaction(() -> database.newDocument("S9992").set("l", target).set("tags", List.of("a")).save());
  }
}
