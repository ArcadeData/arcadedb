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
package com.arcadedb.server.ai;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The schema summary the assistant sends with every question, and the detail of one type. No server: a database of its own. */
class AiSchemaDigestTest {

  @Test
  void formatsATypeOnOneLineWithItsMarkers() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigestFormat", database -> {
      final Schema schema = database.getSchema();
      final VertexType party = schema.buildVertexType().withName("Party").withTotalBuckets(2).create();
      party.createProperty("id", Type.STRING).setMandatory(true);
      final VertexType customer = schema.buildVertexType().withName("Customer").withTotalBuckets(8).create();
      customer.addSuperType(party);
      customer.setCustomValue("description", "A \"buyer\"\nwith‮ tricks");
      customer.createProperty("name", Type.STRING).setMandatory(true).setNotNull(true);
      customer.createProperty("email", Type.STRING);
      customer.createProperty("created", Type.DATETIME).setReadonly(true);
      customer.createProperty("tags", Type.LIST, "STRING");
      customer.createProperty("city", Type.STRING);
      customer.createProperty("body", Type.STRING);
      customer.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      customer.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "email");
      customer.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "city");
      customer.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "body");
      schema.buildEdgeType().withName("Bought").create().createProperty("amount", Type.DECIMAL);
      schema.createDocumentType("Note");
      committed(database, () -> {
        for (int i = 0; i < 1500; i++)
          database.newVertex("Customer").set("id", "c" + i).set("name", "n" + i).set("email", "e" + i).save();
      });

      final String digest = AiSchemaDigest.build(database, "shop");

      assertThat(digest).startsWith("Database shop: 4 types, 4 shown in full\n");
      assertThat(digest).contains("get_type(name)");
      final String customerLine = lineOf(digest, "vertex Customer");
      assertThat(customerLine).contains("~1.5K rows, 8 buckets, extends Party");
      // Quotes become apostrophes, the newline a space, the bidirectional override disappears
      assertThat(customerLine).contains("\"A 'buyer' with tricks\"");
      assertThat(customerLine).doesNotContain("‮");
      assertThat(customerLine).contains("name STRING! nn").contains("created DATETIME ro").contains("tags LIST<STRING>");
      assertThat(customerLine).contains(" | idx: ").contains("name UNIQUE").contains("name+city").contains("email")
          .contains("body FULL_TEXT");
      // The properties of the indexed columns come before the plain ones
      assertThat(customerLine.indexOf("name STRING")).isLessThan(customerLine.indexOf("created DATETIME"));
      assertThat(lineOf(digest, "edge Bought")).contains("amount DECIMAL").doesNotContain("idx:");
      assertThat(lineOf(digest, "doc Note")).contains("~0 rows").doesNotContain(":");
      // Largest first
      assertThat(digest.indexOf("vertex Customer")).isLessThan(digest.indexOf("doc Note"));
      assertThat(digest.lines().allMatch(l -> !l.contains("\r"))).isTrue();
    });
  }

  @Test
  void limitsThePropertiesOfAType() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigestProperties", database -> {
      final VertexType wide = database.getSchema().buildVertexType().withName("Wide").create();
      for (int i = 0; i < 30; i++)
        wide.createProperty(String.format("p%02d", i), Type.STRING);

      final String line = lineOf(AiSchemaDigest.build(database, "db"), "vertex Wide");

      assertThat(line).contains("p00 STRING").contains("p11 STRING").doesNotContain("p12 STRING").contains("+18 more");
    });
  }

  @Test
  @Tag("slow")
  void aSchemaOfFiveHundredTypesStaysSmallAndFast() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigest500", database -> {
      final Schema schema = database.getSchema();
      for (int t = 0; t < 500; t++) {
        final VertexType type = schema.buildVertexType().withName("Type" + String.format("%03d", t)).withTotalBuckets(8).create();
        for (int p = 0; p < 20; p++)
          type.createProperty("property" + p, Type.STRING).setMandatory(p == 0);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, t % 2 == 0, "property0");
        type.setCustomValue("description", "Description of type number " + t + " with some words to make the line longer");
      }
      committed(database, () -> {
        for (int t = 0; t < 500; t += 7)
          for (int i = 0; i < 1 + t % 50; i++)
            database.newVertex("Type" + String.format("%03d", t)).set("property0", "x" + i).save();
      });

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final String digest = AiSchemaDigest.build(database, "huge");
      stopwatch.assertStayedUnder(3_000, "500 types of 8 buckets read from counters, not scanned");

      assertThat(digest.length()).isLessThanOrEqualTo(AiSchemaDigest.MAX_CHARS);
      assertThat(digest).startsWith("Database huge: 500 types, ");
      assertThat(digest).contains("\nOthers (");
      final int shown = Integer.parseInt(digest.substring(digest.indexOf("types, ") + 7, digest.indexOf(" shown")));
      assertThat(shown).isBetween(20, 499);
      // The only "~?" is the legend's own: no type has an unknown count
      assertThat(digest.split("~\\?", -1).length - 1).isEqualTo(1);
      // The types with the most records are the ones shown in full
      final String top = digest.lines().filter(l -> l.startsWith("vertex ")).findFirst().orElseThrow();
      assertThat(top).startsWith("vertex Type049 ~50 rows");
      // Deterministic: the same schema gives the same text
      assertThat(AiSchemaDigest.build(database, "huge")).isEqualTo(digest);
    });
  }

  @Test
  void aLongListOfSmallTypesCannotPushTheLimit() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigestOthers", database -> {
      final Schema schema = database.getSchema();
      final String longName = "T" + "x".repeat(200);
      for (int t = 0; t < 2000; t++)
        schema.createDocumentType(longName + t);

      final String digest = AiSchemaDigest.build(database, "many");

      assertThat(digest.length()).isLessThanOrEqualTo(AiSchemaDigest.MAX_CHARS);
      assertThat(digest).contains("Others (").contains(" more");
      // Names are cut to MAX_NAME characters
      assertThat(digest).doesNotContain("x".repeat(AiSchemaDigest.MAX_NAME + 5));
    });
  }

  @Test
  void countsAreFormattedShortly() {
    assertThat(AiSchemaDigest.count(0)).isEqualTo("0");
    assertThat(AiSchemaDigest.count(950)).isEqualTo("950");
    assertThat(AiSchemaDigest.count(1_000)).isEqualTo("1K");
    assertThat(AiSchemaDigest.count(12_345)).isEqualTo("12.3K");
    assertThat(AiSchemaDigest.count(999_999)).isEqualTo("1000K");
    assertThat(AiSchemaDigest.count(1_200_000)).isEqualTo("1.2M");
    assertThat(AiSchemaDigest.count(450_000_000)).isEqualTo("450M");
    assertThat(AiSchemaDigest.count(3_400_000_000L)).isEqualTo("3.4B");
    assertThat(AiSchemaDigest.count(-1)).isEqualTo("?");
  }

  @Test
  void untrustedTextCannotBreakTheLineOrCarryHiddenInstructions() {
    assertThat(AiSchemaDigest.clean("a\nb\r\nc\td", 50)).isEqualTo("a b c d");
    assertThat(AiSchemaDigest.clean("ev‮il⁦x⁩ ​z⁠w﻿", 50)).isEqualTo("evilx zw");
    assertThat(AiSchemaDigest.clean("a b c\u0000d\u007Fe", 50)).isEqualTo("a b c d e");
    assertThat(AiSchemaDigest.clean("pq", 50)).isEqualTo("pq");
    assertThat(AiSchemaDigest.clean("  spaced   out  ", 50)).isEqualTo("spaced out");
    assertThat(AiSchemaDigest.clean("abcdef", 3)).isEqualTo("abc");
    // Never cut in the middle of a surrogate pair
    assertThat(AiSchemaDigest.clean("😀😀", 1)).isEqualTo("😀");
    assertThat(AiSchemaDigest.clean(null, 5)).isEmpty();
  }

  @Test
  void typeDetailGivesEverythingAboutOneType() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigestDetail", database -> {
      final Schema schema = database.getSchema();
      final VertexType party = schema.buildVertexType().withName("Party").withTotalBuckets(2).create();
      party.createProperty("id", Type.STRING).setMandatory(true);
      final VertexType account = schema.buildVertexType().withName("Account").withTotalBuckets(3).create();
      account.addSuperType(party);
      account.setCustomValue("description", "A bank account");
      account.createProperty("iban", Type.STRING).setRegexp("[A-Z]{2}[0-9]+").setMin("2");
      account.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "iban");
      committed(database, () -> {
        for (int i = 0; i < 10; i++)
          database.newVertex("Account").set("iban", "IT" + i).set("id", "a" + i).save();
      });

      final JSONObject detail = AiSchemaDigest.typeDetail(database, "Account");

      assertThat(detail.getString("name")).isEqualTo("Account");
      assertThat(detail.getString("category")).isEqualTo("vertex");
      assertThat(detail.getString("description")).isEqualTo("A bank account");
      assertThat(detail.getLong("rows")).isEqualTo(10L);
      assertThat(detail.getInt("bucketCount")).isEqualTo(3);
      final JSONArray buckets = detail.getJSONArray("buckets");
      assertThat(buckets.length()).isEqualTo(3);
      assertThat(buckets.getJSONObject(0).getString("name")).startsWith("Account");
      assertThat(detail.getJSONArray("parentTypes").getString(0)).isEqualTo("Party");
      final JSONArray properties = detail.getJSONArray("properties");
      JSONObject iban = null;
      JSONObject id = null;
      for (int i = 0; i < properties.length(); i++) {
        if (properties.getJSONObject(i).getString("name").equals("iban"))
          iban = properties.getJSONObject(i);
        if (properties.getJSONObject(i).getString("name").equals("id"))
          id = properties.getJSONObject(i);
      }
      assertThat(iban).isNotNull();
      assertThat(iban.getString("regexp")).isEqualTo("[A-Z]{2}[0-9]+");
      assertThat(iban.getString("min")).isEqualTo("2");
      assertThat(iban.has("inherited")).isFalse();
      assertThat(id).isNotNull();
      assertThat(id.getBoolean("inherited")).isTrue();
      assertThat(id.getBoolean("mandatory")).isTrue();
      assertThat(detail.getJSONArray("indexes").getJSONObject(0).getBoolean("unique")).isTrue();
      assertThat(detail.toString()).doesNotContain("\n");
    });
  }

  @Test
  void anUnknownTypeIsRefusedWithSuggestions() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigestUnknown", database -> {
      database.getSchema().createDocumentType("Account");
      database.getSchema().createDocumentType("AccountHistory");

      assertThatThrownBy(() -> AiSchemaDigest.typeDetail(database, "account"))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not exist")
          .hasMessageContaining("Did you mean: Account, AccountHistory?");
      assertThatThrownBy(() -> AiSchemaDigest.typeDetail(database, "Nothing")).hasMessageNotContaining("Did you mean");
      assertThatThrownBy(() -> AiSchemaDigest.typeDetail(database, " ")).hasMessageContaining("'name'");
    });
  }

  @Test
  void aHugeTypeIsCutRatherThanReturnedAsHalfJson() throws Exception {
    TestHelper.executeInNewDatabase("AiSchemaDigestHuge", database -> {
      final DocumentType type = database.getSchema().createDocumentType("Wide");
      for (int i = 0; i < 400; i++)
        type.createProperty("p" + i, Type.STRING);

      final JSONObject detail = AiSchemaDigest.typeDetail(database, "Wide");

      assertThat(detail.getJSONArray("properties").length()).isEqualTo(AiSchemaDigest.MAX_TYPE_PROPERTIES);
      assertThat(detail.getInt("propertiesTruncated")).isEqualTo(100);
      assertThat(detail.toString().length()).isLessThan(AiSchemaDigest.MAX_TYPE_CHARS);
    });
  }

  /**
   * Runs the writes in a transaction of their own that really commits. The test helper wraps each test in one transaction, and the
   * record counters the summary reads move only when a transaction commits, never on its pending writes.
   */
  private static void committed(final Database database, final Runnable writes) {
    database.commit();
    database.transaction(writes::run);
    database.begin();
  }

  private static String lineOf(final String digest, final String start) {
    return digest.lines().filter(l -> l.startsWith(start)).findFirst().orElseThrow(() -> new AssertionError("no line " + start + " in\n" + digest));
  }
}
