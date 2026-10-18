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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10201: a JSONL EXPORT DATABASE / IMPORT DATABASE round trip
 * <ol>
 *   <li>re-pointed a link to a deleted record at whatever live record the import placed at its old RID;</li>
 *   <li>changed the type of every undeclared property value JSON cannot tell apart (BigDecimal, Long, Short, Byte, Float,
 *   dates, byte[], ...), at the top level and inside untyped lists and maps;</li>
 *   <li>recreated every type with the default bucket count.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10201JsonlRoundTripFidelityTest {
  private static final String SOURCE_PATH = "target/databases/issue10201-source";
  private static final String TARGET_PATH = "target/databases/issue10201-target";
  private static final String FILE        = "issue10201.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
  }

  @Test
  void aLinkToADeletedRecordIsNotRepointedAtALiveOne() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.uid LONG");
      source.command("sql", "CREATE PROPERTY D.lnk LINK");
      source.command("sql", "CREATE PROPERTY D.lst LIST OF LINK");
      source.command("sql", "CREATE PROPERTY D.mp MAP OF LINK");
      source.transaction(() -> {
        final MutableDocument deleted = source.newDocument("D").set("uid", 2L).save();
        final MutableDocument live = source.newDocument("D").set("uid", 5L).save();
        final RID gone = deleted.getIdentity();
        source.newDocument("D").set("uid", 1L, "lnk", gone, "lst", List.of(gone, live.getIdentity()), "mp", Map.of("a", gone, "b",
            live.getIdentity()), "free", gone, "freeList", new ArrayList<>(List.of(gone, live.getIdentity()))).save();
        source.newDocument("D").set("uid", 3L).save();
        source.newDocument("D").set("uid", 4L).save();
      });
      source.transaction(() -> source.command("sql", "DELETE FROM D WHERE uid = 2"));
      exportTo(source);
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      // REPORTED, NOT ONLY CLEARED: lnk, lst[0], mp.a, free AND freeList[0]
      try (final ResultSet rs = target.command("sql",
          "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath())) {
        assertThat(rs.next().<Long>getProperty("danglingLinks")).isEqualTo(5L);
      }

      final Document one = byUid(target, 1L);
      final RID live = byUid(target, 5L).getIdentity();
      assertThat(one.get("lnk")).isNull();
      // A LIST OF / MAP OF LINK HOLDS NO NULL: THE ELEMENT WHOSE TARGET IS GONE IS DROPPED
      assertThat(one.getList("lst")).containsExactly(live);
      assertThat(one.getMap("mp")).containsOnlyKeys("b").containsEntry("b", live);
      // AN UNDECLARED LINK IS A LINK TOO: REMAPPED WHEN ITS TARGET CAME ALONG, AND NEVER RE-POINTED WHEN IT DID NOT
      assertThat(one.get("free")).isNull();
      assertThat(one.getList("freeList")).containsExactly(null, live);
    }
  }

  /** Dozens of dangling links of mixed kinds in one record: removed from the typed list in order, cleared elsewhere. */
  @Test
  void manyDanglingLinksOfMixedKindsInOneRecord() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.uid LONG");
      source.command("sql", "CREATE PROPERTY D.lst LIST OF LINK");
      source.command("sql", "CREATE PROPERTY D.mp MAP OF LINK");
      source.transaction(() -> {
        final List<RID> list = new ArrayList<>();
        final Map<String, RID> map = new LinkedHashMap<>();
        final List<Object> free = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
          final RID rid = source.newDocument("D").set("uid", 100L + i).save().getIdentity();
          list.add(rid);
          map.put("k" + i, rid);
          free.add(rid);
        }
        source.newDocument("D").set("uid", 1L, "lst", list, "mp", map, "free", free).save();
      });
      // EVERY ODD ONE GOES
      source.transaction(() -> source.command("sql", "DELETE FROM D WHERE uid >= 100 AND uid % 2 = 1"));
      exportTo(source);
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      importInto(target);
      final Document one = byUid(target, 1L);

      final List<RID> expected = new ArrayList<>();
      for (int i = 0; i < 40; i += 2)
        expected.add(byUid(target, 100L + i).getIdentity());

      assertThat(one.getList("lst")).containsExactlyElementsOf(expected);
      assertThat(one.getMap("mp")).hasSize(20);
      final List<Object> free = one.getList("free");
      assertThat(free).hasSize(40);
      for (int i = 0; i < 40; i++)
        assertThat(free.get(i)).isEqualTo(i % 2 == 0 ? expected.get(i / 2) : null);
    }
  }

  /** A MANDATORY link cannot be cleared: the record fails the import as any record that breaks its schema does. */
  @Test
  void aDanglingMandatoryLinkFailsTheImportInsteadOfBeingRepointed() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.uid LONG");
      source.transaction(() -> {
        final MutableDocument target = source.newDocument("D").set("uid", 2L).save();
        source.newDocument("D").set("uid", 1L, "lnk", target.getIdentity()).save();
        source.newDocument("D").set("uid", 3L).set("lnk", target.getIdentity()).save();
      });
      // THE TARGET GOES, THE LINKS TO IT STAY, AND EVERY REMAINING RECORD SATISFIES THE DECLARATION
      source.transaction(() -> source.command("sql", "DELETE FROM D WHERE uid = 2"));
      source.command("sql", "CREATE PROPERTY D.lnk LINK (mandatory true, notnull true)");
      exportTo(source);
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      assertThatThrownBy(() -> importInto(target)).hasStackTraceContaining("lnk");
      // NOTHING WAS LEFT POINTING AT ANOTHER RECORD
      try (final ResultSet rs = target.query("sql", "SELECT FROM D WHERE lnk IS NOT NULL")) {
        while (rs.hasNext()) {
          final Document d = rs.next().getElement().get();
          assertThat(d.get("lnk")).isNotEqualTo(d.getIdentity());
        }
      }
    }
  }

  @Test
  void undeclaredForwardLinksAtAnyDepthAreResolved() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.uid LONG");
      source.transaction(() -> {
        // THE FIRST RECORD IS EXPORTED BEFORE THE ONE IT LINKS TO: EVERY LINK IN IT IS A FORWARD REFERENCE
        final MutableDocument first = source.newDocument("D").set("uid", 1L).save();
        final MutableDocument later = source.newDocument("D").set("uid", 2L).save();
        first.set("free", later.getIdentity());
        first.set("list", new ArrayList<>(List.of("x", later.getIdentity())));
        first.set("map", new LinkedHashMap<>(Map.of("k", new ArrayList<>(List.of(later.getIdentity())))));
        first.newEmbeddedDocument("D", "emb").set("ref", later.getIdentity());
        first.save();
      });
      exportTo(source);
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      importInto(target);

      final Document one = byUid(target, 1L);
      final RID later = byUid(target, 2L).getIdentity();
      assertThat(one.get("free")).isEqualTo(later);
      assertThat(one.getList("list")).containsExactly("x", later);
      assertThat(((List<?>) one.getMap("map").get("k")).get(0)).isEqualTo(later);
      assertThat(((Document) one.get("emb")).get("ref")).isEqualTo(later);
    }
  }

  @Test
  void undeclaredValuesKeepTheirTypes() {
    final Map<String, Object> values = new LinkedHashMap<>();
    values.put("bigdec", new BigDecimal("1.50"));
    values.put("long", 3L);
    values.put("float", 1.5f);
    values.put("short", (short) 7);
    values.put("byte", (byte) 5);
    values.put("integralDouble", 2.0);
    values.put("nan", Double.NaN);
    values.put("int", 42);
    values.put("string", "héllo");
    values.put("bool", true);
    values.put("date", new Date(1_700_000_000_000L));
    values.put("ldate", LocalDate.of(2026, 1, 2));
    values.put("ldt", LocalDateTime.of(2026, 1, 2, 3, 4, 5, 6_000_000));
    values.put("nanos", LocalDateTime.of(2023, 11, 14, 22, 13, 20, 123_456_789));
    values.put("bin", new byte[] { 1, 2, 3 });
    values.put("floats", new float[] { 1f, 2f });
    values.put("ints", new int[] { 4, 5 });

    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.transaction(() -> {
        final MutableDocument d = source.newDocument("D");
        values.forEach(d::set);
        d.set("list", new ArrayList<>(values.values()));
        d.set("map", new LinkedHashMap<>(values));
        d.set("nested", new LinkedHashMap<>(Map.of("inner", new ArrayList<>(List.of(9L, new BigDecimal("0.10"))))));
        // A MAP WITH A KEY "t" NAMING A TYPE IS STILL A MAP, NOT AN EMBEDDED DOCUMENT
        d.set("tmap", new HashMap<>(Map.of("t", "D", "p", 1L)));
        final MutableEmbeddedDocument embedded = d.newEmbeddedDocument("D", "emb");
        embedded.set("long", 8L).set("bigdec", new BigDecimal("2.500"));
        d.save();
      });
      exportTo(source);

      try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
        importInto(target);

        final Document x = only(source);
        final Document y = only(target);
        for (final String key : values.keySet())
          assertSame(key, x.get(key), y.get(key));

        final List<?> lx = x.getList("list");
        final List<?> ly = y.getList("list");
        assertThat(ly).hasSameSizeAs(lx);
        for (int i = 0; i < lx.size(); i++)
          assertSame("list[" + i + "]", lx.get(i), ly.get(i));

        final Map<String, Object> mx = x.getMap("map");
        final Map<String, Object> my = y.getMap("map");
        for (final String key : values.keySet())
          assertSame("map[" + key + "]", mx.get(key), my.get(key));

        final List<?> inner = (List<?>) y.getMap("nested").get("inner");
        assertSame("nested.inner[0]", 9L, inner.get(0));
        assertSame("nested.inner[1]", new BigDecimal("0.10"), inner.get(1));

        assertThat(y.get("tmap")).isInstanceOf(Map.class);
        assertSame("tmap.p", 1L, y.getMap("tmap").get("p"));

        final Document emb = (Document) y.get("emb");
        assertSame("emb.long", 8L, emb.get("long"));
        assertSame("emb.bigdec", new BigDecimal("2.500"), emb.get("bigdec"));
      }
    }
  }

  /** A type tree naming a property the record does not carry, or a type this version does not know, is ignored. */
  @Test
  void aTypeTreeEntryWithoutItsPropertyOrWithAnUnknownTypeIsIgnored() throws Exception {
    final File file = new File(SOURCE_PATH + "-hand.jsonl");
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), ""
        + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":2}}\n"
        + "{\"t\":\"schema\",\"c\":{\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"D\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"D_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}}}}}\n"
        + "{\"t\":\"d\",\"c\":{\"p\":{\"a\":5,\"b\":\"x\"},\"y\":{\"missing\":\"LONG\",\"a\":\"LONG\",\"b\":\"FROM_THE_FUTURE\"},\"r\":\"#1:0\",\"t\":\"D\"}}\n");
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      final Document d = only(target);
      assertThat(d.get("a")).isEqualTo(5L);
      assertThat(d.get("b")).isEqualTo("x");
      assertThat(d.has("missing")).isFalse();
    } finally {
      file.delete();
    }
  }

  @Test
  void bucketCountsAreKept() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D BUCKETS 3");
      source.command("sql", "CREATE VERTEX TYPE V BUCKETS 2");
      source.command("sql", "CREATE DOCUMENT TYPE A BUCKETS 0");
      source.command("sql", "ALTER TYPE D BUCKETSELECTIONSTRATEGY `thread`");
      source.transaction(() -> {
        for (int i = 0; i < 10; i++)
          source.newDocument("D").set("n", i).save();
      });
      exportTo(source);
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      importInto(target);
      assertThat(target.getSchema().getType("D").getBuckets(false)).hasSize(3);
      assertThat(target.getSchema().getType("V").getBuckets(false)).hasSize(2);
      assertThat(target.getSchema().getType("A").getBuckets(false)).isEmpty();
      assertThat(target.getSchema().getType("D").getBucketSelectionStrategy().getName()).isEqualTo("thread");
      assertThat(target.countType("D", false)).isEqualTo(10);
    }
  }

  private static void assertSame(final String what, final Object expected, final Object actual) {
    assertThat(actual).as(what).isNotNull();
    assertThat(actual.getClass()).as(what + " class").isEqualTo(expected.getClass());
    if (expected instanceof byte[] b)
      assertThat((byte[]) actual).as(what).isEqualTo(b);
    else if (expected instanceof float[] f)
      assertThat((float[]) actual).as(what).isEqualTo(f);
    else if (expected instanceof int[] f)
      assertThat((int[]) actual).as(what).isEqualTo(f);
    else if (expected instanceof Double d && d.isNaN())
      assertThat((Double) actual).as(what).isNaN();
    else if (expected instanceof BigDecimal b)
      assertThat(actual).as(what).isEqualTo(b);
    else
      assertThat(actual).as(what + " " + Arrays.asList(expected, actual)).isEqualTo(expected);
  }

  private static void exportTo(final Database source) {
    source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
  }

  private static void importInto(final Database target) {
    target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());
  }

  private static Document only(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM D")) {
      final Result r = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return r.getElement().get();
    }
  }

  private static Document byUid(final Database db, final long uid) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM D WHERE uid = ?", uid)) {
      return rs.next().getElement().get();
    }
  }
}
