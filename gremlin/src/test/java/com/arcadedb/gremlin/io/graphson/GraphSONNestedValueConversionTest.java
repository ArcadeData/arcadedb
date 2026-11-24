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
package com.arcadedb.gremlin.io.graphson;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The conversion of the nested lists and maps a typed GraphSON file carries into values ArcadeDB can store: the result is the
 * same at any depth, and the text of a nested value is only built for an element that has no stored equivalent.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphSONNestedValueConversionTest {
  private static final String DATABASE_PATH = "target/databases/graphson-nested-value-conversion";
  private static final int    DEPTH         = 40;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void nestedListsAreConvertedWithoutTheirTextBeingBuilt() {
    final AtomicInteger texts = new AtomicInteger();

    CountedList innermost = new CountedList(texts);
    innermost.add("leaf");
    innermost.add(7);
    Object value = innermost;
    List<Object> expected = new ArrayList<>(List.of("leaf", 7));
    for (int i = 0; i < DEPTH; i++) {
      final CountedList outer = new CountedList(texts);
      outer.add(value);
      value = outer;
      expected = new ArrayList<>(List.of(expected));
    }

    final Object stored = ArcadeGraphSONValuesModule.storable(value, "the text in the file");

    assertThat(texts.get()).isZero();
    assertThat(stored).isEqualTo(expected);
  }

  @Test
  void nestedMapsAreConvertedWithoutTheirTextBeingBuilt() {
    final AtomicInteger texts = new AtomicInteger();

    CountedMap innermost = new CountedMap(texts);
    innermost.put("leaf", 7);
    Object value = innermost;
    Map<Object, Object> expected = new LinkedHashMap<>(Map.of("leaf", 7));
    for (int i = 0; i < DEPTH; i++) {
      final CountedMap outer = new CountedMap(texts);
      outer.put("k" + i, value);
      value = outer;
      expected = new LinkedHashMap<>(Map.of("k" + i, expected));
    }

    final Object stored = ArcadeGraphSONValuesModule.storable(value, "the text in the file");

    assertThat(texts.get()).isZero();
    assertThat(stored).isEqualTo(expected);
  }

  @Test
  void nestedValuesWithoutAStoredEquivalentKeepTheirText() {
    final List<Object> list = new ArrayList<>();
    list.add(MonthDay.of(3, 4));
    list.add((short) 3);
    list.add('z');
    list.add(ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    list.add(null);
    final Map<Object, Object> map = new LinkedHashMap<>();
    map.put(MonthDay.of(1, 2), MonthDay.of(5, 6));
    map.put("nested", new ArrayList<>(List.of(MonthDay.of(7, 8))));
    list.add(map);

    final Object stored = ArcadeGraphSONValuesModule.storable(new ArrayList<>(List.of(list)), "the text in the file");

    assertThat(stored).isInstanceOf(List.class);
    final List<?> outer = (List<?>) stored;
    assertThat(outer).hasSize(1);
    final List<?> converted = (List<?>) outer.getFirst();
    assertThat(converted).hasSize(6);
    assertThat(converted.get(0)).isEqualTo("--03-04");
    assertThat(converted.get(1)).isEqualTo((short) 3);
    assertThat(converted.get(2)).isEqualTo("z");
    assertThat((byte[]) converted.get(3)).containsExactly(1, 2, 3);
    assertThat(converted.get(4)).isNull();
    final Map<Object, Object> expectedMap = new LinkedHashMap<>();
    expectedMap.put("--01-02", "--05-06");
    expectedMap.put("nested", new ArrayList<>(List.of("--07-08")));
    assertThat(converted.get(5)).isEqualTo(expectedMap);
  }

  @Test
  void aValueWithoutAStoredEquivalentKeepsTheTextOfTheFile() {
    assertThat(ArcadeGraphSONValuesModule.storable(MonthDay.of(1, 2), "the text in the file")).isEqualTo("the text in the file");
    assertThat(ArcadeGraphSONValuesModule.storable(5, "the text in the file")).isEqualTo(5);
    assertThat(ArcadeGraphSONValuesModule.storable(null, "the text in the file")).isNull();
  }

  @Test
  void deeplyNestedTypedValuesAreImportedUnchanged() throws IOException {
    final File file = new File("target/graphson-nested-value-conversion.graphson");
    String typed = "{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"gx:MonthDay\",\"@value\":\"--03-04\"},{\"@type\":\"gx:Int16\",\"@value\":3}]}";
    Object expected = new ArrayList<>(List.of("--03-04", (short) 3));
    for (int i = 0; i < DEPTH; i++) {
      if (i % 2 == 0) {
        typed = "{\"@type\":\"g:List\",\"@value\":[" + typed + "]}";
        expected = new ArrayList<>(List.of(expected));
      } else {
        typed = "{\"@type\":\"g:Map\",\"@value\":[\"k\"," + typed + "]}";
        expected = new LinkedHashMap<>(Map.of("k", expected));
      }
    }
    Files.writeString(file.toPath(), "{\"id\":\"a\",\"label\":\"P\",\"properties\":{\"deep\":[{\"id\":\"1\",\"value\":" + typed + "}]}}\n");

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      try (final ResultSet rs = database.query("sql", "SELECT FROM P")) {
        final Document p = rs.next().getElement().orElseThrow();
        assertThat((Object) p.get("deep")).isEqualTo(expected);
      }
    } finally {
      file.delete();
    }
  }

  /** A list that counts how many times its text is built. */
  private static final class CountedList extends ArrayList<Object> {
    private final AtomicInteger texts;

    private CountedList(final AtomicInteger texts) {
      this.texts = texts;
    }

    @Override
    public String toString() {
      texts.incrementAndGet();
      return super.toString();
    }
  }

  /** A map that counts how many times its text is built. */
  private static final class CountedMap extends LinkedHashMap<Object, Object> {
    private final AtomicInteger texts;

    private CountedMap(final AtomicInteger texts) {
      this.texts = texts;
    }

    @Override
    public String toString() {
      texts.incrementAndGet();
      return super.toString();
    }
  }
}
