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
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * GH #8436: an array passed as a POSITIONAL parameter must behave the same as the
 * same array bound by name, both for {@code IN} and {@code CONTAINSANY}. The issue
 * reports the two operators accepting the array in opposite styles, and the
 * unsupported combinations failing silently with an empty result.
 */
class Issue8436PositionalInArrayTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Chunk8436");
      database.getSchema().getType("Chunk8436").createProperty("uuid", com.arcadedb.schema.Type.STRING);
      database.newDocument("Chunk8436").set("uuid", "u1").save();
      database.newDocument("Chunk8436").set("uuid", "u2").save();
      database.newDocument("Chunk8436").set("uuid", "u3").save();
    });
  }

  private static List<String> uuids(final ResultSet rs) {
    return rs.stream().map(r -> r.<String>getProperty("uuid")).sorted().toList();
  }

  @Test
  void positionalInQuestionMark() {
    final ResultSet rs = database.query("sql", "select uuid from Chunk8436 where uuid IN ?",
        new Object[] { List.of("u1", "u2") });
    assertThat(uuids(rs)).containsExactly("u1", "u2");
  }

  @Test
  void namedInParam() {
    final ResultSet rs = database.query("sql", "select uuid from Chunk8436 where uuid IN :ids",
        Map.of("ids", List.of("u1", "u2")));
    assertThat(uuids(rs)).containsExactly("u1", "u2");
  }

  @Test
  void positionalContainsAnyQuestionMark() {
    final ResultSet rs = database.query("sql", "select uuid from Chunk8436 where uuid CONTAINSANY ?",
        new Object[] { List.of("u1", "u2") });
    assertThat(uuids(rs)).containsExactly("u1", "u2");
  }

  @Test
  void namedContainsAnyParam() {
    final ResultSet rs = database.query("sql", "select uuid from Chunk8436 where uuid CONTAINSANY :ids",
        Map.of("ids", List.of("u1", "u2")));
    assertThat(uuids(rs)).containsExactly("u1", "u2");
  }
}
