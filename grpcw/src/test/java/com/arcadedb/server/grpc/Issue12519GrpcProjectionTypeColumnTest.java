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
package com.arcadedb.server.grpc;

import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12519: {@code ArcadeDbGrpcService.convertResultToGrpcRecord} recovered the type of a row that is not a record from its
 * {@code @type} column, for the rows a follower rebuilds from the JSON its leader answered a forwarded command with (issue #4688).
 * A local projection with a column of that name ({@code SELECT @type, name FROM P}) took the same path, so the client read it as a
 * record of that type and the row lost its projection shape. The type is recovered only for a row the follower marked.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12519GrpcProjectionTypeColumnTest {

  private ArcadeDbGrpcService service;

  @BeforeEach
  void createService() {
    service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
  }

  @AfterEach
  void releaseService() {
    service = null;
  }

  @Test
  void aProjectionWithATypeColumnIsNotARecord() {
    final ResultInternal row = new ResultInternal(columns("@type", "P", "name", "a"));

    final GrpcRecord record = service.convertResultToGrpcRecord(row, null, null);

    assertThat(record.getType()).isEmpty();
    assertThat(record.getRid()).isEmpty();
    assertThat(record.getPropertiesMap()).containsOnlyKeys("@type", "name");
    assertThat(record.getPropertiesMap().get("@type").getStringValue()).isEqualTo("P");
    assertThat(record.getPropertiesMap().get("name").getStringValue()).isEqualTo("a");
  }

  @Test
  void aProjectionWithATypeColumnAndAListColumnKeepsBothColumns() {
    final ResultInternal row = new ResultInternal(columns("@type", "P", "friends", List.of("#1:0", "#1:1")));

    final GrpcRecord record = service.convertResultToGrpcRecord(row, null, null);

    assertThat(record.getType()).isEmpty();
    assertThat(record.getPropertiesMap()).containsOnlyKeys("@type", "friends");
    assertThat(record.getPropertiesMap().get("friends").getListValue().getValuesCount()).isEqualTo(2);
  }

  @Test
  void aProjectionWithAnEmptyOrNonTextTypeColumnIsNotARecordEither() {
    assertThat(service.convertResultToGrpcRecord(new ResultInternal(columns("@type", "", "name", "a")), null, null).getType())
        .isEmpty();
    assertThat(service.convertResultToGrpcRecord(new ResultInternal(columns("@type", 7, "name", "a")), null, null).getType())
        .isEmpty();
  }

  @Test
  void aRowRebuiltFromTheLeadersJsonIsTypedWithItsTypeColumnKept() {
    final ResultInternal row = new ResultInternal(columns("@type", "P", "name", "a"));
    row.setMetadata(ResultInternal.FORWARDED_ROW_METADATA, Boolean.TRUE);

    final GrpcRecord record = service.convertResultToGrpcRecord(row, null, null);

    assertThat(record.getType()).isEqualTo("P");
    assertThat(record.getPropertiesMap()).containsKeys("@type", "name");
    assertThat(record.getPropertiesMap().get("name").getStringValue()).isEqualTo("a");
  }

  @Test
  void aMarkedRowWithNoTypeStaysUntyped() {
    final ResultInternal row = new ResultInternal(columns("name", "a"));
    row.setMetadata(ResultInternal.FORWARDED_ROW_METADATA, Boolean.TRUE);

    final GrpcRecord record = service.convertResultToGrpcRecord(row, null, null);

    assertThat(record.getType()).isEmpty();
    assertThat(record.getPropertiesMap()).containsOnlyKeys("name");
  }

  private static Map<String, Object> columns(final Object... namesAndValues) {
    final Map<String, Object> columns = new LinkedHashMap<>();
    for (int i = 0; i < namesAndValues.length; i += 2)
      columns.put((String) namesAndValues[i], namesAndValues[i + 1]);
    return columns;
  }
}
