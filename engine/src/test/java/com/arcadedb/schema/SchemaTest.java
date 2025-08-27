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
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaTest extends TestHelper {

  @Test
  public void tesSchemaSettings() {
    database.transaction(() -> {
      final ZoneId zoneId = database.getSchema().getZoneId();
      assertThat(zoneId).isNotNull();
      database.getSchema().setZoneId(ZoneId.of("America/New_York"));
      assertThat(database.getSchema().getZoneId()).isEqualTo(ZoneId.of("America/New_York"));

      final TimeZone timeZone = database.getSchema().getTimeZone();
      assertThat(timeZone).isNotNull();
      database.getSchema().setTimeZone(TimeZone.getTimeZone("UK"));
      assertThat(database.getSchema().getTimeZone()).isEqualTo(TimeZone.getTimeZone("UK"));

      final String dateFormat = database.getSchema().getDateFormat();
      assertThat(dateFormat).isNotNull();
      database.getSchema().setDateFormat("yyyy-MMM-dd");
      assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MMM-dd");

      final String dateTimeFormat = database.getSchema().getDateTimeFormat();
      assertThat(dateTimeFormat).isNotNull();
      database.getSchema().setDateTimeFormat("yyyy-MMM-dd HH:mm:ss");
      assertThat(database.getSchema().getDateTimeFormat()).isEqualTo("yyyy-MMM-dd HH:mm:ss");

      final String encoding = database.getSchema().getEncoding();
      assertThat(encoding).isNotNull();
      database.getSchema().setEncoding("UTF-8");
      assertThat(database.getSchema().getEncoding()).isEqualTo("UTF-8");
    });
  }

  @Test
  public void testRenameDocument() {
    final DocumentType type = database.getSchema().createDocumentType("Doc1");
    type.createProperty("id", Type.STRING);
    type.createProperty("total", Type.DOUBLE);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "total");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Doc1").set("id", "id-" + i, "total", i).save();
    });

    type.rename("Doc2");

    assertThat(database.getSchema().existsType("Doc1")).isFalse();

    assertThat(database.getSchema().existsType("Doc2")).isTrue();
    assertThat(database.getSchema().getType("Doc2").getProperty("id").getType()).isEqualByComparingTo(Type.STRING);
    assertThat(database.getSchema().getType("Doc2").getProperty("total").getType()).isEqualByComparingTo(Type.DOUBLE);

    assertThat(database.countType("Doc2", true)).isEqualTo(100L);
  }

  @Test
  public void testRenameVertexAndEdges() {
    final VertexType type = database.getSchema().createVertexType("V1");
    type.createProperty("id", Type.STRING);
    type.createProperty("total", Type.DOUBLE);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "total");

    final EdgeType edgeType = database.getSchema().createEdgeType("E1");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newVertex("V1").set("id", "id-" + i, "total", i).save();
    });

    type.rename("V2");
    edgeType.rename("E2");

    assertThat(database.getSchema().existsType("V1")).isFalse();

    assertThat(database.getSchema().existsType("V2")).isTrue();
    assertThat(database.getSchema().getType("V2").getProperty("id").getType()).isEqualByComparingTo(Type.STRING);
    assertThat(database.getSchema().getType("V2").getProperty("total").getType()).isEqualByComparingTo(Type.DOUBLE);

    assertThat(database.countType("V2", true)).isEqualTo(100L);

    assertThat(database.getSchema().existsType("E1")).isFalse();

    assertThat(database.getSchema().existsType("E2")).isTrue();
  }
}
