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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11207: {@link GrpcTypeConverter#toGrpcValue(Object)}, the encoder of the search and time series RPCs, matched
 * {@code List} but no other container, so a {@link JSONArray} - which is an {@code Iterable} and not a {@code List}, and is
 * what a search hit's array property is - reached the client as its JSON text, where the query path sends the same property
 * as a list. The two encoders now share their scalar and container arms, so a value of a given Java type is encoded the same
 * way whichever RPC returned it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcValueEncoderParityTest {
  private static final String DB_PATH = "target/databases/grpc-value-encoder-parity-test";

  @Test
  void aJsonArrayIsAList() {
    final GrpcValue value = GrpcTypeConverter.toGrpcValue(new JSONArray(List.of("a", "b")));

    assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(value.getListValue().getValuesList()).extracting(GrpcValue::getStringValue).containsExactly("a", "b");
  }

  @Test
  void aJsonObjectWithAnArrayIsAMapWithAList() {
    final GrpcValue value = GrpcTypeConverter.toGrpcValue(new JSONObject().put("tags", new JSONArray(List.of(1, 2))));

    assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.MAP_VALUE);
    final GrpcValue tags = value.getMapValue().getEntriesOrThrow("tags");
    assertThat(tags.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(tags.getListValue().getValuesList()).extracting(GrpcValue::getInt32Value).containsExactly(1, 2);
  }

  /** A Set, the other Collection a property holds, was a string too: the converter's only container arm was List. */
  @Test
  void aSetAndAnIteratorAreLists() {
    final GrpcValue set = GrpcTypeConverter.toGrpcValue(new LinkedHashSet<>(List.of("x", "y")));
    final GrpcValue iterator = GrpcTypeConverter.toGrpcValue(List.of(3L, 4L).iterator());

    assertThat(set.getListValue().getValuesList()).extracting(GrpcValue::getStringValue).containsExactly("x", "y");
    assertThat(iterator.getListValue().getValuesList()).extracting(GrpcValue::getInt64Value).containsExactly(3L, 4L);
  }

  @Test
  void theTextKindsStayText() {
    final UUID uuid = UUID.randomUUID();

    assertThat(GrpcTypeConverter.toGrpcValue('c').getStringValue()).isEqualTo("c");
    assertThat(GrpcTypeConverter.toGrpcValue(uuid).getStringValue()).isEqualTo(uuid.toString());
    assertThat(GrpcTypeConverter.toGrpcValue(Type.STRING).getStringValue()).isEqualTo(String.valueOf(Type.STRING));
    assertThat(GrpcTypeConverter.toGrpcValue(null).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  /** The reported path end to end: an array property of a full-text hit, through the RPC's own conversion of the hits. */
  @Test
  void anArrayPropertyOfAFullTextHitIsAList() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Database database = new DatabaseFactory(DB_PATH).create();
    try {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Article");
        type.createProperty("text", Type.STRING);
        type.createProperty("tags", Type.LIST);
        type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "text");
        database.newDocument("Article").set("text", "graph databases").set("tags", List.of("graph", "db")).save();
      });

      final FullTextSearchResponse response = GrpcVectorSearch.fullTextSearch(database,
          FullTextSearchRequest.newBuilder().setQueryText("graph").setTypeName("Article").build());

      assertThat(response.getResultsCount()).isEqualTo(1);
      final GrpcValue tags = response.getResults(0).getRecord().getPropertiesOrThrow("tags");
      assertThat(tags.getKindCase()).as("the same kind the query path sends for the property").isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      assertThat(tags.getListValue().getValuesList()).extracting(GrpcValue::getStringValue).containsExactly("graph", "db");
    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
