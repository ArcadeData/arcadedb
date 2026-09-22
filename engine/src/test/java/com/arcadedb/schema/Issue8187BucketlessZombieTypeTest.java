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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8187
 * <p>
 * A {@code schema.json} written by a version affected by #8169 carries a dropped type as a real entry with an empty
 * bucket list. It was loaded silently and the only symptom was the first insert failing. The open now names every
 * bucket-less type that can never hold a record (no buckets and no subtypes) and says how to remove it, the insert
 * error says the same, and {@code DROP TYPE} removes the zombie for good.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8187BucketlessZombieTypeTest extends TestHelper {

  @Test
  void aZombieTypeIsReportedAtOpenAndDropTypeRemovesItForGood() throws IOException {
    database.getSchema().createDocumentType("Invoice");
    injectZombie("Order", "PO");

    final List<String> warnings = WarningCapture.captureWarnings(this::reopenDatabase);
    assertThat(warnings).anyMatch(w -> w.contains("'Order'") && w.contains("DROP TYPE"));
    assertThat(warnings).noneMatch(w -> w.contains("'Invoice'") && w.contains("no buckets"));

    assertThat(database.getSchema().existsType("Order")).isTrue();
    assertThat(database.getSchema().existsType("PO")).isTrue();

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Order").set("a", 1).save()))
        .isInstanceOf(SchemaException.class).hasMessageContaining("Order").hasMessageContaining("DROP TYPE");

    database.command("sql", "DROP TYPE Order");
    assertThat(database.getSchema().existsType("Order")).isFalse();
    assertThat(database.getSchema().existsType("PO")).isFalse();

    final List<String> afterDrop = WarningCapture.captureWarnings(this::reopenDatabase);
    assertThat(afterDrop).noneMatch(w -> w.contains("'Order'"));
    assertThat(database.getSchema().existsType("Order")).isFalse();
    assertThat(database.getSchema().existsType("PO")).isFalse();
    assertThat(database.getSchema().getTypes()).extracting(DocumentType::getName).containsExactly("Invoice");
  }

  @Test
  void aBucketlessSuperTypeIsNotReported() {
    final DocumentType base = database.getSchema().createDocumentType("Base");
    database.getSchema().createDocumentType("Child").addSuperType(base);
    for (final var bucket : List.copyOf(base.getBuckets(false)))
      base.removeBucket(bucket);
    assertThat(base.getBuckets(false)).isEmpty();

    final List<String> warnings = WarningCapture.captureWarnings(this::reopenDatabase);
    assertThat(warnings).noneMatch(w -> w.contains("'Base'"));
    assertThat(database.getSchema().getType("Base").getBuckets(false)).isEmpty();
  }

  private void injectZombie(final String name, final String alias) throws IOException {
    database.close();

    final Path schemaFile = Path.of(getDatabasePath(), "schema.json");
    final JSONObject root = new JSONObject(Files.readString(schemaFile, StandardCharsets.UTF_8));
    root.getJSONObject("types").put(name, new JSONObject()//
        .put("type", "d").put("parents", new JSONArray()).put("buckets", new JSONArray())
        .put("aliases", new JSONArray().put(alias)).put("properties", new JSONObject()).put("indexes", new JSONObject())
        .put("custom", new JSONObject()));
    Files.writeString(schemaFile, root.toString(), StandardCharsets.UTF_8);
    database = null;
  }
}
