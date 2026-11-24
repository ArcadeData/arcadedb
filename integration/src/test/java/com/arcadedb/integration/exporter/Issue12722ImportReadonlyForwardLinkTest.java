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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12722: a JSONL IMPORT DATABASE of a READONLY link property that points at a record the export writes later (the exporter
 * writes every vertex before every document) failed, and imported nothing: the importer saves such a record with the link unresolved
 * and rewrites it once the whole file is read, and that second save was validated as an alteration of the readonly property.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12722ImportReadonlyForwardLinkTest {
  private static final String SOURCE_PATH = "target/databases/issue12722-source";
  private static final String TARGET_PATH = "target/databases/issue12722-target";
  private static final String FILE        = "issue12722.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "LINK", "LINK OF Target", "LIST OF LINK", "LIST OF Target", "MAP OF LINK", "MAP OF Target" })
  void aReadonlyLinkToADocumentExportedLaterIsRestored(final String declaration) {
    final Function<RID, Object> wrap = wrapperOf(declaration);
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p " + declaration + " (readonly true)");
      source.transaction(() -> {
        final MutableDocument target = source.newDocument("Target").set("id", 7L);
        target.save();
        source.newVertex("Holder").set("p", wrap.apply(target.getIdentity())).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());

      assertThat(target.getSchema().getType("Holder").getProperty("p").isReadonly()).as("the import leaves the property readonly").isTrue();
      assertThat(idOfLinkIn(target)).isEqualTo(7L);
    }
  }

  /**
   * The relaxation is the import's own rewrite of the links it could not resolve, and nothing else: afterwards the property is
   * as readonly as it was declared.
   */
  @Test
  void theRestoredPropertyStaysReadonlyForEveryLaterWrite() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p LINK OF Target (readonly true)");
      source.transaction(() -> {
        final MutableDocument target = source.newDocument("Target");
        target.save();
        source.newVertex("Holder").set("p", target.getIdentity()).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());

      assertThatThrownBy(() -> target.transaction(() -> {
        final MutableDocument other = target.newDocument("Target").save();
        final MutableVertex holder = target.iterateType("Holder", false).next().asVertex().modify();
        holder.set("p", other.getIdentity()).save();
      })).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
    }
  }

  private static Function<RID, Object> wrapperOf(final String declaration) {
    if (declaration.startsWith("LIST"))
      return rid -> List.of(rid);
    if (declaration.startsWith("MAP"))
      return rid -> Map.of("k", rid);
    return rid -> rid;
  }

  /** The {@code id} of the Target the Holder's link names, whatever the container the link sits in. */
  private static long idOfLinkIn(final Database db) {
    db.begin();
    try {
      final Document holder = db.iterateType("Holder", false).next().asDocument();
      final Object value = holder.get("p");
      final Object link = value instanceof List<?> list ? list.getFirst() : value instanceof Map<?, ?> map ? map.get("k") : value;
      return ((Identifiable) link).asDocument().getLong("id");
    } finally {
      db.rollback();
    }
  }
}
