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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8230, follow-up to #7963.
 * <ol>
 * <li>A {@code schema.json} that cannot be parsed used to be swallowed by {@code readConfiguration()}, so the empty
 * graph it had assembled was published over a perfectly good previous generation.</li>
 * <li>A full load that aborts after {@code readConfiguration()} restored the types (#7963) but not triggers,
 * materialized views, continuous aggregates, function libraries and extensions: those were replaced in place, and the
 * previous generation's trigger listeners were torn down.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8230AbortedSchemaLoadKeepsSchemaMembersTest extends TestHelper {
  private static final String TARGET = "Target8230";
  private static final String AUDIT  = "Audit8230";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TARGET);
    database.command("sql", "CREATE PROPERTY " + TARGET + ".name STRING");
    database.command("sql", "CREATE DOCUMENT TYPE " + AUDIT);
    database.command("sql", "CREATE PROPERTY " + AUDIT + ".what STRING");
    database.command("sql",
        "CREATE TRIGGER audit8230 AFTER CREATE ON TYPE " + TARGET + " EXECUTE SQL \"INSERT INTO " + AUDIT
            + " SET what = 'created'\"");
    database.command("sql", "CREATE MATERIALIZED VIEW View8230 AS SELECT name FROM " + TARGET + " REFRESH INCREMENTAL");
    database.command("sql", "DEFINE FUNCTION lib8230.twice 'return x * 2' PARAMETERS [x] LANGUAGE js");
    schema().setExtension("ext8230", new JSONObject().put("k", "v"));
    schema().saveConfiguration();
  }

  /** A full load that aborts after readConfiguration() leaves every member of the previous generation live. */
  @Test
  void anAbortedFullLoadKeepsTheTriggersViewsLibrariesAndExtensionsOfThePreviousGeneration() throws Exception {
    final LocalSchema schema = schema();
    final MaterializedViewImpl viewBefore = (MaterializedViewImpl) schema.getMaterializedView("View8230");
    assertThat(viewBefore.getChangeListener()).isNotNull();

    schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new FailingBucketFactoryHandler());
    try {
      assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).hasMessageContaining("issue8230");
    } finally {
      schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
    }

    assertThat(schema.existsTrigger("audit8230")).isTrue();
    insertTarget("after-abort");
    assertThat(auditRows()).as("the trigger of the previous generation still fires, exactly once").isEqualTo(1);

    assertThat(schema.getMaterializedView("View8230")).isSameAs(viewBefore);
    assertThat(viewBefore.getChangeListener()).as("and the view is still refreshed").isNotNull();
    assertThat(schema.hasFunctionLibrary("lib8230")).isTrue();
    assertThat(schema.getExtension("ext8230")).isNotNull();

    // A later load still works, and does not stack a second listener.
    schema.load(ComponentFile.MODE.READ_WRITE, true);
    insertTarget("after-reload");
    assertThat(auditRows()).isEqualTo(2);
    assertThat(schema.existsTrigger("audit8230")).isTrue();
    assertThat(schema.hasFunctionLibrary("lib8230")).isTrue();
    assertThat(schema.getExtension("ext8230")).isNotNull();
    assertThat(((MaterializedViewImpl) schema.getMaterializedView("View8230")).getChangeListener()).isNotNull();
  }

  /** A schema.json that cannot be parsed must leave the previous generation published, and must not be overwritten. */
  @Test
  void aSchemaFileThatCannotBeParsedLeavesThePreviousGenerationPublished() throws Exception {
    final LocalSchema schema = schema();
    final File schemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final File prevFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    root.getJSONObject("settings").remove("dateFormat");
    final String broken = root.toString();
    Files.writeString(schemaFile.toPath(), broken, StandardCharsets.UTF_8);
    // The previous copy is the fallback of a CORRUPT file only; a parseable file with a bad value is not one.
    Files.deleteIfExists(prevFile.toPath());

    assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).isInstanceOf(SchemaException.class);

    assertThat(schema.existsType(TARGET)).isTrue();
    assertThat(schema.existsTrigger("audit8230")).isTrue();
    insertTarget("after-bad-file");
    assertThat(auditRows()).isEqualTo(1);
    assertThat(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8))
        .as("the file the operator has to fix is left as it was found").isEqualTo(broken);
  }

  private void insertTarget(final String name) {
    database.transaction(() -> database.newDocument(TARGET).set("name", name).save());
  }

  private long auditRows() {
    return database.countType(AUDIT, false);
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }

  private static final class FailingBucketFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new FailingBucket(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /** A real bucket whose schema hook fails: the load dies AFTER readConfiguration() has restored the members. */
  private static final class FailingBucket extends LocalBucket {
    private FailingBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      super(database, name, filePath, id, mode, pageSize, version);
    }

    @Override
    public void onAfterSchemaLoad() {
      throw new IllegalStateException("issue8230 deliberate failure inside the schema hook pass");
    }
  }
}
