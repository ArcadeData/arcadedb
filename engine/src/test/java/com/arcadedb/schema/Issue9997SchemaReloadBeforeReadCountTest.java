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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.trigger.JavaTrigger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9997: the database counts the BEFORE READ listeners of its own registry and of every type's, and a read skips
 * the type lookup while the count is zero. A schema reload in place replaces every type with a new instance and
 * re-attaches the triggers to the new ones: the retired instances must give their count back, or each reload would
 * leave the database one listener further from the fast path. A load that aborts must leave the count as it was: it
 * attaches no trigger to the types it staged (they are registered at publication, on the published types), the
 * published generation's listeners stay registered, and dropping the trigger afterwards still reaches zero.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9997SchemaReloadBeforeReadCountTest extends TestHelper {
  private static final String        TYPE  = "Reloaded9997";
  static final         AtomicInteger FIRED = new AtomicInteger();

  public static class CountingBeforeReadTrigger implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final Record record, final Record oldRecord) {
      return true;
    }

    @Override
    public boolean executeBeforeRead(final Database database, final RID rid) {
      FIRED.incrementAndGet();
      return true;
    }
  }

  @BeforeEach
  void resetTrigger() {
    FIRED.set(0);
  }

  @Test
  void aSchemaReloadInPlaceSucceededOrAbortedKeepsTheCountExact() throws IOException {
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    database.command("sql", "CREATE TRIGGER countReload BEFORE READ ON TYPE " + TYPE + " EXECUTE JAVA '"
        + CountingBeforeReadTrigger.class.getName() + "'");
    final LocalSchema schema = ((DatabaseInternal) database).getSchema().getEmbedded();
    final DocumentType before = schema.getType(TYPE);

    schema.load(ComponentFile.MODE.READ_WRITE, true);
    schema.load(ComponentFile.MODE.READ_WRITE, true);
    assertThat(schema.getType(TYPE)).as("the reload replaced the type instance").isNotSameAs(before);
    assertThat(scan()).isEqualTo(5);
    assertThat(FIRED.get()).as("one listener after two reloads").isEqualTo(5);

    schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new FailingBucketFactoryHandler());
    try {
      assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).hasMessageContaining("issue9997");
    } finally {
      schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
    }
    assertThat(anyBeforeReadListener()).isTrue();
    assertThat(scan()).isEqualTo(5);
    assertThat(FIRED.get()).isEqualTo(10);

    database.command("sql", "DROP TRIGGER countReload");
    assertThat(anyBeforeReadListener()).as("no retired or staged registry still counts").isFalse();
    assertThat(scan()).isEqualTo(5);
    assertThat(FIRED.get()).isEqualTo(10);
  }

  private boolean anyBeforeReadListener() {
    return ((RecordEventsRegistry) database.getEvents()).hasBeforeReadListenersInDatabase();
  }

  private int scan() {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + TYPE)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private static final class FailingBucketFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new FailingBucket(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /** A real bucket whose schema hook fails: the load dies after readConfiguration() staged the types. */
  private static final class FailingBucket extends LocalBucket {
    private FailingBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      super(database, name, filePath, id, mode, pageSize, version);
    }

    @Override
    public void onAfterSchemaLoad() {
      throw new IllegalStateException("issue9997 deliberate failure inside the schema hook pass");
    }
  }
}
