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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A property read that an interrupt stops is not a damaged record. A parallel type scan stopped by its LIMIT interrupts its
 * producers, and every external property they were reading from disk failed with an {@link InterruptedIOException} that the
 * serializer logged as SEVERE "Possible corrupted record", with a stack trace, and answered as {@code null}. The interrupt now
 * reaches the reader, which is what cancels it, and nothing is logged.
 */
class InterruptedPropertyReadTest extends TestHelper {

  @Test
  void anInterruptedExternalReadFailsInsteadOfReadingNullAsACorruptedRecord() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DocumentType type = db.getSchema().createDocumentType("Doc");
    type.createProperty("e", Type.ARRAY_OF_FLOATS).setExternal(true);
    final float[] vector = new float[768];
    for (int i = 0; i < vector.length; i++)
      vector[i] = i / 768f;
    final RID[] rid = new RID[1];
    db.transaction(() -> rid[0] = db.newDocument("Doc").set("k", "a").set("e", vector).save().getIdentity());

    final PageManager pm = db.getPageManager();
    pm.waitAllPagesOfDatabaseAreFlushed(db);
    pm.removeAllReadPagesOfDatabase(db);
    // the record itself is read now; its external value stays on disk until get("e") asks for it
    final Document doc = rid[0].asDocument(true);
    pm.removeAllReadPagesOfDatabase(db);

    final List<String> severe = new CopyOnWriteArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        if (level.intValue() >= Level.SEVERE.intValue())
          severe.add(message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (level.intValue() >= Level.SEVERE.intValue())
          severe.add(message);
      }

      @Override
      public void flush() {
      }
    });

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> doc.get("e")).as("an interrupted read must not answer null")
          .hasRootCauseInstanceOf(InterruptedIOException.class);
    } finally {
      Thread.interrupted();
      LogManager.instance().setLogger(previous);
    }
    assertThat(severe).as("an interrupted read is not a corrupted record").isEmpty();

    // the record is intact: read again without the interrupt, the value is there
    assertThat(rid[0].asDocument(true).get("e")).isEqualTo(vector);
  }
}
