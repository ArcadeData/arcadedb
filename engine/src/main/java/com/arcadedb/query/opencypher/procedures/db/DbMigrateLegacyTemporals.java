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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@code CALL db.migrateLegacyTemporals() YIELD records, properties}: the one-off conversion of a database written by a
 * release before the native temporal types (issue #9636).
 * <p>
 * Those releases persisted an openCypher {@code time()}, {@code localtime()}, zoned {@code datetime()} or
 * {@code duration()} on a property the schema does not declare as its ISO text, and restored it on read by its shape.
 * Such a database keeps that reading after the upgrade (see {@link TemporalUtil#restoreLegacyText}). This procedure
 * rewrites every such text in its native type - the very values a release with native types would have written - and
 * then marks the database native, which ends the reading: from then on a String is a String, as on a database created
 * with native types. On a database that is already native it converts nothing, since a String there was written as
 * one.
 * <p>
 * The text a record holds is converted exactly when those releases would have read it as a temporal: a top-level value
 * of an undeclared property with the shape of one. A declared property is left alone (a STRING holds text by contract,
 * a temporal one is converted by the deserializer already), and so are the elements of a list, which those releases
 * returned as text on a property read as well.
 * <p>
 * The records are rewritten in transactions of their own, {@link #BATCH_SIZE} at a time, and the database is marked
 * native only once all of them committed: a conversion that fails part way leaves it legacy, and running the procedure
 * again picks up what is left.
 * <p>
 * Run it with writers quiesced: a String written while it runs, with the shape of a temporal, on a property the schema
 * does not declare, is converted too - which is what the legacy reading already made of it. For the same reason a
 * database created by a 26.11.1 development build earlier than this procedure counts as legacy (its schema file has no
 * temporal storage setting either): its genuine Strings with a temporal shape on undeclared properties are read, and
 * converted, as temporals.
 */
public class DbMigrateLegacyTemporals implements CypherProcedure {
  public static final String NAME       = "db.migrateLegacyTemporals";
  public static final int    BATCH_SIZE = 1_000;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "Rewrites the openCypher temporals a release before the native temporal types stored as text into their native types, "
        + "then marks the database native";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("records", "properties");
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    // Ending the legacy reading is a database setting: refused up front, before a record is touched
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS);

    final LocalSchema schema = database.getSchema().getEmbedded();
    long records = 0;
    long properties = 0;
    if (schema.isLegacyTemporalText()) {
      // Streamed: the RIDs of at most one batch are held at a time. A record a batch rewrote holds no legacy text any
      // more, so meeting it again further on in the scan converts nothing twice
      final RID[] batch = new RID[BATCH_SIZE];
      int size = 0;
      for (final DocumentType type : schema.getTypes()) {
        if (type instanceof TimeSeriesType)
          continue;
        final Iterator<Record> iterator = database.iterateType(type.getName(), false);
        while (iterator.hasNext()) {
          final Record record = iterator.next();
          if (record instanceof Document document && hasLegacyText(document)) {
            batch[size++] = record.getIdentity();
            if (size == BATCH_SIZE) {
              properties += convertBatch(database, batch, size);
              records += size;
              size = 0;
            }
          }
        }
      }
      if (size > 0) {
        properties += convertBatch(database, batch, size);
        records += size;
      }

      // Only once every batch committed: a conversion that failed part way leaves the database legacy
      schema.markTemporalStorageNative();
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty("records", records);
    result.setProperty("properties", properties);
    return Stream.of(result);
  }

  /**
   * Rewrites one batch in a transaction of its own. Nested when the caller has one open (a {@code CALL} auto-opens one
   * for a write procedure, and the HTTP handlers wrap every command in one): a nested transaction commits on its own,
   * so a later rollback of the caller's transaction does not undo a batch that is already written.
   */
  private static long convertBatch(final DatabaseInternal database, final RID[] batch, final int size) {
    long converted = 0;
    database.begin();
    try {
      for (int i = 0; i < size; i++)
        converted += convert(batch[i]);
      database.commit();
    } catch (final RuntimeException e) {
      if (database.isTransactionActive())
        database.rollback();
      throw e;
    }
    return converted;
  }

  private static boolean hasLegacyText(final Document document) {
    for (final String name : document.getPropertyNames())
      if (document.get(name) instanceof String text && TemporalUtil.restoreLegacyText(document, name, text) instanceof CypherTemporalValue)
        return true;
    return false;
  }

  private static int convert(final RID rid) {
    final MutableDocument document = rid.asDocument(true).modify();
    int converted = 0;
    for (final String name : document.getPropertyNames()) {
      if (document.get(name) instanceof String text
          && TemporalUtil.restoreLegacyText(document, name, text) instanceof CypherTemporalValue temporal) {
        document.set(name, TemporalUtil.toCoreJavaType(temporal));
        ++converted;
      }
    }
    if (converted > 0)
      document.save();
    return converted;
  }
}
