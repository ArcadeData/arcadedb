/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.stresstest.workload;

import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.stresstest.DatabaseIdentifier;
import com.arcadedb.stresstest.StressTesterSettings;
import org.json.JSONObject;

import java.util.Locale;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class OCRUDWorkload extends BaseDocumentWorkload implements CheckWorkload {

  public static final String USERTYPE_NAME = "StressTestCRUD";
  public static final String INDEX_NAME    = USERTYPE_NAME + ".Index";

  static final String INVALID_FORM_MESSAGE = "CRUD workload must be in form of CxIxUxDxSx where x is a valid number.";
  static final String INVALID_NUMBERS      = "Reads, Updates and Deletes must be less or equals to the Creates";

  private int total = 0;

  private OWorkLoadResult createsResult = new OWorkLoadResult();
  private OWorkLoadResult readsResult   = new OWorkLoadResult();
  private OWorkLoadResult updatesResult = new OWorkLoadResult();
  private OWorkLoadResult deletesResult = new OWorkLoadResult();
  private OWorkLoadResult scansResult   = new OWorkLoadResult();
  private int             creates;
  private int             reads;
  private int             updates;
  private int             deletes;
  private int             scans;

  public OCRUDWorkload() {
    connectionStrategy = RemoteDatabase.CONNECTION_STRATEGY.ROUND_ROBIN;
  }

  @Override
  public String getName() {
    return "CRUD";
  }

  @Override
  public void parseParameters(final String args) {
    final String ops = args.toUpperCase(Locale.ENGLISH);
    char state = ' ';
    final StringBuilder number = new StringBuilder();

    for (int pos = 0; pos < ops.length(); ++pos) {
      final char c = ops.charAt(pos);

      if (c == 'C' || c == 'R' || c == 'U' || c == 'D' || c == 'S') {
        state = assignState(state, number, c);
      } else if (c >= '0' && c <= '9')
        number.append(c);
      else
        throw new IllegalArgumentException("Character '" + c + "' is not valid on CRUD workload. " + INVALID_FORM_MESSAGE);
    }
    assignState(state, number, ' ');

    total = creates + reads + updates + deletes + scans;

    if (reads > creates || updates > creates || deletes > creates)
      throw new IllegalArgumentException(INVALID_NUMBERS);

    if (total == 0)
      throw new IllegalArgumentException(INVALID_FORM_MESSAGE);

    createsResult.total = creates;
    readsResult.total = reads;
    updatesResult.total = updates;
    deletesResult.total = deletes;
    scansResult.total = scans;
  }

  @Override
  public void execute(final StressTesterSettings settings, final DatabaseIdentifier databaseIdentifier) {
    createSchema(databaseIdentifier);
    connectionStrategy = settings.loadBalancing;

    // PREALLOCATE THE LIST TO AVOID CONCURRENCY ISSUES
    final RID[] records = new RID[createsResult.total];

    executeOperation(databaseIdentifier, createsResult, settings, context -> {
      final Document doc = createOperation(context.currentIdx);
      records[context.currentIdx] = doc.getIdentity();
      createsResult.current.incrementAndGet();
      return null;
    });

    if (records.length != createsResult.total)
      throw new RuntimeException("Error on creating records: found " + records.length + " but expected " + createsResult.total);

    executeOperation(databaseIdentifier, scansResult, settings, context -> {
      scanOperation(((OWorkLoadContext) context).getDb());
      scansResult.current.incrementAndGet();
      return null;
    });

    executeOperation(databaseIdentifier, readsResult, settings, context -> {
      readOperation(((OWorkLoadContext) context).getDb(), context.currentIdx);
      readsResult.current.incrementAndGet();
      return null;
    });

    executeOperation(databaseIdentifier, updatesResult, settings, context -> {
      updateOperation(((OWorkLoadContext) context).getDb(), records[context.currentIdx]);
      updatesResult.current.incrementAndGet();
      return null;
    });

    executeOperation(databaseIdentifier, deletesResult, settings, context -> {
      deleteOperation(((OWorkLoadContext) context).getDb(), records[context.currentIdx]);
      records[context.currentIdx] = null;
      deletesResult.current.incrementAndGet();
      return null;
    });
  }

  protected void createSchema(final DatabaseIdentifier databaseIdentifier) {
    final Database database = databaseIdentifier.getEmbeddedDatabase();
    try {
      final Schema schema = database.getSchema();
      if (!schema.existsType(OCRUDWorkload.USERTYPE_NAME)) {
        final DocumentType cls = schema.createDocumentType(OCRUDWorkload.USERTYPE_NAME);
        cls.createProperty("name", String.class);
        schema.createTypeIndex(EmbeddedSchema.INDEX_TYPE.LSM_TREE, true, OCRUDWorkload.USERTYPE_NAME, new String[] { "name" });
      }
    } finally {
      database.close();
    }
  }

  @Override
  public String getPartialResult() {
    final long current =
        createsResult.current.get() + scansResult.current.get() + readsResult.current.get() + updatesResult.current.get() + deletesResult.current.get();

    return String.format("%d%% [Creates: %d%% - Scans: %d%% - Reads: %d%% - Updates: %d%% - Deletes: %d%%]", ((int) (100 * current / total)),
        createsResult.total > 0 ? 100 * createsResult.current.get() / createsResult.total : 0,
        scansResult.total > 0 ? 100 * scansResult.current.get() / scansResult.total : 0,
        readsResult.total > 0 ? 100 * readsResult.current.get() / readsResult.total : 0,
        updatesResult.total > 0 ? 100 * updatesResult.current.get() / updatesResult.total : 0,
        deletesResult.total > 0 ? 100 * deletesResult.current.get() / deletesResult.total : 0);
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("- Created %d records in %.3f secs%s", createsResult.total, (createsResult.totalTime / 1000f), createsResult.toOutput(1)));

    buffer.append(String.format("\n- Scanned %d records in %.3f secs%s", scansResult.total, (scansResult.totalTime / 1000f), scansResult.toOutput(1)));

    buffer.append(String.format("\n- Read %d records in %.3f secs%s", readsResult.total, (readsResult.totalTime / 1000f), readsResult.toOutput(1)));

    buffer.append(String.format("\n- Updated %d records in %.3f secs%s", updatesResult.total, (updatesResult.totalTime / 1000f), updatesResult.toOutput(1)));

    buffer.append(String.format("\n- Deleted %d records in %.3f secs%s", deletesResult.total, (deletesResult.totalTime / 1000f), deletesResult.toOutput(1)));

    return buffer.toString();
  }

  @Override
  public JSONObject getFinalResultAsJson() {
    final JSONObject json = new JSONObject();

    json.put("type", getName());

    json.put("creates", createsResult.toJSON());
    json.put("scans", scansResult.toJSON());
    json.put("reads", readsResult.toJSON());
    json.put("updates", updatesResult.toJSON());
    json.put("deletes", deletesResult.toJSON());

    return json;
  }

  public Document createOperation(final long n) {
    final MutableDocument doc = ((OWorkLoadContext) getContext()).getDb().newDocument(USERTYPE_NAME);
    doc.set("name", "value" + n);
    doc.save();
    return doc;
  }

  public void readOperation(final Database database, final long n) {
    final String query = String.format("SELECT FROM %s WHERE name = ?", USERTYPE_NAME);
    final ResultSet result = database.command("SQL", query, "value" + n);
    final long tot = result.countEntries();
    if (tot != 1) {
      throw new RuntimeException(String.format("The query [%s] result size is %d. Expected size is 1.", query, tot));
    }
  }

  public void scanOperation(final Database database) {
    final String query = String.format("SELECT count(*) FROM %s WHERE notexistent is null", USERTYPE_NAME);
    final ResultSet result = database.command("SQL", query);
    final long tot = result.countEntries();
    if (tot != 1) {
      throw new RuntimeException(String.format("The query [%s] result size is %d. Expected size is 1.", query, tot));
    }
  }

  public void updateOperation(final Database database, final Identifiable rec) {
    final MutableDocument doc = rec.asDocument().modify();
    doc.set("updated", true);
    doc.save();
  }

  public void deleteOperation(final Database database, final Identifiable rec) {
    database.deleteRecord(rec.getRecord());
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'C')
      creates = Integer.parseInt(number.toString());
    else if (state == 'R')
      reads = Integer.parseInt(number.toString());
    else if (state == 'U')
      updates = Integer.parseInt(number.toString());
    else if (state == 'D')
      deletes = Integer.parseInt(number.toString());
    else if (state == 'S')
      scans = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  public int getCreates() {
    return createsResult.total;
  }

  public int getReads() {
    return readsResult.total;
  }

  public int getScans() {
    return scansResult.total;
  }

  public int getUpdates() {
    return updatesResult.total;
  }

  public int getDeletes() {
    return deletesResult.total;
  }

  @Override
  public void check(DatabaseIdentifier databaseIdentifier) {

  }
}
