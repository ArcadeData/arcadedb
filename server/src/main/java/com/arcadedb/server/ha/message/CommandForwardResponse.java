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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import org.json.JSONObject;

import java.util.*;

public class CommandForwardResponse extends HAAbstractCommand {
  private ResultSet        resultset;
  private DatabaseInternal database;

  public CommandForwardResponse() {
  }

  public CommandForwardResponse(final DatabaseInternal database, final ResultSet resultset) {
    this.database = database;
    this.resultset = resultset;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(database.getName());
    while (resultset.hasNext()) {
      final Result next = resultset.next();

      stream.putByte((byte) 1); // ONE MORE RECORD

      if (next.isVertex()) {
        final VertexInternal v = (VertexInternal) next.getVertex().get();
        stream.putString(v.getIdentity().toString());
        stream.putBytes(database.getSerializer().serializeVertex(database, v).getContent());
      } else if (next.isEdge()) {
        final Edge e = next.getEdge().get();
        stream.putString(e.getIdentity().toString());
        stream.putBytes(database.getSerializer().serializeEdge(database, e).getContent());
      } else if (next.isElement()) {
        final Document d = next.getElement().get();
        stream.putString(d.getIdentity().toString());
        stream.putBytes(database.getSerializer().serializeDocument(database, d).getContent());
      } else {
        // PROJECTION
        stream.putString(""); // NO RID
        stream.putString(next.toJSON());
      }

    }
    stream.putByte((byte) 0); // NO MORE RECORDS
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    final String databaseName = stream.getString();

    database = (DatabaseInternal) server.getDatabase(databaseName);

    final Binary buffer = new Binary();

    final List<Result> list = new ArrayList<>();

    while (stream.getByte() == 1) {
      final String ridAsString = stream.getString();
      final RID rid = ridAsString.isEmpty() ? null : new RID(database, ridAsString);

      if (rid == null) {
        // PROJECTION
        final JSONObject json = new JSONObject(stream.getString());
        list.add(new ResultInternal(json.toMap()));

      } else {
        // RECORD
        buffer.clear();
        buffer.putByteArray(stream.getBytes());
        buffer.flip();

        final DocumentType t = database.getSchema().getTypeByBucketId(rid.getBucketId());
        final Record record = database.getRecordFactory().newImmutableRecord(database, t, rid, buffer.copy(), null);
        list.add(new ResultInternal(record));
      }
    }

    resultset = new IteratorResultSet(list.iterator());
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.receivedResponseFromForward(messageNumber, resultset, null);
    return null;
  }

  @Override
  public String toString() {
    return "command-forward-response";
  }
}
