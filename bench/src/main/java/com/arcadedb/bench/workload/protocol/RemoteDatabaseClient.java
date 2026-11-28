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
package com.arcadedb.bench.workload.protocol;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The openCypher client over a {@link RemoteDatabase}, which the HTTP and the gRPC drivers both are.
 */
abstract class RemoteDatabaseClient implements ProtocolClient {
  protected final RemoteDatabase db;

  RemoteDatabaseClient(final RemoteDatabase db) {
    this.db = db;
  }

  @Override
  public List<Map<String, Object>> query(final String cypher, final Map<String, Object> params) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = db.query(LANGUAGE, cypher, params != null ? params : Map.of())) {
      while (rs.hasNext())
        rows.add(row(rs.next()));
    }
    return rows;
  }

  @Override
  public void transaction(final List<String> cyphers, final List<Map<String, Object>> params) {
    db.begin();
    try {
      for (int i = 0; i < cyphers.size(); i++)
        try (final ResultSet rs = db.command(LANGUAGE, cyphers.get(i), ProtocolClient.paramsOf(params, i))) {
          // drained by the close: a statement's rows are not part of the transaction's answer
        }
      db.commit();
    } catch (final RuntimeException e) {
      try {
        db.rollback();
      } catch (final RuntimeException rollback) {
        e.addSuppressed(rollback);
      }
      throw e;
    }
  }

  private static Map<String, Object> row(final Result result) {
    final Map<String, Object> row = new LinkedHashMap<>();
    for (final String name : result.getPropertyNames())
      row.put(name, result.getProperty(name));
    return row;
  }
}
