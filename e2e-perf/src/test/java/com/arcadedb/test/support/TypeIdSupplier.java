package com.arcadedb.test.support;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.Iterator;
import java.util.List;

public class TypeIdSupplier implements java.util.function.Supplier<Integer> {
  /**
   * This class is a supplier for user IDs.
   * It fetches user IDs from the database in batches.
   * It uses an iterator to provide the next user ID when requested.
   */

  private final RemoteDatabase    db;
  private final String            query;
  private final int               batchSize;
  private       Iterator<Integer> idsIt;
  private       int               skip;

  public TypeIdSupplier(RemoteDatabase db, String type) {
    this.db = db;
    query = String.format("SELECT id FROM %s ORDER BY id SKIP ? LIMIT ?", type);
    skip = 0;
    batchSize = 100;
  }

  @Override
  public Integer get() {
    if (idsIt == null || !idsIt.hasNext()) {
      fetchNextBatch();
    }
    if (idsIt.hasNext()) {
      return idsIt.next();
    } else {
      return null; // No more available
    }
  }

  private void fetchNextBatch() {
    ResultSet resultSet = db.query("sql", query, skip, batchSize);
    List<Integer> ids = resultSet.stream()
        .map(r -> r.<Integer>getProperty("id"))
        .toList();
    idsIt = ids.iterator();
    if (!ids.isEmpty()) {
      skip += ids.size(); // Update skip for the next batch
    }

  }
}
