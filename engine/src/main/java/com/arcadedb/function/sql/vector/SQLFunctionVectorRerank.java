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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Declarative multi-stage retrieval (Qdrant-style "prefetch + rerank"). Takes a stage-1
 * candidate set - typically the output of a cheap-but-coarse search like
 * {@code vector.neighbors} on a quantized-vector index - and re-scores each candidate against a
 * {@code query} vector using cosine similarity over a freshly-read property
 * ({@code embeddingProperty}). The resulting top-{@code k} respects the new ranking.
 * <p>
 * The point: stage 1 is allowed to use fast lossy primitives (PQ/binary/SQ/sparse with high
 * over-fetch) because the rerank stage compares against full-precision vectors and corrects
 * the ranking. Server-side and declarative, so the application no longer round-trips between
 * stages.
 * <p>
 * Usage:
 * <pre>
 *   SELECT expand(`vector.rerank`(
 *       `vector.neighbors`('Doc[binary_embedding]', :q_binary, 1000),
 *       :q_full,           -- full-precision query vector
 *       'embedding',       -- full-precision property to read off each record
 *       100))
 * </pre>
 * Records that disappeared between stage 1 and rerank (deleted, schema removed) are silently
 * dropped - the stage-1 candidate list is taken at face value but each row gets re-validated
 * here, so a transient inconsistency cannot crash the query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorRerank extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.rerank";

  public SQLFunctionVectorRerank() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 4)
      throw new CommandSQLParsingException(getSyntax());

    final Object source = params[0];
    final Object queryRaw = params[1];
    if (queryRaw == null)
      throw new CommandSQLParsingException(NAME + " query vector cannot be null");
    final float[] queryVector = toFloatArray(queryRaw);

    if (!(params[2] instanceof String embeddingProperty) || embeddingProperty.isEmpty())
      throw new CommandSQLParsingException(NAME + " 3rd argument must be the embedding property name");

    final int k = params[3] instanceof Number n ? n.intValue() : Integer.parseInt(params[3].toString());
    if (k <= 0)
      return new ArrayList<>(0);

    if (!(source instanceof Iterable<?> iter))
      throw new CommandSQLParsingException(NAME + " source must be an iterable of rows, got: "
          + (source == null ? "null" : source.getClass().getSimpleName()));

    final BasicDatabase db = context.getDatabase();
    final ArrayList<Scored> rescored = new ArrayList<>();
    final int expectedDim = queryVector.length;
    for (final Object row : iter) {
      final RID rid = extractRidFromRow(row);
      if (rid == null)
        continue;
      final Document rec;
      try {
        rec = (Document) db.lookupByRID(rid, true);
      } catch (final RecordNotFoundException e) {
        continue;
      }
      final Object raw = rec.get(embeddingProperty);
      if (raw == null)
        continue;
      final float[] candVec;
      try {
        candVec = toFloatArray(raw);
      } catch (final RuntimeException ignored) {
        continue;
      }
      if (candVec.length != expectedDim)
        throw new CommandSQLParsingException(NAME + " candidate " + rid
            + " has embedding dimension " + candVec.length + ", expected " + expectedDim
            + " (matching the query vector)");

      final float similarity = VectorUtils.cosineSimilarity(queryVector, candVec);
      // The {@code rid} is recoverable from {@code rec.getIdentity()} so it does not need to be
      // stored on the Scored record - dropping it keeps the data class minimal.
      rescored.add(new Scored(rec, similarity));
    }

    rescored.sort((a, b) -> Float.compare(b.score(), a.score()));

    final int targetSize = Math.min(k, rescored.size());
    final ArrayList<Object> out = new ArrayList<>(targetSize);
    for (int i = 0; i < targetSize; i++) {
      final Scored s = rescored.get(i);
      final LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
      entry.put("record", s.record());
      for (final String prop : s.record().getPropertyNames())
        entry.put(prop, s.record().get(prop));
      entry.put("@rid", s.record().getIdentity());
      entry.put("@type", s.record().getTypeName());
      entry.put("score", s.score());
      out.add(entry);
    }
    return out;
  }

  public String getSyntax() {
    return NAME + "(<source>, <queryVector>, <embeddingProperty>, <k>)";
  }

  private record Scored(Document record, float score) {}
}
