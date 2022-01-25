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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.ArrayFilters;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.QueryParameters;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.oplog.Oplog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoDBCollectionWrapper implements MongoCollection<Long> {
  private final Database database;
  private final int      collectionId;
  private final String   collectionName;

//  private static class ProjectingIterable implements Iterable<Document> {
//    private final Iterable<Document> iterable;
//    private final Document           fieldSelector;
//    private final String             idField;
//
//    ProjectingIterable(Iterable<Document> iterable, Document fieldSelector, String idField) {
//      this.iterable = iterable;
//      this.fieldSelector = fieldSelector;
//      this.idField = idField;
//    }
//
//    public Iterator<Document> iterator() {
//      return new ProjectingIterator(this.iterable.iterator(), this.fieldSelector, this.idField);
//    }
//  }
//
//  private static class ProjectingIterator implements Iterator<Document> {
//    private final Iterator<Document> iterator;
//    private final Document           fieldSelector;
//    private final String             idField;
//
//    ProjectingIterator(Iterator<Document> iterator, Document fieldSelector, String idField) {
//      this.iterator = iterator;
//      this.fieldSelector = fieldSelector;
//      this.idField = idField;
//    }
//
//    public boolean hasNext() {
//      return this.iterator.hasNext();
//    }
//
//    public Document next() {
//      Document document = this.iterator.next();
//      return MongoDBToSqlTranslator.projectDocument(document, this.fieldSelector, this.idField);
//    }
//
//    public void remove() {
//      this.iterator.remove();
//    }
//  }

  protected MongoDBCollectionWrapper(final Database database, final String collectionName) {
    this.database = database;
    this.collectionName = collectionName;
    this.collectionId = database.getSchema().getType(collectionName).getBuckets(false).get(0).getId();
  }

//  protected Document getDocument(final Long aLong) {
//    final com.arcadedb.database.Document record = (com.arcadedb.database.Document) database.lookupByRID(new RID(database, collectionId, aLong), true);
//
//    final Document result = new Document();
//
//    for (String p : record.getPropertyNames())
//      result.put(p, record.get(p));
//
//    return result;
//  }

  @Override
  public MongoDatabase getDatabase() {
    return null;
  }

  @Override
  public String getDatabaseName() {
    return database.getName();
  }

  @Override
  public String getFullName() {
    return null;
  }

  @Override
  public String getCollectionName() {
    return collectionName;
  }

  @Override
  public void addIndex(Index<Long> index) {

  }

  @Override
  public void dropIndex(String s) {

  }

  @Override
  public void renameTo(MongoDatabase mongoDatabase, String s) {
  }

  @Override
  public void addDocument(Document document) {

  }

  @Override
  public void removeDocument(Document document) {

  }

  @Override
  public QueryResult handleQuery(QueryParameters queryParameters) {
    int numberToReturn = queryParameters.getLimit();
    if (numberToReturn < 0)
      numberToReturn = -numberToReturn;

    final int numberToSkip = queryParameters.getNumberToSkip();

    final Document queryObject = queryParameters.getQuerySelector();

    Document query;
    Document orderBy;
    if (queryObject.containsKey("query")) {
      query = (Document) queryObject.get("query");
    } else if (queryObject.containsKey("$query")) {
      query = (Document) queryObject.get("$query");
    } else {
      query = queryObject;
    }

    orderBy = (Document) queryObject.remove("$orderBy");

    if (this.count() == 0)
      return new QueryResult();

    final Iterable<Document> objs = this.queryDocuments(query, orderBy, numberToSkip, numberToReturn);

    return new QueryResult(objs);
  }

  @Override
  public List<Document> insertDocuments(final List<Document> list) {
    database.begin();

    for (Document d : list) {
      final MutableDocument record = database.newDocument(collectionName);

      for (Map.Entry<String, Object> p : d.entrySet()) {
        final Object value = p.getValue();
        if (value instanceof ObjectId) {
          final byte[] var2 = ((ObjectId) value).toByteArray();
          int var3 = var2.length;

          final StringBuilder s = new StringBuilder();
          for (int var4 = 0; var4 < var3; ++var4) {
            byte b = var2[var4];
            s.append(String.format("%02x", b));
          }

          record.set(p.getKey(), s.toString());
        } else
          record.set(p.getKey(), value);
      }

      record.save();
    }

    database.commit();

    return list;
  }

  @Override
  public List<Document> insertDocuments(List<Document> list, boolean b) {
    return null;
  }

  @Override
  public Document updateDocuments(final Document document, final Document document1, final ArrayFilters filters, final boolean b, final boolean b1,
      final Oplog opLog) {
    return null;
  }

  @Override
  public int deleteDocuments(final Document document, final int limit) {
    return 0;
  }

  @Override
  public int deleteDocuments(Document document, int i, Oplog oplog) {
    return 0;
  }

  @Override
  public Document handleDistinct(Document document) {
    return null;
  }

  @Override
  public Document getStats() {
    return null;
  }

  @Override
  public Document validate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Document findAndModify(Document document) {
    return null;
  }

  @Override
  public int count(Document document, int i, int i1) {
    return (int) database.countType(collectionName, false);
  }

  @Override
  public int count() {
    return (int) database.countType(getCollectionName(), false);
  }

  @Override
  public int getNumIndexes() {
    return 0;
  }

  @Override
  public List<Index<Long>> getIndexes() {
    return null;
  }

  @Override
  public void drop() {
    database.getSchema().dropType(collectionName);
  }

  private Iterable<Document> queryDocuments(Document query, final Document orderBy, final int numberToSkip, final int numberToReturn) {
    final List<Document> result = new ArrayList<>();

    Iterator it;

    if (query.isEmpty()) {
      // SCAN
      it = database.iterateType(collectionName, false);
    } else {
      // EXECUTE A SQL QUERY
      final StringBuilder sql = new StringBuilder("select from " + collectionName + " where ");

      MongoDBToSqlTranslator.buildExpression(sql, query);

      if (orderBy != null) {
        sql.append(" order by ");
        int i = 0;
        for (String p : orderBy.keySet()) {
          if (i > 0)
            sql.append(", ");
          sql.append(p);
          sql.append(' ');
          sql.append(((Number) orderBy.get(p)).intValue() == 1 ? "asc" : "desc");
          ++i;
        }
      }

      it = database.query("SQL", sql.toString());
    }

    MongoDBToSqlTranslator.fillResultSet(numberToSkip, numberToReturn, result, it);

    return result;
  }
}
