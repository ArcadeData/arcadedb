/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arcadedb.lucene.index; // Changed package

import com.arcadedb.database.Identifiable; // Changed import
import com.arcadedb.database.RID; // Changed import
import com.arcadedb.document.Document; // ArcadeDB Document
import com.arcadedb.exception.ArcadeDBException; // Changed import
import com.arcadedb.index.CompositeKey; // Changed import
import com.arcadedb.index.IndexDefinition; // Changed import
import com.arcadedb.schema.Type; // Changed import
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.document.Field; // Lucene Document Field
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/** Created by enricorisa on 21/03/14. */
public class ArcadeLuceneIndexType { // Changed class name
  public static final String RID_HASH = "_RID_HASH";
  public static final String RID = "_RID"; // Defined locally
  public static final String KEY = "_KEY"; // Defined locally

  public static Field createField( // Simplified, assuming store is passed correctly by caller for specific needs
      final String fieldName, final Object value, final Field.Store store) {
    if (fieldName.startsWith("_CLASS") || fieldName.startsWith("_CLUSTER")) {
      return new StringField(fieldName, value.toString(), store);
    }
    // Defaulting to TextField, assuming analysis. Use StringField if non-analyzed is the default.
    return new TextField(fieldName, value.toString(), store);
  }

  public static String extractId(org.apache.lucene.document.Document doc) { // Lucene Document
    String value = doc.get(RID_HASH);
    if (value != null) {
      int pos = value.indexOf("|");
      if (pos > 0) {
        return value.substring(0, pos);
      } else {
        return value;
      }
    } else {
      return null;
    }
  }

  public static Field createIdField(final Identifiable id, final Object key) { // Changed Identifiable
    return new StringField(RID_HASH, genValueId(id, key), Field.Store.YES);
  }

  public static Field createRidField(final Identifiable id) { // Renamed from createOldIdField, Changed Identifiable
    return new StringField(RID, id.getIdentity().toString(), Field.Store.YES);
  }

  public static String genValueId(final Identifiable id, final Object key) { // Changed Identifiable
    String value = id.getIdentity().toString() + "|";
    value += hashKey(key);
    return value;
  }

  public static List<Field> createFields(
      String fieldName, Object value, Field.Store store, Boolean sort, Type type) { // Added Type parameter
    List<Field> luceneFields = new ArrayList<>();

    if (value instanceof Number) {
      Number number = (Number) value;
      if (type == Type.LONG || value instanceof Long) {
        luceneFields.add(new LongPoint(fieldName, number.longValue()));
        luceneFields.add(new NumericDocValuesField(fieldName, number.longValue())); // For sorting/faceting
        if (store == Field.Store.YES) luceneFields.add(new StoredField(fieldName, number.longValue()));
      } else if (type == Type.FLOAT || value instanceof Float) {
        luceneFields.add(new FloatPoint(fieldName, number.floatValue()));
        luceneFields.add(new FloatDocValuesField(fieldName, number.floatValue())); // For sorting/faceting
        if (store == Field.Store.YES) luceneFields.add(new StoredField(fieldName, number.floatValue()));
      } else if (type == Type.DOUBLE || value instanceof Double) {
        luceneFields.add(new DoublePoint(fieldName, number.doubleValue()));
        luceneFields.add(new DoubleDocValuesField(fieldName, number.doubleValue())); // For sorting/faceting
        if (store == Field.Store.YES) luceneFields.add(new StoredField(fieldName, number.doubleValue()));
      } else { // INTEGER, SHORT, BYTE
        luceneFields.add(new IntPoint(fieldName, number.intValue()));
        luceneFields.add(new NumericDocValuesField(fieldName, number.longValue())); // Use long for DV for all integer types
        if (store == Field.Store.YES) luceneFields.add(new StoredField(fieldName, number.intValue()));
      }
      // Optionally, add the original value as a TextField if it needs to be searchable as text
      // luceneFields.add(new TextField(fieldName, value.toString(), store));
    } else if (type == Type.DATETIME || type == Type.DATE || value instanceof Date) {
      long time = (value instanceof Date) ? ((Date) value).getTime() : Long.parseLong(value.toString());
      luceneFields.add(new LongPoint(fieldName, time));
      luceneFields.add(new NumericDocValuesField(fieldName, time)); // For sorting/faceting
      if (store == Field.Store.YES) luceneFields.add(new StoredField(fieldName, time));
      // Optionally, add the original value as a TextField
      // luceneFields.add(new TextField(fieldName, value.toString(), store));
    } else if (type == Type.STRING || value instanceof String) {
      String stringValue = value.toString();
      luceneFields.add(new TextField(fieldName, stringValue, store)); // Analyzed
      // Or use StringField for non-analyzed:
      // luceneFields.add(new StringField(fieldName, stringValue, store));
      if (Boolean.TRUE.equals(sort)) {
        luceneFields.add(new SortedDocValuesField(fieldName, new BytesRef(stringValue)));
      }
    } else {
      // Default to TextField for other types or if type is null
      luceneFields.add(new TextField(fieldName, value.toString(), store));
      if (Boolean.TRUE.equals(sort)) {
         luceneFields.add(new SortedDocValuesField(fieldName, new BytesRef(value.toString())));
      }
    }
    return luceneFields;
  }

  public static Query createExactQuery(IndexDefinition index, Object key) { // Changed OIndexDefinition
    Query query = null;
    if (key instanceof String) {
      final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      if (index.getFields().size() > 0) {
        for (String idx : index.getFields()) {
          queryBuilder.add(new TermQuery(new Term(idx, key.toString())), BooleanClause.Occur.SHOULD);
        }
      } else {
        queryBuilder.add(new TermQuery(new Term(KEY, key.toString())), BooleanClause.Occur.SHOULD);
      }
      query = queryBuilder.build();
    } else if (key instanceof CompositeKey) { // Changed OCompositeKey
      final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      int i = 0;
      CompositeKey keys = (CompositeKey) key;
      for (String idx : index.getFields()) {
        String val = (String) keys.getKeys().get(i); // Assuming keys are strings
        queryBuilder.add(new TermQuery(new Term(idx, val)), BooleanClause.Occur.MUST);
        i++;
      }
      query = queryBuilder.build();
    }
    return query;
  }

  public static Query createQueryId(Identifiable value) { // Changed OIdentifiable
    return new TermQuery(new Term(RID, value.getIdentity().toString()));
  }

  public static Query createQueryId(Identifiable value, Object key) { // Changed OIdentifiable
    return new TermQuery(new Term(RID_HASH, genValueId(value, key)));
  }

  public static String hashKey(Object key) {
    try {
      String keyString;
      if (key instanceof Document) { // Changed ODocument to ArcadeDB Document
        keyString = ((Document) key).toJSON().toString(); // Assuming toJSON returns JSON object
      } else {
        keyString = key.toString();
      }
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] bytes = sha256.digest(keyString.getBytes("UTF-8"));
      return Base64.getEncoder().encodeToString(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw ArcadeDBException.wrapException(new ArcadeDBException("fail to find sha algorithm"), e); // Changed exception
    } catch (UnsupportedEncodingException e) {
      throw ArcadeDBException.wrapException(new ArcadeDBException("fail to find utf-8 encoding"), e); // Changed exception
    }
  }

  public static Query createDeleteQuery( // Changed OIdentifiable, ODocument
      Identifiable value, List<String> fields, Object key, com.arcadedb.document.Document metadata) {

    // TODO Implementation of Composite keys with Collection
    final BooleanQuery.Builder filter = new BooleanQuery.Builder();
    final BooleanQuery.Builder builder = new BooleanQuery.Builder();
    // TODO: Condition on Id and field key only for backward compatibility
    if (value != null) {
      builder.add(createQueryId(value), BooleanClause.Occur.MUST);
    }
    String field = fields.iterator().next();
    builder.add(
        new TermQuery(new Term(field, key.toString().toLowerCase(Locale.ENGLISH))),
        BooleanClause.Occur.MUST);

    filter.add(builder.build(), BooleanClause.Occur.SHOULD);
    if (value != null) {
      filter.add(createQueryId(value, key), BooleanClause.Occur.SHOULD);
    }
    return filter.build();
  }
}
