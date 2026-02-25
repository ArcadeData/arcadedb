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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.*;
import com.arcadedb.database.Record;
import com.arcadedb.schema.Property;

import java.util.*;
import java.util.stream.Collectors;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * Implementation of a {@link Result}.
 * <p>
 * Created by luigidellaquila on 06/07/16.
 */
public class ResultInternal implements Result {
  protected final Database database;
  protected final Object value;
  protected Map<String, Object> content;
  protected Map<String, Object> temporaryContent;
  protected Map<String, Object> metadata;
  protected Document element;
  protected float score = 0f;
  protected float similarity = 0f;

  public ResultInternal() {
    // Memory optimization: Use smaller initial capacity to reduce memory footprint
    // Default LinkedHashMap capacity is 16, but most projections have < 10 fields
    this.content = new LinkedHashMap<>(8);
    this.database = null;
    this.value = null;
  }

  public ResultInternal(final Map<String, Object> map) {
    this.content = map;
    this.database = null;
    this.value = null;
  }

  public ResultInternal(final Identifiable indent) {
    this.element = (Document) indent.getRecord();
    this.database = null;
    this.value = null;
  }

  public ResultInternal(final Database database) {
    // Memory optimization: Use smaller initial capacity (most projections have < 10 fields)
    this.content = new LinkedHashMap<>(8);
    this.database = database;
    this.value = null;
  }

  public ResultInternal(final Object value) {
    this.value = value;
    this.database = null;
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  /**
   * Returns the raw value stored in this Result, if any.
   * This is used when the Result was created to wrap a plain value (like a String)
   * rather than a Document or Map.
   */
  public Object getValue() {
    return value;
  }

  public void setTemporaryProperty(final String name, Object value) {
    if (temporaryContent == null)
      temporaryContent = new HashMap<>();

    if (value instanceof Optional optional)
      value = optional.orElse(null);

    if (value instanceof Result result && result.isElement())
      temporaryContent.put(name, result.getElement().get());
    else
      temporaryContent.put(name, value);
  }

  public Object getTemporaryProperty(final String name) {
    if (name == null || temporaryContent == null)
      return null;
    return temporaryContent.get(name);
  }

  public Set<String> getTemporaryProperties() {
    return temporaryContent == null ? Collections.emptySet() : temporaryContent.keySet();
  }

  /**
   * Gets the search score for this result.
   * Used for full-text search relevance scoring.
   *
   * @return the score, or 0 if not set
   */
  public float getScore() {
    return score;
  }

  /**
   * Sets the search score for this result.
   * Used for full-text search relevance scoring.
   *
   * @param score the relevance score
   */
  public void setScore(final float score) {
    this.score = score;
  }

  /**
   * Gets the similarity score for this result.
   * Used for More Like This (MLT) queries to indicate similarity to a reference document.
   * The similarity is normalized to 0.0-1.0 where 1.0 means most similar.
   *
   * @return the similarity score, or 0 if not set
   */
  public float getSimilarity() {
    return similarity;
  }

  /**
   * Sets the similarity score for this result (normalized 0.0 to 1.0).
   * Used for SEARCH_INDEX_MORE/SEARCH_FIELDS_MORE functions.
   * Note: No validation is performed. Callers are responsible for ensuring valid range.
   *
   * @param similarity the normalized similarity score (should be 0.0-1.0)
   */
  public void setSimilarity(final float similarity) {
    this.similarity = similarity;
  }

  public ResultInternal setProperty(final String name, Object value) {
    if (value instanceof Optional optional)
      value = optional.orElse(null);

    if (content == null)
      throw new IllegalStateException("Impossible to mutate result set");

    if (value instanceof Result result && result.isElement())
      content.put(name, result.getElement().get());
    else
      content.put(name, value);

    return this;
  }

  public void removeProperty(final String name) {
    if (content != null)
      content.remove(name);
  }

  public <T> T getProperty(final String name) {
    T result;
    if (content != null && !content.isEmpty())
      // IF CONTENT IS PRESENT SKIP CHECKING FOR ELEMENT (PROJECTIONS USED)
      result = (T) content.get(name);
    else if (element != null)
      result = (T) element.get(name);
    else
      result = null;

    // If $score not found in content/element, fall back to score field
    if (result == null && "$score".equals(name))
      return (T) Float.valueOf(score);

    // If $similarity not found in content/element, fall back to similarity field
    if (result == null && "$similarity".equals(name))
      return (T) Float.valueOf(similarity);

    if (!(result instanceof Record) &&
            result instanceof Identifiable identifiable &&
            identifiable.getIdentity() != null)
      result = (T) identifiable.getIdentity();

    return result;
  }

  public <T> T getProperty(final String name, final Object defaultValue) {
    T result;
    if (content != null && content.containsKey(name))
      result = (T) content.get(name);
    else if (element != null && element.has(name))
      result = (T) element.get(name);
    else
      result = (T) defaultValue;

    if (!(result instanceof Record) && result instanceof Identifiable identifiable && identifiable.getIdentity() != null)
      result = (T) identifiable.getIdentity();
    return result;
  }

  @Override
  public Record getElementProperty(final String name) {
    Object result = null;
    if (content != null && content.containsKey(name))
      result = content.get(name);
    else if (element != null)
      result = element.get(name);

    if (result instanceof Result result1)
      result = result1.getRecord().orElse(null);

    if (result instanceof RID iD)
      result = iD.getRecord();

    return result instanceof Record r ? r : null;
  }

  /**
   * Returns the input object transformed into a map when the value is a record. it works recursively.
   * This method was originally used in OrientDB because all the record were immutable. With ArcadeDB all the
   * records returned by a query are always immutable, so this method is not called automatically from the `getProperty()`.
   * <p>
   * From v24.5.1 this method is public to allow to be called for retro compatibility with existent code base or porting
   * from OrientDB.
   */
  public static Object wrap(final Object input) {
    if (input instanceof Document document && document.getIdentity() == null && !(input instanceof EmbeddedDocument)) {
      final Document elem = document;
      final ResultInternal result = new ResultInternal(elem.toMap(false));
      if (elem.getTypeName() != null)
        result.setProperty(Property.TYPE_PROPERTY, elem.getTypeName());
      return result;

    } else if (input instanceof List list) {
      return list.stream().map(ResultInternal::wrap).collect(Collectors.toList());
    } else if (input instanceof Set set) {
      return set.stream().map(ResultInternal::wrap).collect(Collectors.toSet());
    } else if (input instanceof Map) {
      final Map result = new HashMap();
      for (final Map.Entry<String, Object> o : ((Map<String, Object>) input).entrySet())
        result.put(o.getKey(), wrap(o.getValue()));
      return result;
    }
    return input;
  }

  public Set<String> getPropertyNames() {
    final Set<String> result = new LinkedHashSet<>();

    // Include $score in property names if score is set
    if (score > 0)
      result.add("$score");

    // Include $similarity in property names if similarity is set
    if (similarity > 0)
      result.add("$similarity");

    if (element != null)
      result.addAll(element.getPropertyNames());

    if (content != null)
      result.addAll(content.keySet());
    return result;
  }

  public boolean hasProperty(final String propName) {
    // $score is always available as a special property
    if ("$score".equals(propName))
      return true;

    // $similarity is always available as a special property
    if ("$similarity".equals(propName))
      return true;

    if (element != null && element.has(propName))
      return true;

    return content != null && content.containsKey(propName);
  }

  @Override
  public boolean isElement() {
    return this.element != null;
  }

  public Optional<Document> getElement() {
    return Optional.ofNullable(element);
  }

  @Override
  public Map<String, Object> toMap() {
    return element != null ? element.toMap() : content;
  }

  @Override
  public Document toElement() {
    if (isElement())
      return getElement().get();

    return null;
  }

  @Override
  public Optional<RID> getIdentity() {
    if (element != null)
      return Optional.of(element.getIdentity());

    if (hasProperty(RID_PROPERTY)) {
      final Object rid = getProperty(RID_PROPERTY);
      if (rid == null)
        return Optional.empty();
      return Optional.of((RID) (rid instanceof RID ? rid : new RID(rid.toString())));
    }
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return this.element == null;
  }

  @Override
  public Optional<Record> getRecord() {
    return Optional.ofNullable(this.element);
  }

  @Override
  public Object getMetadata(final String key) {
    if (key == null)
      return null;

    return metadata == null ? null : metadata.get(key);
  }

  public void setMetadata(final String key, final Object value) {
    if (key == null)
      return;

    if (metadata == null)
      metadata = new HashMap<>();

    metadata.put(key, value);
  }

  @Override
  public Set<String> getMetadataKeys() {
    return metadata == null ? Collections.emptySet() : metadata.keySet();
  }

  public ResultInternal setElement(final Document element) {
    this.element = element;
    return this;
  }

  @Override
  public String toString() {
    if (value != null)
      return value.toString();
    else if (element != null) {
      try {
        return element.toJSON(false).toString();
      } catch (final Exception e) {
        return element.toString();
      }
    }
    else if (content != null)
      return "{" + content.entrySet().stream().map(x -> x.getKey() + ": " + x.getValue())
              .collect(Collectors.joining(", ")) + "}";
    return "{}";
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other)
      return true;

    if (!(other instanceof final ResultInternal otherResult))
      return false;

    if (element != null) {
      if (otherResult.getElement().isEmpty())
        return false;
      return element.equals(otherResult.getElement().get());
    } else if (value != null)
      return value.equals(otherResult.value);
    else {
      if (otherResult.getElement().isPresent())
        return false;
      return this.content != null && this.content.equals(otherResult.content);
    }
  }

  @Override
  public int hashCode() {
    if (element != null)
      return element.hashCode();
    else if (content != null)
      return content.hashCode();
    else if (value != null)
      return value.hashCode();
    return super.hashCode();
  }

  public ResultInternal setPropertiesFromMap(final Map<String, Object> stats) {
    content.putAll(stats);
    return this;
  }
}
