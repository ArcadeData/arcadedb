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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class ResultInternal implements Result {
  protected Map<String, Object> content;
  protected Map<String, Object> temporaryContent;
  protected Map<String, Object> metadata;
  protected Document            element;

  public ResultInternal() {
    content = new LinkedHashMap<>();
  }

  public ResultInternal(final Map<String, Object> map) {
    this.content = map;
  }

  public ResultInternal(final Document ident) {
    this.element = ident;
  }

  public ResultInternal(final Identifiable ident) {
    this.element = (Document) ident.getRecord();
  }

  public void setTemporaryProperty(final String name, Object value) {
    if (temporaryContent == null) {
      temporaryContent = new HashMap<>();
    }
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (value instanceof Result && ((Result) value).isElement()) {
      temporaryContent.put(name, ((Result) value).getElement().get());
    } else {
      temporaryContent.put(name, value);
    }
  }

  public Object getTemporaryProperty(final String name) {
    if (name == null || temporaryContent == null) {
      return null;
    }
    return temporaryContent.get(name);
  }

  public Set<String> getTemporaryProperties() {
    return temporaryContent == null ? Collections.emptySet() : temporaryContent.keySet();
  }

  public ResultInternal setProperty(final String name, Object value) {
    if (value instanceof Optional)
      value = ((Optional) value).orElse(null);

    if (value instanceof Result && ((Result) value).isElement())
      content.put(name, ((Result) value).getElement().get());
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
    if (content != null && content.containsKey(name))
      result = (T) wrap(content.get(name));
    else if (element != null)
      result = (T) wrap(element.get(name));
    else
      result = null;

    if (!(result instanceof Record) && result instanceof Identifiable && ((Identifiable) result).getIdentity() != null)
      result = (T) ((Identifiable) result).getIdentity();

    return result;
  }

  public <T> T getProperty(final String name, final Object defaultValue) {
    T result;
    if (content != null && content.containsKey(name))
      result = (T) wrap(content.get(name));
    else if (element != null && element.has(name))
      result = (T) wrap(element.get(name));
    else
      result = (T) defaultValue;

    if (!(result instanceof Record) && result instanceof Identifiable && ((Identifiable) result).getIdentity() != null)
      result = (T) ((Identifiable) result).getIdentity();
    return result;
  }

  @Override
  public Record getElementProperty(final String name) {
    Object result = null;
    if (content != null && content.containsKey(name))
      result = content.get(name);
    else if (element != null)
      result = element.get(name);

    if (result instanceof Result)
      result = ((Result) result).getRecord().orElse(null);

    if (result instanceof RID)
      result = ((RID) result).getRecord();

    return result instanceof Record ? (Record) result : null;
  }

  private Object wrap(final Object input) {
    if (input instanceof Document && ((Document) input).getIdentity() == null) {
      final ResultInternal result = new ResultInternal();
      final Document elem = (Document) input;
      for (final String prop : elem.getPropertyNames()) {
        result.setProperty(prop, elem.get(prop));
      }
      if (elem.getTypeName() != null)
        result.setProperty("@type", elem.getTypeName());
      return result;

    } else if (isEmbeddedList(input)) {
      return ((List) input).stream().map(this::wrap).collect(Collectors.toList());
    } else if (isEmbeddedSet(input)) {
      return ((Set) input).stream().map(this::wrap).collect(Collectors.toSet());
    } else if (isEmbeddedMap(input)) {
      final Map result = new HashMap();
      for (final Map.Entry<String, Object> o : ((Map<String, Object>) input).entrySet()) {
        result.put(o.getKey(), wrap(o.getValue()));
      }
      return result;
    }
    return input;
  }

  private boolean isEmbeddedSet(final Object input) {
    if (input instanceof Set) {
      for (final Object o : (Set) input) {
        if (o instanceof Record)
          return false;

        else if (isEmbeddedList(o))
          return true;
        else if (isEmbeddedSet(o))
          return true;
        else if (isEmbeddedMap(o))
          return true;
      }
    }
    return false;
  }

  private boolean isEmbeddedMap(final Object input) {
    if (input instanceof Map) {
      for (final Object o : ((Map) input).values()) {
        if (o instanceof Record)
          return false;//TODO
        else if (isEmbeddedList(o))
          return true;
        else if (isEmbeddedSet(o))
          return true;
        else if (isEmbeddedMap(o))
          return true;
      }
    }
    return false;
  }

  private boolean isEmbeddedList(final Object input) {
    if (input instanceof List) {
      for (final Object o : (List) input) {
        if (o instanceof Record)
          return false;
        else if (isEmbeddedList(o))
          return true;
        else if (isEmbeddedSet(o))
          return true;
        else if (isEmbeddedMap(o))
          return true;
      }
    }
    return false;
  }

  public Set<String> getPropertyNames() {
    final Set<String> result = new LinkedHashSet<>();
    if (element != null)
      result.addAll(element.getPropertyNames());

    if (content != null)
      result.addAll(content.keySet());
    return result;
  }

  public boolean hasProperty(final String propName) {
    if (element != null && element.getPropertyNames().contains(propName))
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

    if (hasProperty("@rid")) {
      final Object rid = getProperty("@rid");
      return Optional.of((RID) (rid instanceof RID ? rid : new RID(null, rid.toString())));
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
    if (element != null)
      return element.toString();

    if (content != null)
      return "{ " + content.entrySet().stream().map(x -> x.getKey() + ": " + x.getValue()).reduce("", (a, b) -> a + b + "\n") + " }";

    return "{}";
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof ResultInternal))
      return false;

    final ResultInternal resultObj = (ResultInternal) obj;
    if (element != null) {
      if (resultObj.getElement().isEmpty()) {
        return false;
      }
      return element.equals(resultObj.getElement().get());
    } else {
      if (resultObj.getElement().isPresent())
        return false;

      return this.content != null && this.content.equals(resultObj.content);
    }
  }

  @Override
  public int hashCode() {
    if (element != null)
      return element.hashCode();

    if (content != null)
      return content.hashCode();

    return super.hashCode();
  }

  public ResultInternal setPropertiesFromMap(final Map<String, Object> stats) {
    content.putAll(stats);
    return this;
  }
}
