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

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code find} projection, parsed once and applied to every document of the answer as {@code mongod} applies it (issue #12072):
 * <ul>
 *   <li>an inclusion ({@code {a: 1}}) keeps the named fields, in the order of the document, not of the projection; an exclusion
 *   ({@code {a: 0}}) keeps everything else. The two cannot be mixed, except that the {@code _id} can always be excluded from an
 *   inclusion and included in an exclusion; an inclusion keeps the {@code _id} unless it excludes it;</li>
 *   <li>a dotted path enters an embedded document, and applies the rest of the path to each element of an array: an embedded
 *   document element is projected (and kept, even when nothing of it is left), a nested array is entered the same way, and a
 *   scalar element is dropped by an inclusion and kept by an exclusion;</li>
 *   <li>an inclusion leaves out a field whose value is a scalar where the path goes on below it, an exclusion keeps it;</li>
 *   <li>two paths of which one is the other or lies below it ({@code a} and {@code a.b}) are a path collision, refused.</li>
 * </ul>
 * The projection operators ({@code $slice}, {@code $elemMatch}, {@code $}) are refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoProjection {
  private static final String ID = "_id";

  private final boolean inclusion;
  private final Node    root;
  /** Whether an inclusion keeps the {@code _id} without naming it. */
  private final boolean implicitId;

  /** A level of the projection: the fields named at that level, each either a leaf (the whole field) or a level of its own. */
  private static final class Node {
    final Map<String, Node> children = new HashMap<>();

    boolean isLeaf() {
      return children.isEmpty();
    }
  }

  private static final Node LEAF = new Node();

  /**
   * @throws IllegalArgumentException for a projection operator, or an inclusion mixed with an exclusion
   * @throws MongoServerError         for a path collision
   */
  MongoProjection(final Document fields) {
    boolean included = false;
    boolean excluded = false;
    for (final Map.Entry<String, Object> entry : fields.entrySet()) {
      final Object flag = entry.getValue();
      if (flag instanceof Document || flag instanceof List)
        throw new IllegalArgumentException("Projection operator on '" + entry.getKey() + "' is not supported");
      // the _id never conflicts: it can be excluded from an inclusion and included in an exclusion
      if (ID.equals(entry.getKey()))
        continue;
      if (Utils.isTrue(flag))
        included = true;
      else
        excluded = true;
    }
    if (included && excluded)
      throw new IllegalArgumentException("Cannot do exclusion on a field in an inclusion projection");
    // only the _id given: {_id: 1} answers the _id alone, {_id: 0} everything else
    this.inclusion = included || (!excluded && fields.containsKey(ID) && Utils.isTrue(fields.get(ID)));
    this.implicitId = inclusion && !fields.containsKey(ID);

    this.root = new Node();
    for (final Map.Entry<String, Object> entry : fields.entrySet()) {
      final String path = entry.getKey();
      // an inclusion names the fields it keeps, an exclusion the ones it removes: an _id given the other way is not a path of it
      if (ID.equals(path) && Utils.isTrue(entry.getValue()) != inclusion)
        continue;
      add(path);
    }
  }

  boolean isInclusion() {
    return inclusion;
  }

  private void add(final String path) {
    Node node = root;
    int start = 0;
    while (true) {
      final int dot = path.indexOf('.', start);
      final String segment = dot < 0 ? path.substring(start) : path.substring(start, dot);
      final Node existing = node.children.get(segment);
      if (dot < 0) {
        // a path below this one is already there ({a.b} then {a})
        if (existing != null)
          throw collision(path, null);
        node.children.put(segment, LEAF);
        return;
      }
      // the whole field is already there ({a} then {a.b})
      if (existing == LEAF)
        throw collision(path, path.substring(dot + 1));
      final Node next = existing != null ? existing : new Node();
      node.children.put(segment, next);
      node = next;
      start = dot + 1;
    }
  }

  private static MongoServerError collision(final String path, final String remaining) {
    return new MongoServerError(31249, "Location31249", "Path collision at " + path + (remaining != null ? " remaining portion " + remaining : ""));
  }

  /**
   * The projected copy of a document; the document itself is never modified.
   */
  Document apply(final Document document) {
    if (document == null)
      return null;
    return inclusion ? include(root, document, implicitId) : exclude(root, document);
  }

  private static Document include(final Node node, final Document document, final boolean keepId) {
    final Document result = new Document();
    for (final Map.Entry<String, Object> entry : document.entrySet()) {
      final String key = entry.getKey();
      final Node child = node.children.get(key);
      if (child == null) {
        if (keepId && ID.equals(key))
          result.put(key, entry.getValue());
      } else if (child.isLeaf())
        result.put(key, entry.getValue());
      else {
        final Object projected = includeBelow(child, entry.getValue());
        if (projected != null)
          result.put(key, projected);
      }
    }
    return result;
  }

  /**
   * What an inclusion keeps of a value the path goes on below: {@code null} for a scalar, which is left out.
   */
  private static Object includeBelow(final Node node, final Object value) {
    if (value instanceof Document embedded)
      return include(node, embedded, false);
    if (value instanceof List<?> list) {
      final List<Object> result = new ArrayList<>(list.size());
      for (final Object element : list) {
        // a scalar element is dropped; an embedded document is kept, even with nothing left of it
        final Object projected = element instanceof Document || element instanceof List ? includeBelow(node, element) : null;
        if (projected != null)
          result.add(projected);
      }
      return result;
    }
    return null;
  }

  private static Document exclude(final Node node, final Document document) {
    final Document result = new Document();
    for (final Map.Entry<String, Object> entry : document.entrySet()) {
      final Node child = node.children.get(entry.getKey());
      if (child == null)
        result.put(entry.getKey(), entry.getValue());
      else if (!child.isLeaf())
        result.put(entry.getKey(), excludeBelow(child, entry.getValue()));
    }
    return result;
  }

  /**
   * What an exclusion keeps of a value the path goes on below: a scalar as it is.
   */
  private static Object excludeBelow(final Node node, final Object value) {
    if (value instanceof Document embedded)
      return exclude(node, embedded);
    if (value instanceof List<?> list) {
      final List<Object> result = new ArrayList<>(list.size());
      for (final Object element : list)
        result.add(excludeBelow(node, element));
      return result;
    }
    return value;
  }
}
