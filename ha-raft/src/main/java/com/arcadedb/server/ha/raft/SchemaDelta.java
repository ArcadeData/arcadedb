/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Difference between two serializations of a database schema, so a {@code SCHEMA_ENTRY} can ship what a DDL
 * actually changed instead of the whole document (issue #6989).
 * <p>
 * {@code CREATE PROPERTY Foo.a STRING} adds a few dozen bytes of logical schema. Before this class every
 * schema entry carried {@code LocalSchema.toJSON().toString()} in full, which on the schema that motivated
 * the issue (1209 types, several MB) was serialized on the leader, written into every node's Raft log,
 * parsed on every follower and rewritten to {@code schema.json} - about 6100 times.
 *
 * <h2>Shape</h2>
 * The schema document is a flat object whose values are either scalars ({@code schemaVersion},
 * {@code dbmsVersion}, ...) or maps keyed by name ({@code types}, {@code triggers},
 * {@code materializedViews}, {@code continuousAggregates}, {@code functions}, {@code extensions},
 * {@code settings}). The delta mirrors that structure and nothing else, so a section added to the schema
 * document later is diffed by the same generic rules with no change here:
 *
 * <pre>
 * { "base":     &lt;schemaVersion the delta was computed against&gt;,
 *   "put":      { "&lt;rootKey&gt;": &lt;value&gt; },                 // changed root values that are NOT maps
 *   "merge":    { "&lt;rootKey&gt;": { "&lt;child&gt;": &lt;value&gt; } },  // added or changed children of a map
 *   "keys":     { "&lt;rootKey&gt;": [ "&lt;child&gt;", ... ] },      // authoritative child key set of that map
 *   "rootKeys": [ "&lt;rootKey&gt;", ... ] }                    // authoritative root key set
 * </pre>
 *
 * <h2>Granularity: one whole child, not a nested diff</h2>
 * A changed child is carried WHOLE - one property added to a type ships that type's entire JSON, not a diff
 * inside it. That is the "send only the affected subtree" option issue #6989 asks for, and it is a deliberate
 * stopping point: one level keeps {@link #compute} and {@link #apply} generic over every section of the schema
 * document, with no per-section knowledge of what a type, a trigger or a function looks like inside, so a
 * section added to the document later needs no change here.
 * <p>
 * The cost is that a type with very many properties still ships all of them when one is added. That is bounded
 * by ONE type rather than by the schema, and the leader's own half-the-document bar refuses a delta that grew
 * past being worth shipping. Recursive diffing would shrink that middle ground and is the obvious next step if
 * a schema ever has types large enough to make it matter.
 *
 * <h2>Why the key sets are carried rather than a list of removals</h2>
 * {@code keys}/{@code rootKeys} make {@link #apply} <b>structure-authoritative</b>: for a section the leader
 * added to or removed from, the result carries exactly the key set the leader had, with the UNCHANGED children
 * taken from the receiver's own copy. A removal therefore needs no separate drop list, and a receiver whose
 * document does not match the leader's base cannot come out of that section with a phantom type no entry ever
 * removes.
 * <p>
 * A key set is proportional to its SECTION, not to the change, so one is emitted only for a section whose key
 * set actually moved - a couple of hundred bytes of names on the DDL that added or dropped something, and
 * nothing at all on a change to another section. Emitting them unconditionally would put every type name of a
 * 1209-type schema into a delta that only touched a setting, purely because {@code types} is a map present in
 * both documents.
 * <p>
 * {@code base} is carried for diagnostics only, for exactly that reason: the applier warns on a mismatch
 * rather than refusing, because the delta entry does not carry a full document to fall back to, and a
 * receiver whose {@code schemaVersion} drifted by a purely local {@code saveConfiguration()} is far more
 * likely than genuine divergence - which the existing WAL-version-gap and verify machinery catches anyway.
 *
 * @author Arcade Data
 */
public final class SchemaDelta {

  /** The {@code schemaVersion} the delta was computed against. Diagnostic - see the class javadoc. */
  static final String FIELD_BASE      = "base";
  /** Root values that are not maps and whose value changed, carried verbatim. */
  static final String FIELD_PUT       = "put";
  /** Per-map upserts: the children that were added or changed, carried verbatim. */
  static final String FIELD_MERGE     = "merge";
  /** Per-map authoritative child key sets. */
  static final String FIELD_KEYS      = "keys";
  /** Authoritative root key set. */
  static final String FIELD_ROOT_KEYS = "rootKeys";

  /** Root field of the schema document holding its monotonic version. */
  static final String SCHEMA_VERSION  = "schemaVersion";

  /**
   * One schema delta as it travels inside a {@code SCHEMA_ENTRY}.
   *
   * @param baseVersion the {@code schemaVersion} the delta was computed against, {@code -1} when unknown
   * @param deltaJson   the serialized delta document ({@link #compute})
   */
  public record Payload(long baseVersion, String deltaJson) {
  }

  private SchemaDelta() {
  }

  /**
   * Computes the delta that turns {@code base} into {@code updated}.
   *
   * @param base    the schema document the receiver is expected to hold
   * @param updated the schema document to reach
   *
   * @return a delta document; never null, and never empty (it always carries {@code base} and
   *         {@code rootKeys})
   */
  public static JSONObject compute(final JSONObject base, final JSONObject updated) {
    final JSONObject delta = new JSONObject();
    delta.put(FIELD_BASE, base.getLong(SCHEMA_VERSION, -1L));

    final JSONObject put = new JSONObject();
    final JSONObject merge = new JSONObject();
    final JSONObject keys = new JSONObject();

    for (final String rootKey : new ArrayList<>(updated.keySet())) {
      final Object updatedValue = updated.opt(rootKey);
      final Object baseValue = base.opt(rootKey);

      if (updatedValue instanceof final JSONObject updatedMap) {
        // A map section: diff it child by child, and carry its key set so apply() can drop what is gone.
        final JSONObject baseMap = baseValue instanceof final JSONObject m ? m : null;
        final JSONObject childUpserts = new JSONObject();

        for (final String childKey : new ArrayList<>(updatedMap.keySet())) {
          final Object childValue = updatedMap.opt(childKey);
          if (baseMap == null || !baseMap.has(childKey) || !sameValue(baseMap.opt(childKey), childValue))
            childUpserts.put(childKey, childValue);
        }

        if (!childUpserts.isEmpty())
          merge.put(rootKey, childUpserts);

        // Only when this section's SHAPE moved. A key set is proportional to the section, not to the change, so
        // emitting it unconditionally would put every one of a 1209-type schema's type names into a delta that
        // only touched, say, a setting or a function - {@code types} is a map present in both documents, so it
        // would qualify without having changed at all. When the two key sets match there is nothing for apply()
        // to drop there, and the children it does not mention are preserved either way.
        if (baseMap == null || !baseMap.keySet().equals(updatedMap.keySet()))
          keys.put(rootKey, new JSONArray(new ArrayList<>(updatedMap.keySet())));

      } else if (!base.has(rootKey) || !sameValue(baseValue, updatedValue))
        put.put(rootKey, updatedValue);
    }

    if (!put.isEmpty())
      delta.put(FIELD_PUT, put);
    if (!merge.isEmpty())
      delta.put(FIELD_MERGE, merge);
    if (!keys.isEmpty())
      delta.put(FIELD_KEYS, keys);
    delta.put(FIELD_ROOT_KEYS, new JSONArray(new ArrayList<>(updated.keySet())));

    return delta;
  }

  /**
   * Applies {@code delta} to {@code current}, returning the resulting schema document. {@code current} is
   * not modified.
   * <p>
   * The result's key set is the one the delta declares, so applying a delta to a document that is not
   * exactly the base it was computed against still yields the leader's structure; only the content of
   * children the delta did not mention is inherited from {@code current}.
   */
  public static JSONObject apply(final JSONObject current, final JSONObject delta) {
    final JSONObject result = current.copy();

    final JSONObject put = delta.getJSONObject(FIELD_PUT, null);
    if (put != null)
      for (final String rootKey : new ArrayList<>(put.keySet()))
        result.put(rootKey, put.opt(rootKey));

    final JSONObject merge = delta.getJSONObject(FIELD_MERGE, null);
    if (merge != null)
      for (final String rootKey : new ArrayList<>(merge.keySet())) {
        final JSONObject upserts = merge.getJSONObject(rootKey);
        final JSONObject target = mapSection(result, rootKey);
        for (final String childKey : new ArrayList<>(upserts.keySet()))
          target.put(childKey, upserts.opt(childKey));
      }

    final JSONObject keys = delta.getJSONObject(FIELD_KEYS, null);
    if (keys != null)
      for (final String rootKey : new ArrayList<>(keys.keySet())) {
        final JSONObject target = mapSection(result, rootKey);
        final Set<String> allowed = toStringSet(keys.getJSONArray(rootKey));
        for (final String childKey : new ArrayList<>(target.keySet()))
          if (!allowed.contains(childKey))
            target.remove(childKey);
      }

    final JSONArray rootKeys = delta.getJSONArray(FIELD_ROOT_KEYS, null);
    if (rootKeys != null) {
      final Set<String> allowed = toStringSet(rootKeys);
      for (final String rootKey : new ArrayList<>(result.keySet()))
        if (!allowed.contains(rootKey))
          result.remove(rootKey);
    }

    return result;
  }

  /**
   * Returns {@code root}'s map-valued child {@code key}, creating (and installing) an empty one when the
   * key is absent or holds something that is not a map. Mutations write through to {@code root}.
   */
  private static JSONObject mapSection(final JSONObject root, final String key) {
    if (root.opt(key) instanceof final JSONObject existing)
      return existing;
    final JSONObject created = new JSONObject();
    root.put(key, created);
    return created;
  }

  /**
   * Structural equality of two values read out of a schema document. {@link JSONObject#equals} compares the
   * underlying tree, so an unchanged type costs no string rendering; everything else in a schema document is
   * a scalar or a small array, for which the rendered form is both cheap and exact.
   * <p>
   * Comparing the RENDERED form is what makes a value that JSON round-tripped into a different Java type - an
   * {@code Integer} on one side and a {@code Long} on the other - compare equal, which {@code equals()} would
   * not. The converse, two numerically equal values rendered differently ({@code 1} against {@code 1.0}),
   * compares as changed and ships a redundant upsert; a schema document's root scalars are a long and two
   * strings, so neither case arises today.
   */
  private static boolean sameValue(final Object left, final Object right) {
    if (left == right)
      return true;
    if (left == null || right == null)
      return false;
    if (left instanceof final JSONObject leftMap)
      return right instanceof JSONObject && leftMap.equals(right);
    if (right instanceof JSONObject)
      return false;
    return String.valueOf(left).equals(String.valueOf(right));
  }

  private static Set<String> toStringSet(final JSONArray array) {
    final Set<String> set = HashSet.newHashSet(array.length());
    for (int i = 0; i < array.length(); i++)
      set.add(array.getString(i));
    return set;
  }

}
