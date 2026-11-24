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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * An enclosing row with up to two variables bound over it, for an openCypher expression that evaluates its body once per
 * element with its own variables in scope: a list comprehension, {@code reduce()}, {@code allReduce()}, a list predicate and
 * the hops of a pattern comprehension (issue #12723). Those used to copy every column of the enclosing row into a new
 * {@link ResultInternal} per element; this row answers the bound names itself and every other name from the enclosing row,
 * so a body over N elements costs no copy at all. The caller builds it once and {@link #bind rebinds} it per element, which
 * is safe because evaluating an expression never keeps the row it was evaluated on.
 * <p>
 * It answers as that copy did: a binding shadows an enclosing column of the same name, the second binding shadows the
 * first (the copy set them in that order), a name not bound here exists when the enclosing row lists it
 * ({@link Result#listsProperty}), and the row is a projection with no element, metadata or database. A name passed as
 * {@code null} is not bound, so the enclosing row answers for it. Read-only.
 */
public final class VariableOverlayResult implements Result {
  private final Result  outer;
  private final String  firstName;
  private final String  secondName;
  /**
   * Whether {@link #outer} answers {@code getProperty()} for the names it lists and nothing else: a plain row with content,
   * whose names are its content keys and which never falls back to its record then, a plain row with no record, and another
   * overlay
   */
  private final boolean outerAnswersListedNamesOnly;
  private       Object  firstValue;
  private       Object  secondValue;

  /**
   * @param outer      the enclosing row, or {@code null} for none
   * @param firstName  the first variable, or {@code null} to bind none
   * @param secondName the second variable, which shadows the first when both have the same name, or {@code null}
   */
  public VariableOverlayResult(final Result outer, final String firstName, final String secondName) {
    this.outer = outer;
    this.firstName = firstName;
    this.secondName = secondName;
    this.outerAnswersListedNamesOnly = outer instanceof VariableOverlayResult || (outer != null && outer.getClass() == ResultInternal.class
        && answersListedNamesOnly((ResultInternal) outer));
  }

  private static boolean answersListedNamesOnly(final ResultInternal row) {
    return row.element == null || (row.content != null && !row.content.isEmpty());
  }

  public VariableOverlayResult(final Result outer, final String name) {
    this(outer, name, null);
  }

  /**
   * Binds the first variable, stored as a {@link ResultInternal} row stores a value.
   */
  public VariableOverlayResult bind(final Object value) {
    this.firstValue = ResultInternal.toStoredValue(null, value);
    return this;
  }

  /**
   * Binds both variables, stored as a {@link ResultInternal} row stores a value.
   */
  public VariableOverlayResult bind(final Object first, final Object second) {
    this.firstValue = ResultInternal.toStoredValue(null, first);
    this.secondValue = ResultInternal.toStoredValue(null, second);
    return this;
  }

  /**
   * Binds the second variable only, as {@code reduce()} does with its accumulator.
   */
  public VariableOverlayResult bindSecond(final Object value) {
    this.secondValue = ResultInternal.toStoredValue(null, value);
    return this;
  }

  private boolean bindsSecond(final String name) {
    return secondName != null && secondName.equals(name);
  }

  private boolean bindsFirst(final String name) {
    return firstName != null && firstName.equals(name);
  }

  private boolean outerLists(final String name) {
    return outer != null && outer.listsProperty(name);
  }

  /**
   * The enclosing row's value for {@code name}, stored and read back as the copy this row replaces stored it.
   */
  private Object outerValue(final String name) {
    return ResultInternal.toPropertyValue(ResultInternal.toStoredValue(null, outer.getProperty(name)));
  }

  @Override
  public <T> T getProperty(final String name) {
    if (bindsSecond(name))
      return (T) ResultInternal.toPropertyValue(secondValue);
    if (bindsFirst(name))
      return (T) ResultInternal.toPropertyValue(firstValue);
    if (outer == null)
      return null;
    // The copy held only the names the enclosing row lists, so any other name read null on it, even one the enclosing row
    // resolves through a record it wraps. A row that answers its listed names only is read directly, without the test
    if (!outerAnswersListedNamesOnly && !outer.listsProperty(name))
      return null;
    return (T) outerValue(name);
  }

  @Override
  public <T> T getProperty(final String name, final Object defaultValue) {
    if (bindsSecond(name) || bindsFirst(name))
      return getProperty(name);
    return outerLists(name) ? (T) outerValue(name) : (T) defaultValue;
  }

  @Override
  public Object getPropertyIfPresent(final String name, final Object absentValue) {
    return getProperty(name, absentValue);
  }

  @Override
  public boolean hasProperty(final String name) {
    return bindsSecond(name) || bindsFirst(name) || outerLists(name);
  }

  @Override
  public boolean listsProperty(final String name) {
    return hasProperty(name);
  }

  @Override
  public Set<String> getPropertyNames() {
    final Set<String> outerNames = outer != null ? outer.getPropertyNames() : Collections.emptySet();
    final Set<String> names = new LinkedHashSet<>(outerNames.size() + 3);
    names.addAll(outerNames);
    if (firstName != null)
      names.add(firstName);
    if (secondName != null)
      names.add(secondName);
    return names;
  }

  @Override
  public Record getElementProperty(final String name) {
    Object result = hasProperty(name) ? getProperty(name) : null;
    if (result instanceof Result r)
      result = r.getRecord().orElse(null);
    if (result instanceof RID rid)
      result = rid.getRecord();
    return result instanceof Record r ? r : null;
  }

  @Override
  public Optional<RID> getIdentity() {
    if (!hasProperty(RID_PROPERTY))
      return Optional.empty();
    final Object rid = getProperty(RID_PROPERTY);
    if (rid == null)
      return Optional.empty();
    if (rid instanceof RID r)
      return Optional.of(r);
    return Optional.of(RID.create(null, rid.toString()));
  }

  @Override
  public boolean isElement() {
    return false;
  }

  @Override
  public Optional<Document> getElement() {
    return Optional.empty();
  }

  @Override
  public Document toElement() {
    return null;
  }

  @Override
  public Optional<Record> getRecord() {
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return true;
  }

  @Override
  public Object getMetadata(final String key) {
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    return Collections.emptySet();
  }

  @Override
  public Database getDatabase() {
    return null;
  }

  @Override
  public Map<String, Object> toMap() {
    final Set<String> names = getPropertyNames();
    final Map<String, Object> map = new LinkedHashMap<>(names.size() * 2);
    for (final String name : names)
      map.put(name, getProperty(name));
    return map;
  }

  @Override
  public String toString() {
    return toMap().toString();
  }
}
