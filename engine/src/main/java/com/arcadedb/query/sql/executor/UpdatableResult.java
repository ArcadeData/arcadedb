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
import com.arcadedb.database.MutableDocument;

import java.util.Optional;
import java.util.Set;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UpdatableResult extends ResultInternal {
  protected     ResultInternal  previousValue = null;
  private final MutableDocument element;

  public UpdatableResult(final MutableDocument element) {
    this.element = element;
  }

  @Override
  public <T> T getProperty(final String name) {
    return (T) element.get(name);
  }

  @Override
  public Set<String> getPropertyNames() {
    return element.getPropertyNames();
  }

  public boolean hasProperty(final String propName) {
    return element != null && element.getPropertyNames().contains(propName);
  }

  @Override
  public boolean isElement() {
    return true;
  }

  @Override
  public Optional<Document> getElement() {
    return Optional.of(element);
  }

  @Override
  public Document toElement() {
    return element;
  }

  @Override
  public ResultInternal setProperty(final String name, final Object value) {
    element.set(name, value);
    return null;
  }

  public void removeProperty(final String name) {
    element.set(name, null);
  }
}
