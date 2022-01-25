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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeProperty<T> implements Property<T> {

  protected final ArcadeElement element;
  protected final String        key;
  protected final ArcadeGraph   graph;
  protected       T             value;
  protected       boolean       removed = false;

  protected ArcadeProperty(final ArcadeElement element, final String key, final T value) {
    this.element = element;
    this.key = key;
    this.value = value;
    this.graph = (ArcadeGraph) element.graph();
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public T value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return value != null;
  }

  @Override
  public Element element() {
    return element;
  }

  @Override
  public void remove() {
    if (this.removed)
      return;
    this.graph.tx().readWrite();

    final MutableDocument mutableElement = element.baseElement.modify();
    mutableElement.remove(key);
    mutableElement.save();
    if (mutableElement != element.baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      element.baseElement = mutableElement;

    this.removed = true;
  }

  @Override
  public final boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  @Override
  public int hashCode() {
    return ElementHelper.hashCode(this);
  }

  @Override
  public String toString() {
    return StringFactory.propertyString(this);
  }

  public static void validateValue(final Object value) {
    if (value != null) {
      if (value.getClass().isArray())
        // DO NOT SUPPORT ARRAY BECAUSE ARCADE TRANSFORM THEM IMPLICITLY INTO LISTS
        throw new IllegalArgumentException("Array type is not supported");
      Type.validateValue(value);
    }
  }
}
