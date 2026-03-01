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
package com.arcadedb.query.select;

/**
 * Fluent block for the BETWEEN operator. Accepts a range of two values via {@code values(low, high)}.
 */
public class SelectWhereBetweenBlock {
  private final Select select;

  public SelectWhereBetweenBlock(final Select select) {
    this.select = select;
  }

  public SelectWhereAfterBlock values(final Object low, final Object high) {
    select.checkNotCompiled();
    if (select.propertyValue != null)
      throw new IllegalArgumentException("Property value has already been set");
    select.propertyValue = new Object[] { low, high };
    return new SelectWhereAfterBlock(select);
  }
}
