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
/* ParserGeneratorCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.graphql.parser;

public class Selection extends SimpleNode {
  protected Name           name;
  protected FieldWithAlias fieldWithAlias;
  protected Field          field;
  protected boolean        ellipsis = false;
  protected FragmentSpread fragmentSpread;
  protected InlineFragment inlineFragment;

  public Selection(final int id) {
    super(id);
  }


  public String getName() {
    return name != null ? name.value : null;
  }

  public Field getField() {
    return field;
  }

  public FieldWithAlias getFieldWithAlias() {
    return fieldWithAlias;
  }

  /**
   * The field node of this selection whichever way it was written: {@code field} for a plain {@code name}, {@code fieldWithAlias}
   * for {@code alias: name}. Null for an ellipsis selection (fragment spread / inline fragment). Every consumer that resolves a
   * selection against the schema must go through this (or the accessors below) rather than through {@link #getField()} alone,
   * or an aliased selection silently resolves to nothing (issues #6384, #6453, #7036).
   */
  public AbstractField getAnyField() {
    return field != null ? field : fieldWithAlias;
  }

  /**
   * The real field name to resolve against the schema: the aliased-away name for {@code alias: name}, otherwise
   * {@link #getName()}, which for an aliased selection carries the alias instead.
   */
  public String getFieldName() {
    return fieldWithAlias != null ? fieldWithAlias.getName() : getName();
  }

  public Arguments getArguments() {
    if (field != null)
      return field.getArguments();
    return fieldWithAlias != null ? fieldWithAlias.getArguments() : null;
  }

  public SelectionSet getSelectionSet() {
    if (field != null)
      return field.getSelectionSet();
    return fieldWithAlias != null ? fieldWithAlias.getSelectionSet() : null;
  }
}
/* ParserGeneratorCC - OriginalChecksum=aac9a2d576730b830f5ef7c02bdf7951 (do not edit this line) */
