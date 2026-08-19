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

/**
 * A selection field written with a GraphQL alias ({@code alias: fieldName}). The alias itself is
 * carried by the enclosing {@link Selection#getName()} (the parser assigns it there before it knows
 * whether a colon follows); this node's {@code name} - inherited from {@link AbstractField} - is the
 * real, aliased-away field name used to resolve the property.
 */
public class FieldWithAlias extends AbstractField {
  protected Arguments    arguments;
  protected SelectionSet selectionSet;

  public FieldWithAlias(final int id) {
    super(id);
  }

  public Arguments getArguments() {
    return arguments;
  }

  public SelectionSet getSelectionSet() {
    return selectionSet;
  }
}
/* ParserGeneratorCC - OriginalChecksum=46c98ffd2c99423a541cd9d46c3fc779 (do not edit this line) */
