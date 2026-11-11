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

public class Type extends SimpleNode {
  protected TypeName typeName;
  protected ListType listType;
  protected boolean  bang = false;

  public Type(final int id) {
    super(id);
  }

  public ListType getListType() {
    return listType;
  }

  public TypeName getTypeName() {
    return typeName;
  }

  public boolean isBang() {
    return bang;
  }

  /**
   * Whether the type is non-null, a named one ({@code String!}) or a list ({@code [String]!}): the grammar puts the bang of a
   * list on the {@link ListType}, so {@link #isBang()} alone answers false for {@code [String]!} (issue #11447).
   */
  public boolean isNonNull() {
    return bang || (listType != null && listType.isBang());
  }

  /** The type as written in a schema, e.g. {@code [String!]!}, for the messages that name it. */
  @Override
  public String toString() {
    if (typeName != null)
      return typeName.getName() + (bang ? "!" : "");
    if (listType != null)
      return "[" + listType.getType() + "]" + (listType.isBang() ? "!" : "");
    return super.toString();
  }


  public String treeToString(final String prefix) {
    return prefix + "Type{" + (typeName != null ? typeName.getName() : listType.getType().getTypeName().getName()) + "}";
  }
}
/* ParserGeneratorCC - OriginalChecksum=c03be8fb7a8fc8bf149d18a7ff368c74 (do not edit this line) */
