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
/* ParserGeneratorCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=false,TRACK_TOKENS=true,NODE_PREFIX=,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.graphql.parser;

/**
 * The {@code null} literal (spec section 2.9.5). It is a value node like any other, so a caller tells "the document wrote null"
 * (this node, whose value is null) apart from "the document wrote nothing" (no node) and from a production that has no scalar
 * value (a list or an object of {@link Value}, for which {@link Value#getValue()} answers null). See issue #11451.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NullValue extends AbstractValue {
  public NullValue(final int id) {
    super(id);
  }

  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public String toString() {
    return "NullValue{}";
  }
}
/* ParserGeneratorCC - OriginalChecksum=4913c8bf7d4c4d9e16b2eae751579c62 (do not edit this line) */
