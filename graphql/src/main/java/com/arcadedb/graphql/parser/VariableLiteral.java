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

import com.arcadedb.exception.CommandParsingException;

public class VariableLiteral extends AbstractValue {
  public VariableLiteral(final int id) {
    super(id);
  }

  /**
   * A variable has no value of its own: it is resolved against the variable values of the operation being executed
   * (see {@code GraphQLSchema.resolveValue}). Reaching this method means the caller is on a path that cannot bind
   * variables, which used to return the never-assigned {@code SimpleNode.value}, i.e. {@code null}, and silently
   * produce a wrong result instead of an error. See issue #6834.
   */
  @Override
  public Object getValue() {
    throw new CommandParsingException("GraphQL variable '$" + getName() + "' cannot be resolved in this context");
  }


  @Override
  public String toString() {
    return "VariableLiteral{" + name + '}';
  }
}
/* ParserGeneratorCC - OriginalChecksum=1d24a436d5861d19357de949dd126579 (do not edit this line) */
