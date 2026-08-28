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

public class Value extends SimpleNode {
  protected IntValue     intValue;
  protected FloatValue   floatValue;
  protected StringValue  stringValue;
  protected BooleanValue booleanValue;
  protected EnumValue    enumValue;
  protected ListValue    listValue;
  protected ObjectValue  objectValue;

  public Value(final int id) {
    super(id);
  }

  /**
   * Returns the single scalar literal this value node holds, mirroring {@link ValueWithVariable#getValue()} for the
   * variable-free productions (currently the default value of a variable definition). Returns {@code null} for the
   * list and object productions: unlike their {@code WithVariable} counterparts those do not extend
   * {@link AbstractValue}, so there is no value to hand back and the caller reports the unsupported literal with the
   * context it has.
   */
  public AbstractValue getValue() {
    if (intValue != null)
      return intValue;
    else if (floatValue != null)
      return floatValue;
    else if (stringValue != null)
      return stringValue;
    else if (booleanValue != null)
      return booleanValue;
    else if (enumValue != null)
      return enumValue;

    return null;
  }

  @Override
  public String toString() {
    final AbstractValue value = getValue();
    // FALL BACK TO THE NODE NAME FOR THE LIST AND OBJECT PRODUCTIONS, RATHER THAN PRINTING A BARE "null"
    return value != null ? value.toString() : super.toString();
  }
}
/* ParserGeneratorCC - OriginalChecksum=3b4e38a9efac4b8f5a5e4a77f5ffce49 (do not edit this line) */
