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
package com.arcadedb.query.sql.executor;/*
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import com.arcadedb.database.Identifiable;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Methods can be used on various objects with different number of arguments. SQL syntax: {@literal <object_name>.<method_name>([parameters])}
 *
 * @author Johann Sorel (Geomatys)
 */
@ExcludeFromJacocoGeneratedReport
public interface SQLMethod extends Comparable<SQLMethod> {

  /**
   * @return method name
   */
  String getName();

  /**
   * Returns a convenient SQL String representation of the method.
   * <p>
   * Example :
   *
   * <pre>
   *  field.myMethod( param1, param2, [optionalParam3])
   * </pre>
   * <p>
   * This text will be used in exception messages.
   *
   * @return String , never null.
   */
  String getSyntax();

  /**
   * Process a record.
   *
   * @param self          current object
   * @param currentRecord : current record
   * @param context       execution context
   * @param params        : function parameters, number is ensured to be within minParams and maxParams.
   *
   * @return evaluation result
   */
  Object execute(Object self, Identifiable currentRecord, CommandContext context, Object[] params);

  boolean evaluateParameters();
}
