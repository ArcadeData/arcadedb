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
package com.arcadedb.utility;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Resolve system variables embedded in a String.
 *
 * @author Luca Garulli
 */
public class SystemVariableResolver implements VariableParserListener {
  public static final String VAR_BEGIN = "${";
  public static final String VAR_END   = "}";

  public static final SystemVariableResolver INSTANCE = new SystemVariableResolver();

  public String resolveSystemVariables(final String iPath) {
    return resolveSystemVariables(iPath, null);
  }

  public String resolveSystemVariables(final String iPath, final String iDefault) {
    if (iPath == null)
      return iDefault;

    return (String) VariableParser.resolveVariables(iPath, VAR_BEGIN, VAR_END, this, iDefault);
  }

  public static String resolveVariable(final String variable) {
    if (variable == null)
      return null;

    String resolved = System.getProperty(variable);

    if (resolved == null)
      // TRY TO FIND THE VARIABLE BETWEEN SYSTEM'S ENVIRONMENT PROPERTIES
      resolved = System.getenv(variable);

    if (resolved == null) {
      final GlobalConfiguration cfg = GlobalConfiguration.findByKey(variable);
      if (cfg != null)
        resolved = cfg.getValueAsString();
    }

    return resolved;
  }

  @Override
  public String resolve(final String variable) {
    return resolveVariable(variable);
  }

  public static void setEnv(final String name, final String value) {
    final Map<String, String> map = new HashMap<String, String>(System.getenv());
    map.put(name, value);
    setEnv(map);
  }

  public static void setEnv(final Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException ignore) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception e2) {
        LogManager.instance().log(SystemVariableResolver.class, Level.SEVERE, "", e2);
      }
    } catch (Exception e1) {
      LogManager.instance().log(SystemVariableResolver.class, Level.SEVERE, "", e1);
    }
  }
}
