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
package com.arcadedb.query.sql.executor;

import com.arcadedb.query.sql.parser.SimpleNode;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9628: the AST walks behind statement classification, the correlated sub-query cache and the parallel-safety check
 * descend through parse-tree nodes and through an explicit list of plain holder classes
 * ({@link SqlAstInspector#AST_HOLDER_CLASSES}). A node that gains a field of some other parser-package class would hide
 * whatever that class holds - a nested {@code INSERT} included - from all three. This pins the list to the tree.
 */
class SqlAstHolderClassesTest {

  @Test
  void everyParserClassReachableFromANodeIsANodeOrAListedHolder() throws Exception {
    final String pkg = SimpleNode.class.getPackageName();
    // THE MAIN CLASSES DIRECTORY, NOT THE FIRST ONE ON THE CLASSPATH WITH THIS PACKAGE (TEST CLASSES SHARE IT)
    final File root = new File(SimpleNode.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertThat(root).isDirectory();
    final File[] files = new File(root, pkg.replace('.', '/')).listFiles((d, name) -> name.endsWith(".class"));
    assertThat(files).isNotEmpty();

    final Set<String> unlisted = new TreeSet<>();
    int walked = 0;
    for (final File file : files) {
      final Class<?> type = Class.forName(pkg + "." + file.getName().substring(0, file.getName().length() - 6), false,
          SimpleNode.class.getClassLoader());
      if (!SimpleNode.class.isAssignableFrom(type) && !SqlAstInspector.AST_HOLDER_CLASSES.contains(type))
        continue;
      ++walked;
      for (final Field f : type.getDeclaredFields()) {
        if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers()))
          continue;
        final List<Class<?>> referenced = new ArrayList<>();
        collect(f.getGenericType(), referenced);
        for (final Class<?> r : referenced)
          // AN ENUM HOLDS NO STATEMENT; AN INTERFACE (Node, BinaryCompareOperator) IS JUDGED BY THE RUNTIME CLASS OF ITS VALUE
          if (pkg.equals(r.getPackageName()) && !r.isEnum() && !r.isInterface() && !SimpleNode.class.isAssignableFrom(r)
              && !SqlAstInspector.AST_HOLDER_CLASSES.contains(r))
            unlisted.add(type.getSimpleName() + "." + f.getName() + " -> " + r.getSimpleName());
      }
    }
    assertThat(walked).isGreaterThan(100);
    assertThat(unlisted).as("parser classes held by a node but not walked: add them to SqlAstInspector.AST_HOLDER_CLASSES, "
        + "or mark the field transient if it is a cache").isEmpty();
  }

  private static void collect(final Type type, final List<Class<?>> out) {
    if (type instanceof Class<?> c) {
      if (c.isArray())
        collect(c.getComponentType(), out);
      else
        out.add(c);
    } else if (type instanceof ParameterizedType p) {
      collect(p.getRawType(), out);
      for (final Type arg : p.getActualTypeArguments())
        collect(arg, out);
    } else if (type instanceof GenericArrayType g)
      collect(g.getGenericComponentType(), out);
    else if (type instanceof WildcardType w)
      for (final Type bound : w.getUpperBounds())
        collect(bound, out);
  }
}
