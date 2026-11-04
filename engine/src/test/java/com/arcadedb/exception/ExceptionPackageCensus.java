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
package com.arcadedb.exception;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test helper shared by the remote drivers' exception censuses (issues #11045, #11198): the concrete public exceptions of the
 * engine's {@code com.arcadedb.exception} package, one scan for both the HTTP and the gRPC driver instead of a copy in each.
 */
public final class ExceptionPackageCensus {
  private ExceptionPackageCensus() {
  }

  public static List<Class<?>> concreteExceptions() throws Exception {
    final String packagePath = ArcadeDBException.class.getPackageName().replace('.', '/');
    final URL location = ArcadeDBException.class.getProtectionDomain().getCodeSource().getLocation();
    final List<String> classNames = new ArrayList<>();
    final Path root = Path.of(location.toURI());
    if (Files.isDirectory(root)) {
      try (final Stream<Path> files = Files.list(root.resolve(packagePath))) {
        files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".class") && !n.contains("$"))
            .forEach(n -> classNames.add(n.substring(0, n.length() - ".class".length())));
      }
    } else {
      try (final JarFile jar = new JarFile(root.toFile())) {
        jar.stream().map(JarEntry::getName)
            .filter(n -> n.startsWith(packagePath + "/") && n.endsWith(".class") && !n.contains("$") && n.indexOf('/',
                packagePath.length() + 1) < 0)
            .forEach(n -> classNames.add(n.substring(packagePath.length() + 1, n.length() - ".class".length())));
      }
    }

    final List<Class<?>> types = new ArrayList<>();
    for (final String simpleName : classNames) {
      final Class<?> type = Class.forName(ArcadeDBException.class.getPackageName() + "." + simpleName);
      if (ArcadeDBException.class.isAssignableFrom(type) && Modifier.isPublic(type.getModifiers()) && !Modifier.isAbstract(
          type.getModifiers()))
        types.add(type);
    }
    assertThat(types).as("the scan found the package").hasSizeGreaterThan(30);
    return types;
  }
}
