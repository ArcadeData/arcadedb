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
package com.arcadedb.query.polyglot;

import com.arcadedb.utility.LRUCache;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Decides which host classes a polyglot script may resolve through {@code Java.type(...)}.
 * <p>
 * Patterns are matched <b>literally</b>, never as regular expressions (GHSA-wx28-2265-f788: the previous
 * implementation used {@link String#matches(String)}, so an entry such as {@code "java.util.*"} was compiled as a
 * regex whose unescaped dots and trailing {@code .*} matched anything shaped {@code java<any char>util<anything>} -
 * silently admitting {@code java.util.zip.ZipFile} and {@code java.util.jar.JarFile}, both of which open an arbitrary
 * filesystem path from a plain {@code String} and expose {@code getInputStream()}). Three forms are supported:
 * <ul>
 * <li>{@code com.example.*} - only the classes declared directly in package {@code com.example}, subpackages
 * excluded;</li>
 * <li>{@code com.example.**} - package {@code com.example} and every subpackage, recursively;</li>
 * <li>{@code com.example.MyClass} - that exact class and nothing else.</li>
 * </ul>
 * <p>
 * On top of the caller-supplied allow-list, {@link #DENIED} is always enforced and always wins. It exists because a
 * package-prefix allow-list is a coarse instrument: even packages made of pure value types host I/O-capable
 * neighbours ({@code java.util.zip}, {@code java.util.jar}, {@code java.util.logging.FileHandler}), and
 * {@code java.util} itself declares {@code Formatter} (creates/truncates a file from a path string), {@code Scanner},
 * {@code Timer} (spawns host threads outside GraalVM's thread policy) and {@code ServiceLoader}. Keeping the deny-list
 * inside the engine means an embedder who configures a broad allow-list still cannot reach a process, a file, a
 * socket, the reflection API or the class loader.
 * <p>
 * A bare (non-wildcard) {@code DENIED} entry names one exact class, so it does not by itself cover a subclass that
 * lives elsewhere in an allow-listed package - {@code java.util.PropertyResourceBundle} and
 * {@code java.util.ListResourceBundle} both extend the denied {@code java.util.ResourceBundle} and inherit its
 * classpath-reading {@code getBundle(String)} factory, yet neither name matches the {@code "java.util.ResourceBundle"}
 * pattern (GHSA-j57p-qmrh-v7xv). To close that gap, a class admitted by name is additionally checked by walking its
 * resolved superclass and interface hierarchy: if any ancestor's name matches a <i>precise</i> deny entry - one that
 * pins a single type, i.e. a bare class name or a {@code Type$*} nested-type pattern - the class is rejected too. A
 * package-wildcard entry ({@code pkg.*} or {@code pkg.**}) is deliberately excluded from this walk: it already
 * matches every class in that namespace by name regardless of hierarchy, and applying it during the walk would also
 * catch unrelated marker interfaces that merely happen to live in a denied package (e.g. {@code java.io.Serializable},
 * implemented by most collection classes) and reject classes that were never meant to be denied.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HostClassLookupFilter implements Predicate<String> {
  /**
   * Class families that are never resolvable from a script, whatever the allow-list says. Grouped by the capability
   * they would hand over: process/native execution, reflection and class loading, filesystem and network I/O, host
   * thread creation, service/provider loading and code generation.
   */
  public static final String[] DENIED = {                //
      // PROCESS, JVM AND NATIVE CONTROL
      "java.lang.Runtime",                               //
      "java.lang.ProcessBuilder",                        //
      "java.lang.ProcessBuilder$*",                      //
      "java.lang.Process",                               //
      "java.lang.ProcessHandle",                         //
      "java.lang.System",                                //
      "java.lang.Shutdown",                              //
      "java.lang.SecurityManager",                       //
      "java.lang.Thread",                                //
      "java.lang.ThreadGroup",                           //
      "java.lang.Module",                                //
      "java.lang.ModuleLayer",                           //
      "java.lang.Package",                               //
      "java.lang.foreign.**",                            //
      "java.lang.instrument.**",                         //
      "java.lang.management.**",                         //
      "java.lang.module.**",                             //
      // REFLECTION AND CLASS LOADING
      "java.lang.Class",                                 //
      "java.lang.ClassLoader",                           //
      "java.lang.reflect.**",                            //
      "java.lang.invoke.**",                             //
      "java.beans.**",                                   //
      // FILESYSTEM, NETWORK AND SERIALIZATION
      "java.io.**",                                      //
      "java.nio.**",                                     //
      "java.net.**",                                     //
      "java.rmi.**",                                     //
      "javax.net.**",                                    //
      "java.util.zip.**",                                //
      "java.util.jar.**",                                //
      "java.util.logging.**",                            //
      "java.util.prefs.**",                              //
      "java.util.Formatter",                             //
      "java.util.Scanner",                               //
      // HOST THREADS OUTSIDE THE POLYGLOT THREAD POLICY
      "java.util.concurrent.**",                         //
      "java.util.Timer",                                 //
      "java.util.TimerTask",                             //
      // SERVICE/PROVIDER LOOKUP, SCRIPTING AND COMPILATION
      "java.util.ServiceLoader",                         //
      "java.util.ServiceLoader$*",                       //
      "java.util.ResourceBundle",                        //
      "java.util.spi.**",                                //
      "java.security.**",                                //
      "javax.security.**",                               //
      "javax.naming.**",                                 //
      "javax.management.**",                             //
      "javax.script.**",                                 //
      "javax.tools.**",                                  //
      "java.sql.**",                                     //
      "javax.sql.**",                                    //
      // JDK AND RUNTIME INTERNALS
      "jdk.**",                                          //
      "sun.**",                                          //
      "com.sun.**",                                      //
      "org.graalvm.**",                                  //
      "com.oracle.truffle.**",                           //
  };

  /**
   * Bound on {@link #hierarchyDenialCache}. A {@code HostClassLookupFilter} instance is not short-lived - one is
   * cached for the lifetime of a reusable {@code PolyglotQueryEngine}/{@code GraalPolyglotEngine} - so an
   * unbounded cache keyed by attacker-suppliable class-name strings (including names that fail to resolve, which
   * are cached too) would let a script exhaust memory by probing many distinct names in an allow-listed package.
   */
  private static final int HIERARCHY_CACHE_SIZE = 256;

  private final String[] allowed;
  private final String[] denied;
  /** Subset of {@link #denied} that pins a single type (bare class name or {@code Type$*}), used for the hierarchy walk. */
  private final String[] preciseDenied;
  /** Per-instance, size-bounded cache of {@link #inheritsAPreciselyDeniedType(String)}, keyed by class name. */
  private final Map<String, Boolean> hierarchyDenialCache = Collections.synchronizedMap(new LRUCache<>(HIERARCHY_CACHE_SIZE));

  public HostClassLookupFilter(final Collection<String> allowedPatterns, final Collection<String> extraDeniedPatterns) {
    this.allowed = allowedPatterns == null || allowedPatterns.isEmpty() ?
        null :
        allowedPatterns.toArray(new String[allowedPatterns.size()]);

    if (extraDeniedPatterns == null || extraDeniedPatterns.isEmpty())
      this.denied = DENIED;
    else {
      this.denied = new String[DENIED.length + extraDeniedPatterns.size()];
      System.arraycopy(DENIED, 0, this.denied, 0, DENIED.length);
      int i = DENIED.length;
      for (final String pattern : extraDeniedPatterns)
        this.denied[i++] = pattern;
    }

    final List<String> precise = new ArrayList<>();
    for (final String pattern : this.denied)
      if (isPreciseTypePattern(pattern))
        precise.add(pattern);
    this.preciseDenied = precise.toArray(new String[precise.size()]);
  }

  @Override
  public boolean test(final String className) {
    if (className == null || allowed == null)
      // NO ALLOW-LIST CONFIGURED: HOST-CLASS LOOKUP IS COMPLETELY DISABLED
      return false;

    for (int i = 0; i < denied.length; i++)
      if (matches(className, denied[i]))
        return false;

    boolean permitted = false;
    for (int i = 0; i < allowed.length; i++)
      if (matches(className, allowed[i])) {
        permitted = true;
        break;
      }
    if (!permitted)
      return false;

    // The class is admitted by name, but it may still inherit a capability from a denied ancestor it does not share
    // a name with (GHSA-j57p-qmrh-v7xv).
    Boolean deniedByHierarchy = hierarchyDenialCache.get(className);
    if (deniedByHierarchy == null) {
      deniedByHierarchy = inheritsAPreciselyDeniedType(className);
      hierarchyDenialCache.put(className, deniedByHierarchy);
    }
    return !deniedByHierarchy;
  }

  /**
   * A pattern that pins a single named type: a bare class name (no trailing wildcard) or a {@code Type$*} nested-type
   * entry. Package wildcards ({@code pkg.*}, {@code pkg.**}) are excluded because they already match every class in
   * that namespace by name, independently of type hierarchy.
   */
  private static boolean isPreciseTypePattern(final String pattern) {
    return !pattern.endsWith("*") || pattern.endsWith("$*");
  }

  /**
   * Resolves {@code className} through this filter's classloader and walks its superclass and interface hierarchy
   * (excluding the class itself, already checked by name), looking for an ancestor whose name matches one of
   * {@link #preciseDenied}. An unresolvable class name (already rejected by every other host-class-lookup surface,
   * since GraalVM cannot load it either) is not treated as inheriting anything denied.
   * <p>
   * This relies on GraalVM resolving {@code Java.type(...)} through the same classloader used here: neither
   * {@code GraalPolyglotEngine} nor its {@code Context.Builder} ever calls {@code hostClassLoader(...)}, so the
   * engine falls back to the embedding application's classloader, matching what is used below. If that ever
   * changes, an unresolvable name would need to fail closed instead of being treated as non-denied.
   */
  private boolean inheritsAPreciselyDeniedType(final String className) {
    final Class<?> resolved;
    try {
      resolved = Class.forName(className, false, HostClassLookupFilter.class.getClassLoader());
    } catch (final ClassNotFoundException | LinkageError ignored) {
      // Does not resolve to a real class - the only expected failure shape for a name that already passed the
      // by-name checks above. A broader catch would also swallow an unrelated VM error (e.g. OutOfMemoryError)
      // as "not denied", which is not a safe default for a security check.
      return false;
    }

    final Set<Class<?>> visited = new HashSet<>();
    final Deque<Class<?>> toVisit = new ArrayDeque<>();
    addAncestors(toVisit, resolved);

    while (!toVisit.isEmpty()) {
      final Class<?> ancestor = toVisit.poll();
      if (!visited.add(ancestor))
        continue;

      final String ancestorName = ancestor.getName();
      for (final String pattern : preciseDenied)
        if (matches(ancestorName, pattern))
          return true;

      addAncestors(toVisit, ancestor);
    }
    return false;
  }

  private static void addAncestors(final Deque<Class<?>> toVisit, final Class<?> type) {
    final Class<?> superclass = type.getSuperclass();
    if (superclass != null)
      toVisit.add(superclass);
    for (final Class<?> iface : type.getInterfaces())
      toVisit.add(iface);
  }

  /**
   * Literal (non-regex) match of a fully qualified class name against one allow/deny pattern. A trailing {@code **}
   * matches any remaining characters, a trailing {@code *} matches the rest of the current package level only (no
   * further {@code .} separator), and a pattern without wildcard must be the class name itself.
   */
  public static boolean matches(final String className, final String pattern) {
    if (pattern == null || pattern.isEmpty())
      return false;

    if (pattern.endsWith("**"))
      return className.startsWith(pattern.substring(0, pattern.length() - 2));

    if (pattern.endsWith("*")) {
      final int prefixLength = pattern.length() - 1;
      if (className.length() <= prefixLength || !className.regionMatches(0, pattern, 0, prefixLength))
        return false;
      // SUBPACKAGES ARE NOT INCLUDED BY A SINGLE '*'
      return className.indexOf('.', prefixLength) < 0;
    }

    return className.equals(pattern);
  }
}
