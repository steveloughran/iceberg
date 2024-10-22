/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.hadoop.wrappedio;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to assist binding to Hadoop APIs through reflection. Source: {@code
 * org.apache.parquet.hadoop.util.wrapped.io.BindingUtils}.
 */
final class BindingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BindingUtils.class);

  private BindingUtils() {}

  /**
   * Load a class by name.
   *
   * @param className classname
   * @return the class or null if it could not be loaded.
   */
  static Class<?> loadClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      LOG.debug("No class {}", className, e);
      return null;
    }
  }

  /**
   * Load a class by name.
   *
   * @param className classname
   * @return the class.
   * @throws RuntimeException if the class was not found.
   */
  static Class<?> loadClassSafely(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Load a class by name.
   *
   * @param cl classloader to use.
   * @param className classname
   * @return the class or null if it could not be loaded.
   */
  static Class<?> loadClass(ClassLoader cl, String className) {
    try {
      return cl.loadClass(className);
    } catch (ClassNotFoundException e) {
      LOG.debug("No class {}", className, e);
      return null;
    }
  }

  /**
   * Get an invocation from the source class, which will be unavailable() if the class is null or
   * the method isn't found.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or "unavailable"
   */
  static <T> DynMethods.UnboundMethod loadInvocation(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    if (source != null) {
      final DynMethods.UnboundMethod m =
          new DynMethods.Builder(name).impl(source, name, parameterTypes).orNoop().build();
      if (m.isNoop()) {
        // this is a sign of a mismatch between this class's expected
        // signatures and actual ones.
        // log at debug.
        LOG.debug("Failed to load method {} from {}", name, source);
      } else {
        LOG.debug("Found method {} from {}", name, source);
      }
      return m;
    } else {
      return noop(name);
    }
  }

  /**
   * Load a static method from the source class, which will be a noop() if the class is null or the
   * method isn't found. If the class and method are not found, then an {@code
   * IllegalStateException} is raised on the basis that this means that the binding class is broken,
   * rather than missing/out of date.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or a no-op.
   * @throws IllegalStateException if the method is not static.
   */
  static <T> DynMethods.UnboundMethod loadStaticMethod(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    final DynMethods.UnboundMethod method =
        loadInvocation(source, returnType, name, parameterTypes);
    Preconditions.checkState(method.isStatic(), "Method is not static %s", method);
    return method;
  }

  /**
   * Create a no-op method.
   *
   * @param name method name
   * @return a no-op method.
   */
  static DynMethods.UnboundMethod noop(final String name) {
    return new DynMethods.Builder(name).orNoop().build();
  }

  /**
   * Given a sequence of methods, verify that they are all available.
   *
   * @param methods methods
   * @return true if they are all implemented
   */
  static boolean implemented(DynMethods.UnboundMethod... methods) {
    for (DynMethods.UnboundMethod method : methods) {
      if (method.isNoop()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Require a method to be available.
   *
   * @param method method to probe
   * @throws UnsupportedOperationException if the method was not found.
   */
  static void checkAvailable(DynMethods.UnboundMethod method) throws UnsupportedOperationException {
    if (method.isNoop()) {
      throw new UnsupportedOperationException("Unbound " + method);
    }
  }

  /**
   * Is a method available?
   *
   * @param method method to probe
   * @return true iff the method is found and loaded.
   */
  static boolean available(DynMethods.UnboundMethod method) {
    return !method.isNoop();
  }

  /**
   * Invoke the supplier, catching any {@code UncheckedIOException} raised, extracting the inner
   * IOException and rethrowing it.
   *
   * @param <T> type of result
   * @param call call to invoke
   * @return result
   * @throws IOException if the call raised an IOException wrapped by an UncheckedIOException.
   */
  static <T> T extractIOEs(Supplier<T> call) throws IOException {
    try {
      return call.get();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
