/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.kafka.sink.spi;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional per-task callback for Cassandra write success and failure. Implementations are loaded
 * only when {@value #CLASS_CONFIG} is set (optionally with {@value #PATH_CONFIG}) so third-party
 * metrics or other observers can be shipped as a separate JAR. When the class name is unset, a
 * no-op is used. A {@code META-INF/services} provider on the plugin classloader is not loaded.
 *
 * <p>Load and callback failures are logged and swallowed so a listener cannot fail the sink task.
 */
public abstract class RecordListener implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RecordListener.class);

  public static final String CLASS_CONFIG = "record.listener.class";
  public static final String PATH_CONFIG = "record.listener.path";

  public void configure(Map<String, String> props) {}

  public abstract void onSuccess(SinkRecord record);

  /**
   * Invoked after a record is not written. {@code cql} is non-null when the Cassandra driver
   * rejected or failed the statement; it is null for mapping/decode failures.
   */
  public abstract void onFailure(SinkRecord record, Throwable error, String cql);

  @Override
  public void close() {}

  /**
   * Create a listener for one sink task. Never throws; returns {@link NoOpRecordListener#INSTANCE}
   * when unset or when loading fails.
   */
  public static RecordListener load(Map<String, String> props, ClassLoader parent) {
    if (props == null) {
      return NoOpRecordListener.INSTANCE;
    }
    String className = blankToNull(props.get(CLASS_CONFIG));
    String path = blankToNull(props.get(PATH_CONFIG));
    // Off unless an implementation is named. Path alone is not enough.
    if (className == null) {
      if (path != null) {
        log.warn("{} is set but {} is blank; record listener disabled", PATH_CONFIG, CLASS_CONFIG);
      }
      return NoOpRecordListener.INSTANCE;
    }
    ClassLoader parentCl = parent != null ? parent : RecordListener.class.getClassLoader();
    URLClassLoader child = null;
    try {
      ClassLoader cl = parentCl;
      if (path != null) {
        URL[] urls = toUrls(path);
        if (urls.length == 0) {
          log.warn(
              "{} did not resolve to any jar or directory; record listener disabled", PATH_CONFIG);
          return NoOpRecordListener.INSTANCE;
        }
        child = new URLClassLoader(urls, parentCl);
        cl = child;
      }
      Class<?> clazz = Class.forName(className, true, cl);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (!(instance instanceof RecordListener)) {
        log.warn("{} is not a {}: {}", CLASS_CONFIG, RecordListener.class.getName(), className);
        closeQuietly(child);
        return NoOpRecordListener.INSTANCE;
      }
      RecordListener listener = (RecordListener) instance;
      RecordListener configured = configureSafe(listener, props, cl);
      if (configured == NoOpRecordListener.INSTANCE) {
        closeQuietly(child);
        return configured;
      }
      if (child != null) {
        return new ContextClassLoaderRecordListener(configured, cl, child);
      }
      return new ContextClassLoaderRecordListener(configured, cl);
    } catch (Throwable t) {
      if (t instanceof VirtualMachineError || t instanceof ThreadDeath) {
        throw (Error) t;
      }
      log.warn("Failed to load RecordListener {}; continuing without one", className, t);
      closeQuietly(child);
      return NoOpRecordListener.INSTANCE;
    }
  }

  public static void invokeSuccess(RecordListener listener, SinkRecord record) {
    if (listener == null) {
      return;
    }
    try {
      listener.onSuccess(record);
    } catch (RuntimeException e) {
      log.warn("RecordListener.onSuccess failed", e);
    }
  }

  public static void invokeFailure(
      RecordListener listener, SinkRecord record, Throwable error, String cql) {
    if (listener == null) {
      return;
    }
    try {
      listener.onFailure(record, error, cql);
    } catch (RuntimeException e) {
      log.warn("RecordListener.onFailure failed", e);
    }
  }

  public static void invokeClose(RecordListener listener) {
    if (listener == null) {
      return;
    }
    try {
      listener.close();
    } catch (RuntimeException e) {
      log.warn("RecordListener.close failed", e);
    }
  }

  private static RecordListener configureSafe(
      RecordListener listener, Map<String, String> props, ClassLoader cl) {
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    try {
      if (cl != null) {
        Thread.currentThread().setContextClassLoader(cl);
      }
      listener.configure(props);
      return listener;
    } catch (RuntimeException e) {
      log.warn("RecordListener.configure failed; continuing without one", e);
      invokeClose(listener);
      return NoOpRecordListener.INSTANCE;
    } finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  private static URL[] toUrls(String path) throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();
    String[] parts = path.split(",");
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i].trim();
      if (part.isEmpty()) {
        continue;
      }
      File file = new File(part);
      if (!file.exists()) {
        log.warn("{} entry does not exist: {}", PATH_CONFIG, file.getAbsolutePath());
        continue;
      }
      if (file.isFile()) {
        urls.add(file.toURI().toURL());
        continue;
      }
      urls.add(file.toURI().toURL());
      File[] jars = file.listFiles();
      if (jars == null) {
        continue;
      }
      for (int j = 0; j < jars.length; j++) {
        File jar = jars[j];
        if (jar.isFile() && jar.getName().endsWith(".jar")) {
          urls.add(jar.toURI().toURL());
        }
      }
    }
    return urls.toArray(new URL[0]);
  }

  private static String blankToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static void closeQuietly(URLClassLoader child) {
    if (child == null) {
      return;
    }
    try {
      child.close();
    } catch (Exception e) {
      log.warn("Failed to close RecordListener classloader", e);
    }
  }

  /** Listener that does nothing. Shared singleton. */
  public static final class NoOpRecordListener extends RecordListener {
    public static final NoOpRecordListener INSTANCE = new NoOpRecordListener();

    private NoOpRecordListener() {}

    @Override
    public void onSuccess(SinkRecord record) {}

    @Override
    public void onFailure(SinkRecord record, Throwable error, String cql) {}
  }

  private static final class ContextClassLoaderRecordListener extends RecordListener {
    private final RecordListener delegate;
    private final ClassLoader classLoader;
    private final URLClassLoader child;

    ContextClassLoaderRecordListener(RecordListener delegate, ClassLoader classLoader) {
      this(delegate, classLoader, null);
    }

    ContextClassLoaderRecordListener(
        RecordListener delegate, ClassLoader classLoader, URLClassLoader child) {
      this.delegate = delegate;
      this.classLoader = classLoader;
      this.child = child;
    }

    @Override
    public void configure(Map<String, String> props) {
      run(
          new Runnable() {
            @Override
            public void run() {
              delegate.configure(props);
            }
          });
    }

    @Override
    public void onSuccess(final SinkRecord record) {
      run(
          new Runnable() {
            @Override
            public void run() {
              delegate.onSuccess(record);
            }
          });
    }

    @Override
    public void onFailure(final SinkRecord record, final Throwable error, final String cql) {
      run(
          new Runnable() {
            @Override
            public void run() {
              delegate.onFailure(record, error, cql);
            }
          });
    }

    @Override
    public void close() {
      try {
        run(
            new Runnable() {
              @Override
              public void run() {
                delegate.close();
              }
            });
      } finally {
        closeQuietly(child);
      }
    }

    private void run(Runnable action) {
      ClassLoader prev = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(classLoader);
        action.run();
      } finally {
        Thread.currentThread().setContextClassLoader(prev);
      }
    }
  }
}
