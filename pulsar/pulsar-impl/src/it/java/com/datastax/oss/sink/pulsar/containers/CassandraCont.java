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
package com.datastax.oss.sink.pulsar.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.script.ScriptException;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.delegate.CassandraDatabaseDelegate;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

/**
 * Extension to avoid {@link org.testcontainers.ext.ScriptUtils.ScriptLoadException}.
 *
 * @param <Self>
 */
public class CassandraCont<Self extends CassandraContainer<Self>> extends CassandraContainer<Self> {
  public CassandraCont(String dockerImageName) {
    super(dockerImageName);
  }

  @Override
  protected void containerIsStarted(InspectContainerResponse containerInfo) {
    if (this.initScript != null) {
      try {
        URL resource = getClass().getResource(this.initScript);
        if (resource == null) {
          this.logger().warn("Could not load classpath init script: {}", this.initScript);
          throw new ScriptUtils.ScriptLoadException(
              "Could not load classpath init script: " + this.initScript + ". Resource not found.");
        }

        String cql = IOUtils.toString(resource, StandardCharsets.UTF_8);
        DatabaseDelegate databaseDelegate = new CassandraDatabaseDelegate(this);
        ScriptUtils.executeDatabaseScript(databaseDelegate, this.initScript, cql);
      } catch (IOException var4) {
        this.logger().warn("Could not load classpath init script: {}", this.initScript);
        throw new ScriptUtils.ScriptLoadException(
            "Could not load classpath init script: " + this.initScript, var4);
      } catch (ScriptException var5) {
        this.logger().error("Error while executing init script: {}", this.initScript, var5);
        throw new ScriptUtils.UncategorizedScriptException(
            "Error while executing init script: " + this.initScript, var5);
      }
    }
  }

  private String initScript;

  @Override
  public Self withInitScript(String initScript) {
    this.initScript = initScript;
    return this.self();
  }
}
