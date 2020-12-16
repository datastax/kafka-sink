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

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.driver.core.Session;
import com.datastax.oss.sink.pulsar.BaseSink;
import com.datastax.oss.sink.pulsar.BytesSink;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.datastax.oss.sink.util.Tuple2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public abstract class ContainersBase {

  private static Network network = Network.newNetwork();

  protected static String pulsarInternalHost = "puls";
  protected static String cassanraInternalHost = "cass";
  protected static String keyspace = "testks";

  protected static String sinkPkg = "/pulsar/connectors/sink.nar";
  protected static String sinkPkgUrl = "file:" + sinkPkg;

  protected static PulsarContainer pulsar;

  protected static CassandraContainer cassandra;

  protected static PulsarAdmin pulsarAdmin;
  protected static PulsarClient pulsarClient;
  protected static Session cassandraSession;

  protected static void initClients() throws PulsarClientException {
    pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();
    pulsarClient = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
    cassandraSession = cassandra.getCluster().connect(keyspace);
  }

  protected static void releaseClients() throws PulsarClientException {
    pulsarAdmin.close();
    pulsarClient.close();
    cassandraSession.close();
  }

  private static Map<String, Object> defaultSinkConfig;

  static {
    InputStream is =
        ContainersBase.class.getClassLoader().getResourceAsStream("frompom.properties");
    Properties properties = new Properties();
    File nar = new File("target/cassandra-sink-pulsar-1.0.0-SNAPSHOT.nar");
    try {
      properties.load(is);
      nar =
          new File(
              properties.getProperty("build.dir") + File.separator + properties.getProperty("nar"));
    } catch (IOException e) {
      e.printStackTrace();
    }

    pulsar =
        new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0-SNAPSHOT"))
            .withImagePullPolicy(PullPolicy.defaultPolicy())
            .withFunctionsWorker()
            .withCopyFileToContainer(MountableFile.forHostPath(nar.getAbsolutePath()), sinkPkg)
            .withNetwork(network)
            .withNetworkAliases(pulsarInternalHost);
    pulsar.start();

    cassandra =
        (CassandraContainer)
            new CassandraCont("cassandra:3.11.2")
                .withInitScript("/script.cql")
                .withImagePullPolicy(PullPolicy.defaultPolicy())
                .withStartupTimeout(Duration.ofSeconds(120))
                .withStartupAttempts(1)
                .withNetwork(network)
                .withNetworkAliases(cassanraInternalHost);
    cassandra.start();

    try {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      //noinspection unchecked
      defaultSinkConfig =
          mapper.readValue(ContainersBase.class.getResourceAsStream("/config.yaml"), Map.class);
    } catch (Exception ex) {
      throw new RuntimeException("could not read default config", ex);
    }
  }

  protected static Map<String, Object> defaultSinkConfig() {
    return ConfigUtil.copy(defaultSinkConfig);
  }

  protected abstract String basicName();

  protected abstract Class<? extends BaseSink> sinkClass();

  protected String name(String string) {
    return String.format("%s-%s", basicName(), string);
  }

  protected static void registerSink(
      Map<String, Object> config, String name, Class<? extends BaseSink> sinkClass)
      throws PulsarAdminException {
    registerSink(config, name, name, sinkClass);
  }

  protected static void registerSink(
      Map<String, Object> config, String name, String topics, Class<? extends BaseSink> sinkClass)
      throws PulsarAdminException {
    ConfigUtil.printMap(config);
    pulsarAdmin
        .sinks()
        .createSinkWithUrl(
            SinkConfig.builder()
                .name(name)
                .tenant("public")
                .namespace("default")
                .className(sinkClass.getName())
                .inputs(Arrays.asList(topics.split("\\s*,\\s*")))
                .configs(config)
                .build(),
            sinkPkgUrl);
  }

  protected static void unregisterSink(String name) throws PulsarAdminException {
    pulsarAdmin.sinks().deleteSink("public", "default", name);
  }

  protected void regSink(
          String name, String table, String mapping, Tuple2<String, Object>... configExt)
          throws PulsarAdminException {
    Map<String, Object> config = defaultSinkConfig();
    config.put("topics", name);
    if (table != null)
      ConfigUtil.rename(config, "topic.mytopic.testks.testtbl", table);
    else
      table = "testtbl";
    ConfigUtil.rename(config, "topic.mytopic", name);
    Map<String, Object> ext =
            Arrays.stream(configExt)
                    .collect(HashMap::new, (m, t) -> m.put(t._1, t._2), HashMap::putAll);
    if (mapping != null)
      ext.put("topic." + name + ".testks." + table + ".mapping", mapping);
    registerSink(ConfigUtil.extend(config, ext), name, sinkClass());
    waitForReadySink(name);
  }


  protected void waitForReadySink(String sinkName) {
    Failsafe.with(checkPolicy)
        .run(
            () -> {
              SinkStatus status = pulsarAdmin.sinks().getSinkStatus("public", "default", sinkName);
              assertTrue(status.getInstances().get(0).getStatus().isRunning());
            });
  }

  private Map<String, Integer> lastMessageNum = new HashMap<>();

  protected int lastMessageNum(String sinkName) {
    return lastMessageNum.computeIfAbsent(sinkName, k -> 0);
  }

  protected void waitForProcessedMessages(String sinkName, int messageNum) {
    Failsafe.with(checkPolicy)
        .run(
            () -> {
              SinkStatus status = pulsarAdmin.sinks().getSinkStatus("public", "default", sinkName);
              assertEquals(
                  messageNum, status.getInstances().get(0).getStatus().getNumReadFromPulsar());
              assertEquals(
                  messageNum, status.getInstances().get(0).getStatus().getNumWrittenToSink());
              lastMessageNum.put(sinkName, messageNum);
            });
  }

  protected static final RetryPolicy<?> checkPolicy =
      new RetryPolicy<>().withDelay(Duration.ofSeconds(3)).withMaxRetries(5);

  protected String topic(String shortName) {
    return "persistent://public/default/" + shortName;
  }

  protected org.apache.avro.generic.GenericRecord statrec;

  {
    statrec =
        new GenericData.Record(
            SchemaBuilder.record("test")
                .fields()
                .optionalString("part")
                .optionalString("id")
                .requiredInt("number")
                .requiredBoolean("isfact")
                .endRecord());
    statrec.put("part", "P1");
    statrec.put("id", UUID.randomUUID().toString());
    statrec.put("number", 345);
    statrec.put("isfact", true);
  }

  protected static final ObjectMapper mapper = new ObjectMapper();

  protected static final JsonNode statNode =
      mapper
          .createObjectNode()
          .put("part", "P1")
          .put("id", UUID.randomUUID().toString())
          .put("number", 345)
          .put("isfact", true);

  protected static void runScript(String script, Session session) throws IOException {
    URL url = ContainersBase.class.getResource(script);
    if (url == null) throw new IllegalArgumentException(script);
    String cql = Resources.toString(url, Charset.forName("UTF-8"));
    cql = cql.replaceAll("(?m)//.*$", "");
    String[] statements = cql.split(";");
    for (String statement : statements) {
      statement = statement.trim();
      if (statement.isEmpty()) continue;
      session.execute(statement);
    }
  }

  public static class Pojo {
    private String part;
    private String id;
    private int number;
    private boolean isfact;

    public String getPart() {
      return part;
    }

    public void setPart(String part) {
      this.part = part;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getNumber() {
      return number;
    }

    public void setNumber(int number) {
      this.number = number;
    }

    public boolean isIsfact() {
      return isfact;
    }

    public void setIsfact(boolean isfact) {
      this.isfact = isfact;
    }
  }
}
