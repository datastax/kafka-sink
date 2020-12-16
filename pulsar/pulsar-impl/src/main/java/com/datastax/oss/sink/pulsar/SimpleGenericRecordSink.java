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
package com.datastax.oss.sink.pulsar;

import com.datastax.oss.sink.EngineAPIAdapter;
import com.datastax.oss.sink.RecordProcessor;
import com.datastax.oss.sink.RetriableException;
import com.datastax.oss.sink.config.ConfigException;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.datastax.oss.sink.record.SchemaSupport;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
        name = "dssc-simple",
        type = IOType.SINK,
        help = "DataStax Pulsar Sink is used for moving messages from Pulsar to Cassandra",
        configClass = PulsarSinkConfig.class
)
public class SimpleGenericRecordSink implements Sink<GenericRecord> {

    private static class TemplateBasedSchema {

        private static TemplateBasedSchema ofPrimitive(Object value) {
            if (value == null) {
                return new TemplateBasedSchema(SchemaSupport.Type.NULL);
            } 
            if (value instanceof Integer) {
                return new TemplateBasedSchema(SchemaSupport.Type.INT32);
            }
            if (value instanceof String) {
                return new TemplateBasedSchema(SchemaSupport.Type.STRING);
            }
            if (value instanceof Long) {
                return new TemplateBasedSchema(SchemaSupport.Type.INT64);
            }
            throw new UnsupportedOperationException("type "+value.getClass());
        }
        private Map<String, TemplateBasedSchema> fields;
        private SchemaSupport.Type type;

        public TemplateBasedSchema(GenericRecord template) {
            this.fields = new ConcurrentHashMap<>();
            update(template);
                
            this.type = SchemaSupport.Type.STRUCT;
        }

        private TemplateBasedSchema(SchemaSupport.Type type) {
            this.type = type;
            this.fields = Collections.emptyMap();
        }
        public TemplateBasedSchema getField(String name) {
            return fields.get(name);
        }
        public final void update(GenericRecord template) {
            for (Field f : template.getFields()) {
                Object value = template.getField(f);
                
                log.info("update field "+f.getName()+" -> "+value);
                if (value != null) {
                    TemplateBasedSchema schemaForField = TemplateBasedSchema.ofPrimitive(value);
                    this.fields.put(f.getName(), schemaForField);
                } else {
                    TemplateBasedSchema schemaForField = TemplateBasedSchema.ofPrimitive("NUMMY_STRING");
                    this.fields.put(f.getName(), schemaForField);
                }
            }
        }

        @Override
        public String toString() {
            return "TemplateBasedSchema{" + "fields=" + fields + ", type=" + type + '}';
        }

        private Set<String> getFields() {
            return fields.keySet();
        }
        
    }
    
    private ConcurrentHashMap<String, TemplateBasedSchema> schematas = new ConcurrentHashMap<>();
    
    private EngineAPIAdapter<Record<GenericRecord>, ?, ?, ?, Object> adapter
            = new EngineAPIAdapter<Record<GenericRecord>, TemplateBasedSchema, Record<GenericRecord>, Object, Object>() {
        @Override
        public RuntimeException adapt(ConfigException ex) {
            throw ex;
        }

        @Override
        public RuntimeException adapt(RetriableException ex) {
            throw ex;
        }

        @Override
        public String topic(Record<GenericRecord> record) {
            return shortTopic(record);
        }

        @Override
        public Object key(Record<GenericRecord> record) {
            return record.getKey().orElse(null);
        }

        @Override
        public Object value(Record record) {
            return record;
        }

        @Override
        public Set<Object> headers(Record<GenericRecord> record) {
            return (Set<Object>) (Set) record.getProperties().entrySet();
        }

        @Override
        public String headerKey(Object header) {
            return ((Map.Entry) header).getKey().toString();
        }

        @Override
        public Object headerValue(Object header) {
            return ((Map.Entry) header).getValue();
        }

        @Override
        public TemplateBasedSchema headerSchema(Object header) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Long timestamp(Record<GenericRecord> record) {
            return record.getEventTime().orElse((Long) null);
        }

        @Override
        public boolean isStruct(Object object) {
            log.info("isStruct ? "+object);
            return object instanceof GenericRecord || object instanceof Record;
        }

        @Override
        public TemplateBasedSchema schema(Record<GenericRecord> struct) {
            TemplateBasedSchema res =  ensureAndUpdateSchema(struct);
            return res;
        }

      

        @Override
        public Set<String> fields(Record<GenericRecord> struct) {
           return ensureAndUpdateSchema(struct).getFields();
        }

        @Override
        public Object fieldValue(Record<GenericRecord> struct, String fieldName) {
            Object res =  struct.getValue().getField(fieldName);
            log.info("fieldValue for "+struct+" fieldName "+fieldName+" = "+res);
            return res;
        }

        @Override
        public TemplateBasedSchema fieldSchema(TemplateBasedSchema schema, String fieldName) {
            TemplateBasedSchema res =  schema.getField(fieldName);
            log.info("fieldSchema in "+schema+" fieldNAme "+fieldName+" -> "+res);
            return res;
        }

        @Override
        public SchemaSupport.Type type(TemplateBasedSchema schema) {
            return schema.type;
        }

        @Override
        public TemplateBasedSchema valueSchema(TemplateBasedSchema schema) {
            return schema;
        }

        @Override
        public TemplateBasedSchema keySchema(TemplateBasedSchema schema) {
            // TODO: support KeyValueSchematas
            return TemplateBasedSchema.ofPrimitive("STRING!");
        }

        @Override
        public Class<Record<GenericRecord>> structClass() {
            return (Class) Record.class;
        }

    };

      private TemplateBasedSchema ensureAndUpdateSchema(Record<GenericRecord> struct) {
          String schemaDef = struct.getSchema().getSchemaInfo().getSchemaDefinition();
          TemplateBasedSchema res =  schematas.computeIfAbsent(schemaDef,
                    s -> {
                        return new TemplateBasedSchema(struct.getValue());
                    });
            // need to recover nulls from previous records
            res.update(struct.getValue());
            return res;
        }
    private static final Logger log = LoggerFactory.getLogger(SimpleGenericRecordSink.class);
    protected RecordProcessor<Record<GenericRecord>, Object> processor;

    @Override
    public void open(Map<String, Object> cfg, SinkContext sc) throws Exception {
        log.info("start {}, config {}", getClass().getName(), cfg);
        try {
            log.debug("starting processor");
            processor = new RecordProcessor<Record<GenericRecord>, Object>() {
                @Override
                public EngineAPIAdapter<Record<GenericRecord>, ?, ?, ?, Object> apiAdapter() {
                    return adapter;
                }

                @Override
                protected void beforeStart(Map<String, String> config) {
                }

                @Override
                protected void onProcessingStart() {
                }

                @Override
                protected void handleFailure(Record record, Throwable e, String cql, Runnable failCounter) {
                    log.error("failed record {} for {}", record, cql, e);
                    record.fail();
                }

                @Override
                protected void handleSuccess(Record record) {
                    record.ack();
                }

                @Override
                public String version() {
                    return "1.0";
                }

                @Override
                public String appName() {
                    return "PulsarSink";
                }

            };

            processor.start(ConfigUtil.flatString(cfg));
            processor
                    .config()
                    .getTopicConfigs()
                    .forEach(
                            (topic, topicConfig) -> {

                            });
            log.debug("started {}", getClass().getName());
        } catch (Throwable ex) {
            log.error("initialization error", ex);
            close();
            throw ex;
        }
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        ensureAndUpdateSchema(record);
        log.info("write " + record);
        for (Field f : record.getValue().getFields()) {
            log.info("field "+f.getIndex()+" "+f.getName()+" -> "+record.getValue().getField(f));
        }
        processor.process(Collections.singleton(record));
    }

    @Override
    public void close() throws Exception {
        if (processor != null) {
            processor.stop();
        }
    }

    public static String shortTopic(Record<?> record) {
        return record.getTopicName().map(s -> s.substring(s.lastIndexOf("/") + 1)).orElse(null);
    }
}
