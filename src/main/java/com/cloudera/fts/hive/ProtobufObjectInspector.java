/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.fts.hive;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

/**
 *
 */
public class ProtobufObjectInspector extends SettableStructObjectInspector {

  private static final String _comment = "EventBI has no comments";

  private static class ProtoField implements StructField {

    private final FieldDescriptor fd;
    private final ObjectInspector objectInspector;

    public ProtoField(FieldDescriptor fd, ObjectInspector objectInspector) {
      this.fd = fd;
      this.objectInspector = objectInspector;
    }

    public FieldDescriptor getFieldDescriptor() {
      return fd;
    }

    @Override
    public String getFieldName() {
      return fd.getName();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return objectInspector;
    }

    @Override
    public String getFieldComment() {
      return fd.getFullName() + _comment;
    }
  }

  public static ObjectInspector get(Descriptor descriptor) {
    if (INSPECTOR_CACHE.containsKey(descriptor)) {
      return INSPECTOR_CACHE.get(descriptor);
    }
    // Create a new instance first; cache the instance to support recursion.
    ProtobufObjectInspector poi = new ProtobufObjectInspector();
    INSPECTOR_CACHE.put(descriptor, poi);
    poi.init(descriptor);
    return poi;
  }

  private static final Map<Descriptor, ProtobufObjectInspector> INSPECTOR_CACHE = Maps.newHashMap();

  private static ObjectInspector getObjectInspector(FieldDescriptor fd) {
    ObjectInspector oi = null;
    switch (fd.getJavaType()) {
    case STRING:
      oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
      break;
    case INT:
      oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
      break;
    case LONG:
      oi = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
      break;
    case FLOAT:
      oi = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
      break;
    case DOUBLE:
      oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
      break;
    case BOOLEAN:
      oi = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
      break;
    case MESSAGE:
      oi = get(fd.getMessageType());
      break;
    default:
      throw new UnsupportedOperationException("No support for java type: " + fd.getJavaType());
    }
    if (fd.isRepeated()) {
      return ObjectInspectorFactory.getStandardListObjectInspector(oi);
    } else {
      return oi;
    }
  }

  private Descriptor descriptor;
  private List<ProtoField> fields;

  private ProtobufObjectInspector() {
  }

  private void init(Descriptor descriptor) {
    this.descriptor = descriptor;
    List<FieldDescriptor> protoFields = descriptor.getFields();
    this.fields = Lists.newArrayListWithCapacity(protoFields.size());
    for (int i = 0; i < protoFields.size(); i++) {
      FieldDescriptor fd = protoFields.get(i);
      this.fields.add(new ProtoField(fd, getObjectInspector(fd)));
    }
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return descriptor.getName();
  }

  @Override
  public Object create() {
    return null;
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object value) {
    ProtoField pf = (ProtoField) field;
    return ((Message) data).toBuilder().setField(pf.getFieldDescriptor(), value).build();
  }

  @Override
  public Object getStructFieldData(Object data, StructField field) {
    return ((Message) data).getField(((ProtoField) field).getFieldDescriptor());
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    List<Object> result = Lists.newArrayListWithCapacity(fields.size());
    for (ProtoField pf : fields) {
      result.add(((Message) data).getField(pf.getFieldDescriptor()));
    }
    return result;
  }
}