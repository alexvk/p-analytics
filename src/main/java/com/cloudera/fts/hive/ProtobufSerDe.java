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

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.Message;

/**
 * A base {@code SerDe} implementation for protocol buffers. Subclasses should
 * provide an actual {@code Type} for the parent constructor.
 */
public class ProtobufSerDe implements SerDe {

  private final Message instance;
  private final ObjectInspector oi;

  /**
   * @param objectType
   * @throws SerDeException
   */
  public ProtobufSerDe(Message instance) throws SerDeException {
    this.instance = instance;
    this.oi = ProtobufObjectInspector.get(instance.getDescriptorForType());
  }

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    Message.Builder mb = instance.newBuilderForType();
    try {
      BytesWritable bw = (BytesWritable) field;
      return mb.mergeFrom(bw.getBytes(), 0, bw.getLength()).build();
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    // do nothing for now.
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  @Override
  public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
    throw new UnsupportedOperationException("We cannot serialize to protocol buffers yet");
  }

  @Override
  public SerDeStats getSerDeStats() {
    SerDeStats stats = new SerDeStats();
    stats.setRawDataSize(instance.getSerializedSize());
    return stats;
  }
}
