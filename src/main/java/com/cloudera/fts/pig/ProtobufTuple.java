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
package com.cloudera.fts.pig;

import java.util.Iterator;
import java.util.List;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

/**
 * This class wraps a protocol buffer message and attempts to delay parsing until individual
 * fields are requested.
 */
@SuppressWarnings("serial")
public class ProtobufTuple extends AbstractLazyTuple {

  private final Message msg_;
  private final Descriptor descriptor_;
  private final List<FieldDescriptor> fieldDescriptors_;
  private final ProtobufToPig protoConv_;
  private final int protoSize_;

  public ProtobufTuple(Message msg) {
    msg_ = msg;
    descriptor_ = msg.getDescriptorForType();
    fieldDescriptors_ = descriptor_.getFields();
    protoSize_ = fieldDescriptors_.size();
    protoConv_ = new ProtobufToPig();
    initRealTuple(protoSize_);
  }

  @Override
  protected Object getObjectAt(int idx) {
    FieldDescriptor fieldDescriptor = fieldDescriptors_.get(idx);
    Object fieldValue = msg_.getField(fieldDescriptor);
    return protoConv_.fieldToPig(fieldDescriptor, fieldValue);
  }

  @Override
  public long getMemorySize() {
    // The protobuf estimate is obviously inaccurate.
    return msg_.getSerializedSize() + realTuple.getMemorySize();
  }

  @Override
  public Iterator<Object> iterator() {
    // TODO Auto-generated method stub
    return null;
  }
}