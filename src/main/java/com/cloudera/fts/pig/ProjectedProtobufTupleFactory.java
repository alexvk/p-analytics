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

import java.util.List;

import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

/**
 * A tuple factory to create protobuf tuples where
 * only a subset of fields are required.
 */
public class ProjectedProtobufTupleFactory {

  private static TupleFactory tf  = TupleFactory.getInstance();

  private final List<FieldDescriptor> requiredFields;
  private final ProtobufToPig protoConv;

  public ProjectedProtobufTupleFactory(Message protoInstance, RequiredFieldList requiredFieldList) {

    List<FieldDescriptor> protoFields = protoInstance.getDescriptorForType().getFields();
    protoConv = new ProtobufToPig();

    if (requiredFieldList != null) {
      List<RequiredField> tupleFields = requiredFieldList.getFields();
      requiredFields = Lists.newArrayListWithCapacity(tupleFields.size());

      // should we handle nested projections?
      for(RequiredField f : tupleFields) {
        requiredFields.add(protoFields.get(f.getIndex()));
      }
    } else {
      requiredFields = protoFields;
    }
  }

  public Tuple newTuple(Message msg) throws ExecException {
    int size = requiredFields.size();
    Tuple tuple = tf.newTuple(size);

    for(int i=0; i < size; i++) {
      FieldDescriptor fdesc = requiredFields.get(i);
      Object value = msg.getField(fdesc);
      tuple.set(i, protoConv.fieldToPig(fdesc, value));
    }
    return tuple;
  }
}
