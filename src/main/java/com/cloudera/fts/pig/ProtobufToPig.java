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
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

/**
 * A class for turning codegen'd protos into Pig Tuples and Schemas for custom
 * Pig LoadFuncs.
 * 
 */
public class ProtobufToPig {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufToPig.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  private static BagFactory bagFactory_ = BagFactory.getInstance();

  public ProtobufToPig() {
  }

  /**
   * Turn a generic message into a Tuple.  Individual fields that are enums
   * are converted into their string equivalents.  Fields that are not filled
   * out in the protobuf are set to null, unless there is a default field value in
   * which case that is used instead.
   * @param msg the protobuf message
   * @return a pig tuple representing the message.
   */
  public Tuple toTuple(Message msg) {
    if (msg == null) {
      // Pig tuples deal gracefully with nulls.
      // Also, we can be called with null here in recursive calls.
      return null;
    }

    Descriptor msgDescriptor = msg.getDescriptorForType();
    Tuple tuple = tupleFactory_.newTuple(msgDescriptor.getFields().size());
    int curField = 0;
    try {
      // Walk through all the possible fields in the message.
      for (FieldDescriptor fieldDescriptor : msgDescriptor.getFields()) {
        // Get the set value, or the default value, or null.
        Object fieldValue = getFieldValue(msg, fieldDescriptor);

        if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
          tuple.set(curField++, messageToTuple(fieldDescriptor, fieldValue));
        } else {
          tuple.set(curField++, singleFieldToTuple(fieldDescriptor, fieldValue));
        }
      }
    } catch (ExecException e) {
      LOG.warn("Could not convert msg " + msg + " to tuple", e);
    }

    return tuple;
  }

  /**
   * Returns either {@link #messageToTuple(FieldDescriptor, Object)}
   * or {@link #singleFieldToTuple(FieldDescriptor, Object)} depending
   * on whether the field is a Message or a simple field.
   */
  protected Object fieldToPig(FieldDescriptor fieldDescriptor, Object fieldValue) {
    if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
      return messageToTuple(fieldDescriptor, fieldValue);
    } else {
      return singleFieldToTuple(fieldDescriptor, fieldValue);
    }
  }

  /**
   * Translate a nested message to a tuple.  If the field is repeated, it walks the list and adds each to a bag.
   * Otherwise, it just adds the given one.
   * @param fieldDescriptor the descriptor object for the given field.
   * @param fieldValue the object representing the value of this field, possibly null.
   * @return the object representing fieldValue in Pig -- either a bag or a tuple.
   */
  @SuppressWarnings("unchecked")
  protected Object messageToTuple(FieldDescriptor fieldDescriptor, Object fieldValue) {
    assert fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE : "messageToTuple called with field of type " + fieldDescriptor.getType();

    if (fieldDescriptor.isRepeated()) {
      // The protobuf contract is that if the field is repeated, then the object returned is actually a List
      // of the underlying datatype, which in this case is a nested message.
      List<Message> messageList = (List<Message>) (fieldValue != null ? fieldValue : Lists.newArrayList());
      DataBag bag = bagFactory_.newDefaultBag();
      for (Message m : messageList) {
        bag.add(new ProtobufTuple(m));
      }
      return bag;
    } else {
      return new ProtobufTuple((Message)fieldValue);
    }
  }

  /**
   * Translate a single field to a tuple.  If the field is repeated, it walks the list and adds each to a bag.
   * Otherwise, it just adds the given one.
   * @param fieldDescriptor the descriptor object for the given field.
   * @param fieldValue the object representing the value of this field, possibly null.
   * @return the object representing fieldValue in Pig -- either a bag or a single field.
   * @throws ExecException if Pig decides to.  Shouldn't happen because we won't walk off the end of a tuple's field set.
   */
  @SuppressWarnings("unchecked")
  protected Object singleFieldToTuple(FieldDescriptor fieldDescriptor, Object fieldValue) {
    assert fieldDescriptor.getType() != FieldDescriptor.Type.MESSAGE : "messageToFieldSchema called with field of type " + fieldDescriptor.getType();

    if (fieldDescriptor.isRepeated()) {
      // The protobuf contract is that if the field is repeated, then the object returned is actually a List
      // of the underlying datatype, which in this case is a "primitive" like int, float, String, etc.
      // We have to make a single-item tuple out of it to put it in the bag.
      DataBag bag = bagFactory_.newDefaultBag();
      List<Object> fieldValueList = (List<Object>) (fieldValue != null ? fieldValue : Lists.newArrayList());
      for (Object singleFieldValue : fieldValueList) {
        Object nonEnumFieldValue = coerceToPigTypes(fieldDescriptor, singleFieldValue);
        Tuple innerTuple = tupleFactory_.newTuple(1);
        try {
          innerTuple.set(0, nonEnumFieldValue);
        } catch (ExecException e) { // not expected
          throw new RuntimeException(e);
        }
        bag.add(innerTuple);
      }
      return bag;
    } else {
      return coerceToPigTypes(fieldDescriptor, fieldValue);
    }
  }

  /**
   * If the given field value is an enum, translate it to the enum's name as a string, since Pig cannot handle enums.
   * Also, if the given field value is a bool, translate it to 0 or 1 to avoid Pig bools, which can be sketchy.
   * @param fieldDescriptor the descriptor object for the given field.
   * @param fieldValue the object representing the value of this field, possibly null.
   * @return the object, unless it was from an enum field, in which case we return the name of the enum field.
   */
  private Object coerceToPigTypes(FieldDescriptor fieldDescriptor, Object fieldValue) {
    if (fieldDescriptor.getType() == FieldDescriptor.Type.ENUM && fieldValue != null) {
      EnumValueDescriptor enumValueDescriptor = (EnumValueDescriptor)fieldValue;
      return enumValueDescriptor.getName();
    } else if (fieldDescriptor.getType() == FieldDescriptor.Type.BOOL && fieldValue != null) {
      Boolean boolValue = (Boolean)fieldValue;
      return new Integer(boolValue ? 1 : 0);
    } else if (fieldDescriptor.getType() == FieldDescriptor.Type.BYTES && fieldValue != null) {
      ByteString bsValue = (ByteString)fieldValue;
      return new DataByteArray(bsValue.toByteArray());
    }
    return fieldValue;
  }

  /**
   * A utility function for getting the value of a field in a protobuf message.  It first tries the
   * literal set value in the protobuf's field list.  If the value isn't set, and the field has a default
   * value, it uses that.  Otherwise, it returns null.
   * @param msg the protobuf message
   * @param fieldDescriptor the descriptor object for the given field.
   * @return the value of the field, or null if none can be assigned.
   */
  protected Object getFieldValue(Message msg, FieldDescriptor fieldDescriptor) {
    Object o = null;
    Map<FieldDescriptor, Object> setFields = msg.getAllFields();
    if (setFields.containsKey(fieldDescriptor)) {
      o = setFields.get(fieldDescriptor);
    } else if (fieldDescriptor.hasDefaultValue()) {
      o = fieldDescriptor.getDefaultValue();
    }

    return o;
  }

  /**
   * Turn a generic message descriptor into a Schema.  Individual fields that are enums
   * are converted into their string equivalents.
   * @param msgDescriptor the descriptor for the given message type.
   * @return a pig schema representing the message.
   */
  public Schema toSchema(Descriptor msgDescriptor) {
    Schema schema = new Schema();

    try {
      // Walk through all the possible fields in the message.
      for (FieldDescriptor fieldDescriptor : msgDescriptor.getFields()) {
        if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
          schema.add(messageToFieldSchema(fieldDescriptor));
        } else {
          schema.add(singleFieldToFieldSchema(fieldDescriptor));
        }
      }
    } catch (FrontendException e) {
      LOG.warn("Could not convert descriptor " + msgDescriptor + " to schema", e);
    }

    return schema;
  }

  /**
   * Turn a nested message into a Schema object.  For repeated nested messages, it generates a schema for a bag of
   * tuples.  For non-repeated nested messages, it just generates a schema for the tuple itself.
   * @param fieldDescriptor the descriptor object for the given field.
   * @return the Schema for the nested message.
   * @throws FrontendException if Pig decides to.
   */
  private FieldSchema messageToFieldSchema(FieldDescriptor fieldDescriptor) throws FrontendException {
    assert fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE : "messageToFieldSchema called with field of type " + fieldDescriptor.getType();

    Schema innerSchema = toSchema(fieldDescriptor.getMessageType());

    if (fieldDescriptor.isRepeated()) {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new FieldSchema(fieldDescriptor.getName() + "_tuple", innerSchema, DataType.TUPLE));
      return new FieldSchema(fieldDescriptor.getName(), tupleSchema, DataType.BAG);
    } else {
      return new FieldSchema(fieldDescriptor.getName(), innerSchema, DataType.TUPLE);
    }
  }

  /**
   * Turn a single field into a Schema object.  For repeated single fields, it generates a schema for a bag of single-item tuples.
   * For non-repeated fields, it just generates a standard field schema.
   * @param fieldDescriptor the descriptor object for the given field.
   * @return the Schema for the nested message.
   * @throws FrontendException if Pig decides to.
   */
  private FieldSchema singleFieldToFieldSchema(FieldDescriptor fieldDescriptor) throws FrontendException {
    assert fieldDescriptor.getType() != FieldDescriptor.Type.MESSAGE : "singleFieldToFieldSchema called with field of type " + fieldDescriptor.getType();

    if (fieldDescriptor.isRepeated()) {
      Schema itemSchema = new Schema();
      itemSchema.add(new FieldSchema(fieldDescriptor.getName(), null, getPigDataType(fieldDescriptor)));
      Schema itemTupleSchema = new Schema();
      itemTupleSchema.add(new FieldSchema(fieldDescriptor.getName() + "_tuple", itemSchema, DataType.TUPLE));

      return new FieldSchema(fieldDescriptor.getName() + "_bag", itemTupleSchema, DataType.BAG);
    } else {
      return new FieldSchema(fieldDescriptor.getName(), null, getPigDataType(fieldDescriptor));
    }
  }

  /**
   * Translate between protobuf's datatype representation and Pig's datatype representation.
   * @param fieldDescriptor the descriptor object for the given field.
   * @return the byte representing the pig datatype for the given field type.
   */
  private byte getPigDataType(FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getType()) {
      case INT32:
      case UINT32:
      case SINT32:
      case FIXED32:
      case SFIXED32:
      case BOOL: // We convert booleans to ints for pig output.
        return DataType.INTEGER;
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case STRING:
      case ENUM: // We convert enums to strings for pig output.
        return DataType.CHARARRAY;
      case BYTES:
        return DataType.BYTEARRAY;
      case MESSAGE:
        throw new IllegalArgumentException("getPigDataType called on field " + fieldDescriptor.getFullName() + " of type message.");
      default:
        throw new IllegalArgumentException("Unexpected field type. " + fieldDescriptor.toString() + " " + fieldDescriptor.getFullName() + " " + fieldDescriptor.getType());
    }
  }

}