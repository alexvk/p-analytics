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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

/*
 * This UDF converts array of bytes into a string.
 *
 */

@Description(name = "to_string",
             value = "_FUNC_(bytes) - Returns the string representation of byte array.",
    extended = "Example:\n  > SELECT _FUNC_(bytes) FROM messages LIMIT 1;\n")
public class TestUDF extends GenericUDF {

  private static final int BYTES_IDX = 0;
  private static final int ARG_COUNT = 1; // Number of arguments to this UDF

  // External Name (for explain)
  private static final String FUNC_NAME = "TO_STRING";

  final static Text retString = new Text("N/A");
  final static Object[] retValue = { retString };

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {

    if (args.length != ARG_COUNT) {
        throw new UDFArgumentException(FUNC_NAME + "() takes one argument");
    }

    if (!args[BYTES_IDX].getCategory().equals(Category.LIST)) {
      throw new UDFArgumentException(
          FUNC_NAME + "() first argument should be a byte array");
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("message");
    fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    retString.set("N/A");
    if (arguments[BYTES_IDX].get() != null) {
      List<Byte> byteList = (List<Byte>) arguments[BYTES_IDX].get();
      byte[] bytes = new byte[byteList.size()];
      for (int i = 0; i < byteList.size(); i++) {
        bytes[i] = byteList.get(i);
      }
      retString.set(new String(bytes));
    }
    return retValue;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);

    final StringBuilder sb = new StringBuilder();
    sb.append(FUNC_NAME);
    sb.append("(");
    sb.append(Joiner.on(",").join(children));
    sb.append(")");

    return sb.toString();
  }
}
