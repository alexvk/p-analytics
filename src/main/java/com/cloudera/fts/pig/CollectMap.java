/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.fts.pig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;

/**
 * Collect maps from a bag of tuples of maps. The order does matter.
 * 
 * <pre>
 * Example: {([three#1]),([one#1,two#2]),([three#3])} --> [one#1,two#2,three#3]
 * </pre>
 * 
 * If input bag is null, this UDF will return null.
 * 
 */
public class CollectMap extends EvalFunc<Map<String, Object>> {

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> exec(Tuple inputTuple) throws IOException {

    if (inputTuple.size() != 1) {
      throw new ExecException("Expecting 1 input, found " + inputTuple.size(), PigException.INPUT);
    }

    if (inputTuple.get(0) == null) {
      return null;
    }

    if (!(inputTuple.get(0) instanceof DataBag)) {
      throw new ExecException("Usage CollectMap(DataBag of Maps)", PigException.INPUT);
    }

    DataBag inputBag = (DataBag) (inputTuple.get(0));
    try {
      Map<String, Object> outputMap = Maps.newHashMap();

      for (Tuple t : inputBag) {
        if (t != null) {
          for (int i = 0; i < t.size(); i++) {
            if (t.get(i) != null) {
              if (!(t.get(i) instanceof Map<?, ?>)) {
                throw new ExecException("The tuples should contain maps, got a " + t.get(i).getClass().getCanonicalName(),
                    PigException.INPUT);
              }

              // TODO: Decide what to do if the value type does not match

              outputMap.putAll((Map<String, Object>) t.get(i));
            }
          }
        }
      }
      return outputMap;
    } catch (Exception e) {
      String msg = "Encourntered error while collecting maps in " + this.getClass().getCanonicalName();
      throw new ExecException(msg, PigException.BUG, e);
    }
  }

  @Override
  public Schema outputSchema(Schema inputSchema) {
    try {
      if ((inputSchema == null) || inputSchema.size() != 1) {
        throw new RuntimeException("Expecting 1 input, found " + ((inputSchema == null) ? 0 : inputSchema.size()));
      }

      Schema.FieldSchema inputFieldSchema = inputSchema.getField(0);
      if (inputFieldSchema.type != DataType.BAG) {
        throw new RuntimeException("Expecting a bag of tuples: {()}");
      }

      // first field in the bag schema
      Schema.FieldSchema firstFieldSchema = inputFieldSchema.schema.getField(0);
      if ((firstFieldSchema == null) || (firstFieldSchema.schema == null) || firstFieldSchema.type != DataType.TUPLE
          || firstFieldSchema.schema.size() < 1
          || firstFieldSchema.schema.getField(0).type != DataType.MAP) {
        throw new RuntimeException("Expecting a tuple of maps: {(k#v)}, found: " + inputSchema);
      }

      // TODO: Add a check that all elements in the tuple are maps of compatible
      // type

      return new Schema(firstFieldSchema.schema.getField(0));
    } catch (FrontendException e) {
      e.printStackTrace();
      return null;
    }
  }

}
