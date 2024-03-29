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
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Converts a map into a bag of (key, value) tuples.
 * 
 * <pre>
 * Example: [three#3,one#1,two#2]] --> {(one,1),(two,2),(three,3)}
 * </pre>
 * 
 * If input map is null, this UDF will return null.
 * 
 */
public class MapToBag extends EvalFunc<DataBag> {

  private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
  private static final BagFactory mBagFactory = BagFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException {
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) input.get(0);
      DataBag result = null;
      if (map != null) {
        result = mBagFactory.newDefaultBag();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          Tuple tuple = mTupleFactory.newTuple(2);
          tuple.set(0, entry.getKey());
          tuple.set(1, entry.getValue());
          result.add(tuple);
        }
      }
      return result;
    }
    catch (Exception e) {
      String msg = "Encountered error while processing map in " + this.getClass().getCanonicalName();
      throw new ExecException(msg, PigException.BUG, e);
    }
  }
}
