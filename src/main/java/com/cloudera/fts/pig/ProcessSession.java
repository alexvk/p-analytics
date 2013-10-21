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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Processes a bag of sorted (timestamp, page) pairs and find the most visited
 * pages.
 * 
 * <pre>
 * Example: {(D,1379546800),(C,1379546720),(A,1379546680),(D,1379546675),(B,1379546670),(A,1379546662)} --> {(1,80,C),(2,48,A),(3,5,B),(4,5,D)}
 * </pre>
 * 
 * If input bag is null, this UDF will return null.
 * 
 */
@OutputSchema("y:bag{t:tuple(rank:int,len:long,page:chararray)}")
public class ProcessSession extends EvalFunc<DataBag> {

  private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
  private static final BagFactory mBagFactory = BagFactory.getInstance();

  private static class LongStringPair implements Comparable<LongStringPair> {
    long lvalue;
    String svalue;

    public LongStringPair(long lvalue, String svalue) {
      super();
      this.lvalue = lvalue;
      this.svalue = svalue;
    }

    @Override
    public int compareTo(LongStringPair o) {
      Preconditions.checkNotNull(o);
      LongStringPair that = o;
      // Reverse sorting order on time!
      return ComparisonChain.start().compare(that.lvalue, this.lvalue)
          .compare(this.svalue, that.svalue).result();
    }
  }

  @Override
  public DataBag exec(Tuple inputTuple) throws IOException {

    if (inputTuple.size() != 1) {
      throw new ExecException("Expecting 1 input, found " + inputTuple.size(), PigException.INPUT);
    }

    if (inputTuple.get(0) == null) {
      return null;
    }

    if (!(inputTuple.get(0) instanceof DataBag)) {
      throw new ExecException(
          "Usage ProcessSession(DataBag of (timestamp, page) tuples)",
          PigException.INPUT);
    }

    try {
      DataBag outBag = mBagFactory.newDefaultBag();
      Map<String, Long> timesOnPage = Maps.newHashMap();

      long nextStamp = 0L;
      for (Tuple view : (DataBag) inputTuple.get(0)) {
        Preconditions.checkElementIndex(1, view.size());
        try {
          long timestamp = Long.parseLong(view.get(0).toString());
          String page = view.get(1).toString();
          if (nextStamp != 0L) {
            timesOnPage.put(page,
                timesOnPage.containsKey(page) ? timesOnPage.get(page)
                    + nextStamp - timestamp : nextStamp - timestamp);
          }
          nextStamp = timestamp;
        } catch (NumberFormatException e) {
          throw new ExecException(
              "The first element of the tuples should be timestamp, got a "
                  + view.get(0).getClass().getCanonicalName() + " with value "
                  + view.get(0).toString(), PigException.INPUT);
        }
      }
      Set<LongStringPair> sortedTimes = Sets.newTreeSet();
      for (Map.Entry<String, Long> entry : timesOnPage.entrySet()) {
        sortedTimes.add(new LongStringPair(entry.getValue(), entry.getKey()));
      }
      int rank = 0;
      for (LongStringPair pair : sortedTimes) {
        Tuple tup = mTupleFactory.newTuple(3);
        tup.set(0, new Integer(++rank));
        tup.set(1, new Long(pair.lvalue));
        tup.set(2, pair.svalue);
        outBag.add(tup);
      }
      return outBag;
    } catch (Exception e) {
      String msg = "Encountered error while processing visits in " + this.getClass().getCanonicalName();
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

      // check the tuple fields
      Schema.FieldSchema tupleSchema = inputFieldSchema.schema.getField(0);
      if ((tupleSchema == null) || (tupleSchema.schema == null)
          || tupleSchema.type != DataType.TUPLE) {
        throw new RuntimeException(
            "Expecting a tuple of (timestamp, page) tuples, found: "
                + inputSchema);
      }
      if (tupleSchema.schema.size() < 2
          || tupleSchema.schema.getField(0).type != DataType.CHARARRAY
          || tupleSchema.schema.getField(1).type != DataType.CHARARRAY) {
        throw new RuntimeException(
            "The fields in the (timestamp, page) tuple should be CHARARRAY, found: "
                + tupleSchema.schema.getField(0).getClass().toString() + ", "
                + tupleSchema.schema.getField(1).getClass().toString());
      }

      Schema bagSchema = new Schema();
      final List<Schema.FieldSchema> fields =
          ImmutableList.of(new Schema.FieldSchema("rank", DataType.INTEGER), new Schema.FieldSchema("len", DataType.LONG), new Schema.FieldSchema("page", DataType.CHARARRAY));
      bagSchema.add(new Schema.FieldSchema("t", new Schema(fields)));
      return new Schema(new Schema.FieldSchema("y", bagSchema, DataType.BAG));
      // return super.outputSchema(inputSchema);
    } catch (FrontendException e) {
      e.printStackTrace();
      return null;
    }
  }
}
