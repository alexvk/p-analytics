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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

@SuppressWarnings("serial")
/**
 * This is base class for Tuple implementations that delay parsing until
 * individual fields are requested.
 */
public abstract class AbstractLazyTuple implements Tuple {

  private static TupleFactory tf  = TupleFactory.getInstance();

  protected Tuple realTuple;
  protected boolean isRef; // i.e. reference() is invoked.
  protected BitSet idxBits;

  protected void initRealTuple(int tupleSize) {
    realTuple = tf.newTuple(tupleSize);
    idxBits = new BitSet(tupleSize);
    isRef = false;
  }

  /**
   * Returns object for the given index. This is invoked only
   * once for each instance.
   */
  protected abstract Object getObjectAt(int index);

  @Override
  public void append(Object obj) {
    realTuple.append(obj);
  }

  @Override
  public Object get(int idx) throws ExecException {
    if (!isRef && !idxBits.get(idx)) {
      realTuple.set(idx, getObjectAt(idx));
      idxBits.set(idx);
    }
    return realTuple.get(idx);
  }

  @Override
  public List<Object> getAll() {
    convertAll();
    return realTuple.getAll();
  }

  @Override
  public long getMemorySize() {
    return realTuple.getMemorySize();
  }

  @Override
  public byte getType(int idx) throws ExecException {
    get(idx);
    return realTuple.getType(idx);
  }

  @Override
  public boolean isNull(int idx) throws ExecException {
    get(idx);
    return realTuple.isNull(idx);
  }

  @Override
  public void reference(Tuple t) {
    if (t != this) {
      realTuple = t;
      isRef = true; // don't invoke getObjetAt() anymore.
    }
  }

  @Override
  public void set(int idx, Object val) throws ExecException {
    realTuple.set(idx, val);
    idxBits.set(idx);
  }

  @Override
  public int size() {
    return realTuple.size();
  }

  @Override
  public String toDelimitedString(String delim) throws ExecException {
    convertAll();
    return realTuple.toDelimitedString(delim);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Tuple t = tf.newTuple(realTuple.size());
    t.readFields(in);
    reference(t);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    convertAll();
    realTuple.write(out);
  }

  @Override
  public String toString() {
    convertAll();
    return realTuple.toString();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(Object arg0) {
    convertAll();
    return realTuple.compareTo(arg0);
  }

  protected void convertAll() {
    if (isRef) {
      return;
    }
    int size = realTuple.size();
    for (int i = 0; i < size; i++) {
      try {
        get(i);
      } catch (ExecException e) {
        throw new RuntimeException("Unable to process field " + i, e);
      }
    }
  }
}
