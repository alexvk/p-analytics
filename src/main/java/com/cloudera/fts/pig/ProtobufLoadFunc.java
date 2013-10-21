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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.crunch.types.Protos;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.google.protobuf.Message;

/**
 * A Pig {@code LoadFunc} implementation for reading protocol buffers stored in Sequence Files.
 */
public class ProtobufLoadFunc extends LoadFunc implements LoadMetadata, LoadPushDown {

  private static final ProtobufToPig PROTO_TO_PIG = new ProtobufToPig();
  private static final String PROJECTION_KEY = "ProtobufLoadFunc_projectedFields";
  
  private final Message instance;
  private String contextSignature;
  private RecordReader<NullWritable, BytesWritable> reader;
  private RequiredFieldList requiredFieldList;
  private ProjectedProtobufTupleFactory tupleFactory;
  
  @SuppressWarnings("unchecked")
  public ProtobufLoadFunc(String protoClassName) {
    try {
      this.instance = ReflectionUtils.newInstance(
          (Class<? extends Message>) Class.forName(protoClassName), null);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
  public ProtobufLoadFunc(Class<? extends Message> clazz) {
    // this.protoInstance = ReflectionUtils.newInstance(protoClazz, null);
    this.instance = Protos.getDefaultInstance(clazz);
  }
  
  @Override
  public InputFormat<NullWritable, BytesWritable> getInputFormat() throws IOException {
    return new SequenceFileInputFormat<NullWritable, BytesWritable>();
  }

  /** UDF properties for this class based on context signature */
  protected Properties getUDFProperties() {
    return UDFContext.getUDFContext()
        .getUDFProperties(this.getClass(), new String[] {contextSignature});
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }
  
  @Override
  public List<OperatorSet> getFeatures() {
    return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
    try {
      getUDFProperties().setProperty(PROJECTION_KEY, ObjectSerializer.serialize(requiredFieldList));
    } catch (IOException e) { // not expected
      throw new FrontendException(e);
    }
    return new RequiredFieldResponse(true);
  }
  
  @Override
  public Tuple getNext() throws IOException {
    if (tupleFactory == null) {
      tupleFactory = new ProjectedProtobufTupleFactory(instance, requiredFieldList);
    }
    try {
      if (reader != null && reader.nextKeyValue()) {
        BytesWritable bw = reader.getCurrentValue();
        Message v = instance.newBuilderForType().mergeFrom(bw.getBytes(), 0, bw.getLength()).build();
        return tupleFactory.newTuple(v);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return null;
  }

	@Override
  public void prepareToRead(RecordReader recordReader, PigSplit pigSplit)
	    throws IOException {
		this.reader = recordReader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
    
    String projectedFields = getUDFProperties().getProperty(PROJECTION_KEY);
    if (projectedFields != null) {
      requiredFieldList =
        (RequiredFieldList) ObjectSerializer.deserialize(projectedFields);
    }
  }

  @Override
  public ResourceSchema getSchema(String path, Job job) throws IOException {
    return new ResourceSchema(PROTO_TO_PIG.toSchema(instance.getDescriptorForType()));
  }

  @Override
  public String[] getPartitionKeys(String path, Job job) throws IOException {
    // NOT IMPLEMENTED
    return null;
  }

  @Override
  public ResourceStatistics getStatistics(String path, Job job)
      throws IOException {
 // NOT IMPLEMENTED
    return null;
  }

  @Override
  public void setPartitionFilter(Expression expr) throws IOException {
    // NOT IMPLEMENTED
  }

}
