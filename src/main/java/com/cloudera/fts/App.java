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
package com.cloudera.fts;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.fts.avro.Ascii2AvroItemFn;
import com.cloudera.fts.avro.AvroItem;
import com.cloudera.fts.crunch.AbstractAttribsRecord;
import com.cloudera.fts.crunch.SplitFactory;
import com.cloudera.fts.proto.Ascii2PItemAttributeFn;
import com.cloudera.fts.proto.Item.PItemAttribute;
import com.cloudera.fts.proto.Item.PItemRecord;
import com.cloudera.fts.proto.PItemAttributes2PItemRecordFn;
import com.google.common.base.Preconditions;

/**
 * The main program which invokes the pipelines
 */
public class App extends Configured implements Tool {

  private App() {
  }
  
  public transient static final PType<PItemAttribute> daType = PTypes.protos(
      PItemAttribute.class, WritableTypeFamily.getInstance());
	public transient static final PType<PItemRecord> drType = PTypes.protos(
	    PItemRecord.class, WritableTypeFamily.getInstance());

	private transient static final SplitFactory splitFactory = new SplitFactory();

	/*
	 * private static final GroupingOptions groupingOptions = GroupingOptions
	 * .builder() .partitionerClass(
	 * JoinUtils.getPartitionerClass(WritableTypeFamily.getInstance()))
	 * .groupingComparatorClass(
	 * JoinUtils.getGroupingComparator(WritableTypeFamily.getInstance()))
	 * .build();
	 */

	private static class SerialNumFn extends MapFn<String, String> {
		private static final long serialVersionUID = 5670412215095296207L;
		@Override
		public String map(String input) {
			AbstractAttribsRecord record = splitFactory.create(input);
			return record.getSerialNum();
		}
  }

  private static class ExtractSerialNumFn extends MapFn<PItemAttribute, String> {
		private static final long serialVersionUID = -1623236528589594790L;
		@Override
    public String map(PItemAttribute input) {
			return input.getSerialNum();
		}
	}

  private static class ExtractSeqFn extends DoFn<PItemAttribute, Long> {
		private static final long serialVersionUID = -1830826167938214704L;
		@Override
    public void process(PItemAttribute input, Emitter<Long> emitter) {
			Preconditions.checkNotNull(input);
			emitter.emit(input.getSeq());
		}
	}

  private void printUsage() {
    GenericOptionsParser.printGenericCommandUsage(System.err);
    System.err.println("Basic Usage: [avro,proto,text2pb,count] <inputdir> <outputdir>");
    System.exit(1);
  }

	private void printAvroUsage() {
		GenericOptionsParser.printGenericCommandUsage(System.err);
		System.err
		    .println("Avro requires one extra argument, the event file name: avro <inputdir> <events_file> <outputdir>");
		System.exit(1);
	}

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      printUsage();
      return 1;
    }

    String cmd = args[0];
    String input = args[1];
    String output = args[2];

    System.out.println("App.run -> executing following command: " + cmd);

		if ("text2pb".equals(cmd)) {
			Pipeline p = new MRPipeline(App.class, getConf());
			p.read(From.textFile(input))
          .parallelDo("text2pb", new Ascii2PItemAttributeFn(),
			        daType).write(To.sequenceFile(output));
			p.done();
		} else if ("avro".equals(cmd)) {
			String eventsFile = output;
			if (args.length < 4) {
				printAvroUsage();
				return 1;
			}
			output = args[3];
			Pipeline p = new MRPipeline(App.class, getConf());
      PTable<String, String> item = p.read(From.textFile(eventsFile)).by(
          new SerialNumFn(), Writables.strings());
			PTable<String, String> attrs = p.read(From.textFile(input)).by(new SerialNumFn(), Writables.strings());
      item.cogroup(attrs)
          .parallelDo("records_avro", new Ascii2AvroItemFn(),
              Avros.records(AvroItem.class)).write(To.avroFile(output));
			/* .write(To.textFile(output)); */
			p.done();
		} else if ("proto".equals(cmd)) {
      Pipeline p = new MRPipeline(App.class, getConf());
      PCollection<PItemAttribute> attr = p.read(From.textFile(input))
          .parallelDo("ascii2attr", new Ascii2PItemAttributeFn(), daType);
      PGroupedTable<String, PItemAttribute> grouped = attr.by("serial_num",
          new ExtractSerialNumFn(), Writables.strings()).groupByKey();
      PCollection<PItemRecord> out = grouped.parallelDo("records_proto",
          new PItemAttributes2PItemRecordFn(), drType);
      System.out.println("Total output records: " + out.getSize());
      p.write(out, To.sequenceFile(output));
      p.done();
    } else if ("count".equals(cmd)) {
      Pipeline p = new MRPipeline(App.class, getConf());
			p.read(From.sequenceFile(input, daType))
			    .parallelDo(new ExtractSeqFn(), Writables.longs()).count().write(To.textFile(output));
      p.done();
    } else {
      System.err.println("Unknown command: " + args[0]);
      return 1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new App(), args);
  }
}
