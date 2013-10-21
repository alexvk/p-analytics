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
package com.cloudera.fts.proto;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.cloudera.fts.proto.Item.PItemAttribute;
import com.cloudera.fts.proto.Item.PItemRecord;
import com.google.common.base.Preconditions;

/**
 * Reduce-side function to collect the attributes into a {@link PItemRecord}
 */
public class PItemAttributes2PItemRecordFn extends
    DoFn<Pair<String, Iterable<PItemAttribute>>, PItemRecord> {
	private static final long serialVersionUID = -5734136043853739194L;

	private static final Log LOG = LogFactory.getLog(PItemAttributes2PItemRecordFn.class);

	@Override
  public void process(Pair<String, Iterable<PItemAttribute>> input,
      Emitter<PItemRecord> emitter) {
		String serialNum = input.first();
		Preconditions.checkNotNull(serialNum);
		Preconditions.checkArgument(serialNum.length() > 0);
		emitter.emit(map(serialNum, input.second()));
	}

  public PItemRecord map(String serialNum, Iterable<PItemAttribute> attrs) {
    long last_seq = 0L;
    PItemRecord.Builder dr = PItemRecord.newBuilder().setSerialNum(serialNum)
        .setStatus("P");
    for (PItemAttribute attr : attrs) {
			LOG.debug("serial_num: " + serialNum + " attr: " + attr.toString());
			assert (serialNum.equals(attr.getSerialNum()));
      if (last_seq < attr.getSeq()) {
        last_seq = attr.getSeq();
      }
      dr.addAttributes(attr.toBuilder().clearSerialNum().build());
    }
		return dr.build();
  }
}
