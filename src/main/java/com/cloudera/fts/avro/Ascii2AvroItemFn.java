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
package com.cloudera.fts.avro;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.cloudera.fts.crunch.AbstractAttribsRecord;
import com.cloudera.fts.crunch.SplitFactory;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Fills the {@link AvroItem} structure with information from the secondary sort
 * 
 */
public class Ascii2AvroItemFn extends DoFn<Pair<String, Pair<Collection<String>, Collection<String>>>, AvroItem> {
	private static final long serialVersionUID = -3202810447740733765L;

	private static final Log LOG = LogFactory.getLog(Ascii2AvroItemFn.class);

	private final SplitFactory splitFactory = new SplitFactory();
  
	@Override
  public void process(Pair<String, Pair<Collection<String>, Collection<String>>> input,
      Emitter<AvroItem> emitter) {
	  // The first collection is events, the second attributes: need to sort them on seq
		if (input.second().first().size() < 1)
			return;
		String eventString = Iterables.getOnlyElement(input.second().first());
		splitFactory.setType(SplitFactory.Type.EVENT);
		AbstractAttribsRecord eventRecord = splitFactory.create(eventString);
		LOG.debug(input.first() + " event: " + eventRecord.toString());
		assert(eventRecord.getSerialNum().equals(input.first()));
		if (input.second().second().size() < 1)
			return;
		Set<AbstractAttribsRecord> attribs = new TreeSet<AbstractAttribsRecord>();
		splitFactory.setType(SplitFactory.Type.ATTRS);
		Map<CharSequence, CharSequence> map = Maps.newHashMap();
		for (String attrString: input.second().second()) {
			AbstractAttribsRecord attribsRecord = splitFactory.create(attrString);
			LOG.debug(input.first() + " attribs: " + attribsRecord.toString());
			assert(attribsRecord.getSerialNum().equals(input.first()));
			if (attribsRecord.getSeq() <= eventRecord.getSeq()) {
				attribs.add(attribsRecord);
			}
		}
		for (AbstractAttribsRecord attribsRecord: attribs) {
			map.put(attribsRecord.getAttrName(), attribsRecord.getAttrValue());
		}
		if (LOG.isDebugEnabled()) {
			for (Map.Entry<CharSequence, CharSequence> entry : map.entrySet()) {
				LOG.debug(input.first() + " map: " + entry.getKey() + "#"
				    + entry.getValue());
			}
		}
    emitter.emit(AvroItem.newBuilder().setSerialNum(input.first())
        .setStatus(eventRecord.getStatus())
		    .setSeq(eventRecord.getSeq()).setAttributes(map).build());
  }
}
