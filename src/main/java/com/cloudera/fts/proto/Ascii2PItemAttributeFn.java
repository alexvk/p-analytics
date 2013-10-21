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

import com.cloudera.fts.crunch.AttribsRecord;
import com.cloudera.fts.crunch.SplitFactory;
import com.cloudera.fts.proto.Item.PItemAttribute;

/**
 * Converts a delimited line of text from the database dump of the attr table
 * into a {@code PItemAttribute} message.
 * 
 */
public class Ascii2PItemAttributeFn extends DoFn<String, PItemAttribute> {
  private static final long serialVersionUID = 5468738127600291617L;

  private static final Log LOG = LogFactory.getLog(Ascii2PItemAttributeFn.class);

	private final SplitFactory sFactory = new SplitFactory();
  
	public Ascii2PItemAttributeFn() {
    sFactory.setType(SplitFactory.Type.ATTRS);
  }
  
  @Override
  public void process(String input, Emitter<PItemAttribute> emitter) {
    if (input != null && !input.isEmpty()) {
      PItemAttribute da = map(input);
      if (da != null) {
				emitter.emit(da);
      }
    }
  }
  
  public PItemAttribute map(String input) {
		AttribsRecord attr = (AttribsRecord) sFactory.create(input);
    PItemAttribute.Builder pb = PItemAttribute.newBuilder();
    
		LOG.debug("attr: " + attr.toString());

		if (attr.getSerialNum() == null || attr.getSerialNum().length() == 0) {
			throw new IllegalStateException("Serial number cannot be missing: "
			    + attr.toString() + " in '" + input + "'");
    }
		pb.setSerialNum(attr.getSerialNum());

    pb.setSeq(attr.getSeq());

    if (attr.getName() == null || attr.getName().length() == 0) {
      throw new IllegalStateException("Attribute should have a name: " + attr.toString());
    }
    pb.setName(attr.getName());

		if (attr.getAttrValue() != null && attr.getAttrValue().length() > 0) {
			pb.setValue(attr.getAttrValue());
    }

    // TODO: throw in more checks here...

    return pb.build();
  }
}
