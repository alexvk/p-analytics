/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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

/*
 * Original date: Feb 9, 2013
 */
package com.cloudera.fts.crunch;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

/**
 * 
 */
public abstract class AbstractAttribsRecord implements Comparable<AbstractAttribsRecord> {
	private final String serial_num;
	private final long seq;
	protected final String load;

	public AbstractAttribsRecord(String[] fields) {
		super();
		Preconditions.checkNotNull(fields);
		Preconditions.checkArgument(fields.length > 2);
		this.serial_num = fields[0];
		this.seq = Long.parseLong(fields[1]);
		this.load = fields[2];
	}

	/**
	 * @return the serial_num
	 */
	public String getSerialNum() {
		return serial_num;
	}

	/**
	 * @return the seq
	 */
	public long getSeq() {
		return seq;
	}

	/**
	 * @return the load
	 */
	protected String getLoad() {
		return load;
	}
		public String getName() {
		return (load == null) ? "N/A" : load.split(",", 2)[0];
	}

	@Override
	public int compareTo(AbstractAttribsRecord o) {
		return ComparisonChain.start().compare(this.serial_num, o.serial_num)
		    .compare(this.seq, o.seq).compare(this.load, o.load).result();
	}

	/**
	 * Return a nice string representation
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Joiner.on(",").join(serial_num, seq, load);
	}

	/**
	 * @return the cert status
	 */
	public abstract String getStatus();

  /**
	 * @return the attribute name
	 */
  public abstract String getAttrName();
			
	/**
	 * @return the attribute value
	 */
	public abstract String getAttrValue();
}
