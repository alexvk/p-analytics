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
package com.cloudera.fts.crunch;

import com.google.common.base.Joiner;

/**
 * Represents an Event record as it is represented in the file dump from the
 * database.
 */
public class CertEventStrings {

  private final String serialNumber;
  private final long seq;
	private final String status;

	private final String sep;

  /**
   * Parse the actual ascii record
   * 
   * @param line
   * @param sep
   */
  public CertEventStrings(String[] line, String sep) {
		this.sep = sep;
		if (line != null && line.length > 2) {
      this.serialNumber = line[0];
			this.seq = Long.parseLong(line[1]);
			this.status = line[3];
    } else {
      this.serialNumber = "UNKNOWN";
			this.seq = 0L;
			this.status = "U";
    }
  }

  /**
   * @return the serialNumber
   */
  public String getSerialNumber() {
    return serialNumber;
  }

  /**
   * @return the seq
   */
  public long getSeq() {
    return seq;
  }

  /**
	 * @return the status
	 */
	public String getStatus() {
		return status;
  }

  /**
   * @return the sep
   */
  public String getSep() {
    return sep;
  }

  @Override
  public String toString() {
		return Joiner.on(sep).join(serialNumber, seq, status);
  }
}
