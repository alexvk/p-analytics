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

/**
 * 
 */
public class AttribsRecord extends AbstractAttribsRecord {
	public AttribsRecord(String[] fields) {
		super(fields);
	}

	@Override
	public String getStatus() {
		/* Should never happen */
		assert (false);
		return null;
	}

	/**
	 * @return the attribute name
	 */
	@Override
	public String getAttrName() {
		return (load == null) ? "N/A" : load.split(",", 2)[0];
	}

	/**
	 * @return the attribute value
	 */
	@Override
	public String getAttrValue() {
		if (load != null) {
			String retValue = load.split(",", 2)[1];
			return (retValue == null) ? "" : retValue;
		}
		return "N/A";
	}
}
