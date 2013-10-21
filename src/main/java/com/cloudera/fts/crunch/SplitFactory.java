/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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

import java.io.Serializable;

/**
 * Creates a {@code EventRecord} or {@code AttribsRecord} object by parsing a
 * line of text from a database dump file.
 */
@SuppressWarnings("serial")
public class SplitFactory implements Serializable {

	public final static String SEP = ",";
	private final int numFields = 3;

	public enum Type {
		EVENT, ATTRS
	};

	private Type type = Type.EVENT;
  
	/**
	 * @return the type
	 */
	public Type getType() {
		return type;
	}

	/**
	 * @param type
	 *          the type to set
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Create corresponding record
	 * 
	 * @param line
	 * @return {@see AbstractAttribsRecord} record
	 */
	public AbstractAttribsRecord create(String line) {
		switch (type) {
		case EVENT:
			return new EventRecord(line.split(SEP, numFields));
		case ATTRS:
			return new AttribsRecord(line.split(SEP, numFields));
		}
		return null;
	}
}
