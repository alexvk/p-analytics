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
package com.cloudera.fts.hive;

import org.apache.hadoop.hive.serde2.SerDeException;

import com.cloudera.fts.proto.Item.PItemAttribute;

/**
 * A SerDe for {@code PItemAttributeSerDe} objects.
 */
public class PItemAttributeSerDe extends ProtobufSerDe {
  public PItemAttributeSerDe() throws SerDeException {
    super(PItemAttribute.getDefaultInstance());
  }
}
