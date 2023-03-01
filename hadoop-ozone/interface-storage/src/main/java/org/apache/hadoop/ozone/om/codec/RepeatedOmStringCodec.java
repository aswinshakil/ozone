/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.codec;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmString;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RepeatedString;

import java.io.IOException;

/**
 * Codec to encode RepeatedOmString as byte array.
 */
public class RepeatedOmStringCodec implements Codec<RepeatedOmString> {
  @Override
  public byte[] toPersistedFormat(RepeatedOmString object) throws IOException {
    Preconditions
        .checkNotNull(object, "Null object can't be converted to byte array.");
    return object.getProto().toByteArray();
  }

  @Override
  public RepeatedOmString fromPersistedFormat(byte[] rawData)
      throws IOException {
    Preconditions.checkNotNull(rawData,
        "Null byte array can't converted to real object.");
    try {
      return RepeatedOmString.getFromProto(RepeatedString.parseFrom(rawData));
    } catch (InvalidProtocolBufferException ex) {
      throw new IllegalArgumentException(
          "Can't encode the the raw data from the byte array", ex);
    }
  }

  @Override
  public RepeatedOmString copyObject(RepeatedOmString object) {
    return object.copyObject();
  }
}
