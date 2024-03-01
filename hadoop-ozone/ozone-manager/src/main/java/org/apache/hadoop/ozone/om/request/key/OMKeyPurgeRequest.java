/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_SNAPSHOT_ERROR;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeletedKeys> bucketDeletedKeysList = purgeKeysRequest
        .getDeletedKeysList();
    List<SnapshotMoveKeyInfos> keysToUpdateList = purgeKeysRequest
        .getKeysToUpdateList();
    UUID fromSnapshotId = purgeKeysRequest.hasSnapshotId() ?
        fromProtobuf(purgeKeysRequest.getSnapshotId()) : null;
    List<String> keysToBePurgedList = new ArrayList<>();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;

    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      for (String deletedKey : bucketWithDeleteKeys.getKeysList()) {
        keysToBePurgedList.add(deletedKey);
      }
    }

    try {
      if (purgeKeysRequest.hasSnapshotTableKey()) {
        throw new OMException("The field snapshotTableKey from PurgeKeysRequest is deprecated." +
            " The request will be retried in the next try", OMException.ResultCodes.INTERNAL_ERROR);
      }

      SnapshotInfo fromSnapshotInfo = null;
      if (fromSnapshotId != null) {
        String snapTableKey = metadataManager.getSnapshotChainManager()
            .getTableKey(fromSnapshotId);
        fromSnapshotInfo = metadataManager.getSnapshotInfoTable().get(snapTableKey);

        if (fromSnapshotInfo == null) {
          LOG.error("SnapshotInfo for Snapshot: {} is not found", snapTableKey);
          throw new OMException("SnapshotInfo for Snapshot: " + snapTableKey +
              " is not found", INVALID_SNAPSHOT_ERROR);
        }
      }
      omClientResponse = new OMKeyPurgeResponse(omResponse.build(),
          keysToBePurgedList, fromSnapshotInfo, keysToUpdateList);
    } catch (IOException ex) {
      omClientResponse = new OMKeyPurgeResponse(
          createErrorOMResponse(omResponse, ex));
    }

    return omClientResponse;
  }

}
