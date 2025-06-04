---
title: Container Reconciliation
summary: Detect and repair data inconsistencies between container replicas
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Container Reconciliation

## Overview

Container Reconciliation is a feature in Apache Ozone that automatically detects and repairs data inconsistencies between container replicas, ensuring data integrity and reliability.

## Key Benefits

✅ Automatic inconsistency detection
✅ Peer-to-peer data repair
✅ Minimal performance impact
✅ Enhanced data durability

## Configuration

### Datanode Configurations

| Configuration | Description | Default | Recommended Range |
|--------------|-------------|---------|------------------|
| `hdds.datanode.container.reconciliation.enabled` | Enable container reconciliation | `true` | `true`/`false` |
| `hdds.datanode.container.reconciliation.interval` | Reconciliation check interval | `3600000` ms (1 hour) | `1800000`-`86400000` ms |
| `hdds.datanode.container.reconciliation.max.retry` | Maximum reconciliation retries | `3` | `1`-`10` |
| `hdds.datanode.container.checksum.lock.stripes` | Lock stripes for checksum operations | `64` | `16`-`128` |
| `hdds.datanode.container.reconciliation.chunk.buffer.size` | Chunk transfer buffer size | `1048576` bytes (1MB) | `512000`-`4194304` bytes |

### SCM Configurations

| Configuration | Description | Default | Recommended Range |
|--------------|-------------|---------|------------------|
| `hdds.scm.container.reconciliation.enabled` | Enable SCM-level reconciliation | `true` | `true`/`false` |
| `hdds.scm.container.reconciliation.timeout` | Reconciliation command timeout | `1800000` ms (30 min) | `600000`-`3600000` ms |
| `hdds.scm.container.reconciliation.scan.interval` | SCM reconciliation scan interval | `86400000` ms (24 hours) | `3600000`-`604800000` ms |

## Usage

### Manual Reconciliation

Trigger reconciliation for a specific container:

```bash
ozone admin container reconcile <container-id>
```

### View Container Checksum

To view container replica checksums:

```bash
ozone admin container info <container-id> --format=json
```

Example JSON output:
```json
{
  "containerId": 1234,
  "replicas": [
    {
      "datanodeId": "datanode1",
      "dataChecksum": "0x1a2b3c4d",
      "status": "HEALTHY"
    },
    {
      "datanodeId": "datanode2", 
      "dataChecksum": "0x1a2b3c4d",
      "status": "HEALTHY"
    }
  ]
}
```

## Metrics

### Reconciliation Metrics

| Metric Name | Description | Type |
|------------|-------------|------|
| `ozone_container_reconciliation_attempts_total` | Total reconciliation attempts | Counter |
| `ozone_container_reconciliation_successes_total` | Successful reconciliations | Counter |
| `ozone_container_reconciliation_failures_total` | Failed reconciliations | Counter |
| `ozone_container_reconciliation_duration_seconds` | Reconciliation operation duration | Histogram |

### Data Transfer Metrics

| Metric Name | Description | Type |
|------------|-------------|------|
| `ozone_container_reconciliation_chunks_downloaded_total` | Chunks downloaded during reconciliation | Counter |
| `ozone_container_reconciliation_blocks_downloaded_total` | Blocks downloaded during reconciliation | Counter |
| `ozone_container_reconciliation_bytes_transferred_total` | Bytes transferred during reconciliation | Counter |

### Container Health Metrics

| Metric Name | Description | Type |
|------------|-------------|------|
| `ozone_container_reconciliation_containers_processed_total` | Containers reconciled | Counter |
| `ozone_container_reconciliation_checksum_mismatches_total` | Checksum inconsistencies detected | Counter |
| `ozone_container_reconciliation_corruptions_repaired_total` | Corrupt chunks/blocks repaired | Counter |

## Monitoring and Prometheus Queries

### Sample Prometheus Queries

1. Reconciliation Success Rate
```
rate(ozone_container_reconciliation_successes_total[1h]) / 
rate(ozone_container_reconciliation_attempts_total[1h])
```

2. Average Reconciliation Duration
```
histogram_quantile(0.95, rate(ozone_container_reconciliation_duration_seconds_bucket[1h]))
```

3. Total Corruptions Repaired
```
increase(ozone_container_reconciliation_corruptions_repaired_total[24h])
```

## Best Practices

1. Enable regular scanning and reconciliation
2. Monitor reconciliation metrics
3. Set appropriate timeout and retry configurations
4. Ensure sufficient network bandwidth for data transfers

## Limitations

- Supports only Ratis replicated containers
- Does not support Erasure Coded (EC) containers

## Troubleshooting

### Common Issues

1. **High Reconciliation Failure Rate**
   - Check network connectivity
   - Verify datanode health
   - Review system logs

2. **Performance Concerns**
   - Adjust `hdds.datanode.replication.streams.limit`
   - Tune chunk buffer size
   - Balance reconciliation interval

## Future Roadmap

- Automated reconciliation with replication manager
- Erasure Coded container support
- Advanced predictive maintenance

## Conclusion

Container Reconciliation ensures data integrity by proactively detecting and repairing inconsistencies in your Apache Ozone cluster.