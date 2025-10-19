package dto;

import java.util.List;
import java.util.UUID;

/**
 * A clean DTO to hold final, parsed partition information.
 */
public record PartitionData(
        int partitionId,
        String topicUuid,
        List<Integer> replicas,
        List<Integer> inSyncReplicas,
        int leader,
        int leaderEpoch,
        int partitionEpoch
) {}