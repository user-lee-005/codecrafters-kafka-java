package dto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Holds the processed metadata, indexed for fast lookups
 * by topic name and topic UUID.
 */
public record MetadataCache(
        Map<String, List<PartitionData>> byName,
        Map<String, List<PartitionData>> byUuid
) {}