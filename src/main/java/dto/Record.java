package dto;

import java.util.Arrays;

public class Record {
        private int frameVersion;
        private int type;
        private int nameLength;
        private int taggedFieldsCount;
        private String name;
        private int featureLevel;
        private int version;
        private int partitionId;
        private int replicaArrayLength;
        private int replicaId;
        private int leader;
        private int leaderEpoch;
        private int partitionEpoch;
        private int dirArrayLength;
        private String dirUuid;
        private int syncReplicaArrayLength;
        private int inSyncReplicaIds;
        private int removingReplicaArrayLength;
        private int addingReplicaArrayLength;

        // Common for all
        private int length;
        private int attributes;
        private int timeStampDelta;
        private int offsetData;
        private int keyLength;
        private byte[] key;
        private int valueLength;
        private int headersArrayCount;

        public int getLength() {
                return length;
        }

        public void setLength(int length) {
                this.length = length;
        }

        public int getAttributes() {
                return attributes;
        }

        public void setAttributes(int attributes) {
                this.attributes = attributes;
        }

        public int getTimeStampDelta() {
                return timeStampDelta;
        }

        public void setTimeStampDelta(int timeStampDelta) {
                this.timeStampDelta = timeStampDelta;
        }

        public int getOffsetData() {
                return offsetData;
        }

        public void setOffsetData(int offsetData) {
                this.offsetData = offsetData;
        }

        public int getKeyLength() {
                return keyLength;
        }

        public void setKeyLength(int keyLength) {
                this.keyLength = keyLength;
        }

        public byte[] getKey() {
                return key;
        }

        public void setKey(byte[] key) {
                this.key = key;
        }

        public int getValueLength() {
                return valueLength;
        }

        public void setValueLength(int valueLength) {
                this.valueLength = valueLength;
        }

        public int getFrameVersion() {
                return frameVersion;
        }

        public void setFrameVersion(int frameVersion) {
                this.frameVersion = frameVersion;
        }

        public int getType() {
                return type;
        }

        public void setType(int type) {
                this.type = type;
        }

        public int getNameLength() {
                return nameLength;
        }

        public void setNameLength(int nameLength) {
                this.nameLength = nameLength;
        }

        public int getTaggedFieldsCount() {
                return taggedFieldsCount;
        }

        public void setTaggedFieldsCount(int taggedFieldsCount) {
                this.taggedFieldsCount = taggedFieldsCount;
        }

        public String topicName;
        public String topicUuid;

        public String getTopicName() {
                return topicName;
        }

        public void setTopicName(String topicName) {
                this.topicName = topicName;
        }

        public String getTopicUuid() {
                return topicUuid;
        }

        public void setTopicUuid(String topicUuid) {
                this.topicUuid = topicUuid;
        }

        public int getFeatureLevel() {
                return featureLevel;
        }

        public void setFeatureLevel(int featureLevel) {
                this.featureLevel = featureLevel;
        }

        public String getName() {
                return name;
        }

        public void setName(String name) {
                this.name = name;
        }

        public int getVersion() {
                return version;
        }

        public void setVersion(int version) {
                this.version = version;
        }

        public int getPartitionId() {
                return partitionId;
        }

        public void setPartitionId(int partitionId) {
                this.partitionId = partitionId;
        }
        public int getReplicaArrayLength() {
                return replicaArrayLength;
        }

        public void setReplicaArrayLength(int replicaArrayLength) {
                this.replicaArrayLength = replicaArrayLength;
        }

        public int getReplicaId() {
                return replicaId;
        }

        public void setReplicaId(int replicaId) {
                this.replicaId = replicaId;
        }

        public int getLeader() {
                return leader;
        }

        public void setLeader(int leader) {
                this.leader = leader;
        }

        public int getLeaderEpoch() {
                return leaderEpoch;
        }

        public void setLeaderEpoch(int leaderEpoch) {
                this.leaderEpoch = leaderEpoch;
        }

        public int getPartitionEpoch() {
                return partitionEpoch;
        }

        public void setPartitionEpoch(int partitionEpoch) {
                this.partitionEpoch = partitionEpoch;
        }

        public int getDirArrayLength() {
                return dirArrayLength;
        }

        public void setDirArrayLength(int dirArrayLength) {
                this.dirArrayLength = dirArrayLength;
        }

        public String getDirUuid() {
                return dirUuid;
        }

        public void setDirUuid(String dirUuid) {
                this.dirUuid = dirUuid;
        }

        public int getSyncReplicaArrayLength() {
                return syncReplicaArrayLength;
        }

        public void setSyncReplicaArrayLength(int syncReplicaArrayLength) {
                this.syncReplicaArrayLength = syncReplicaArrayLength;
        }

        public int getInSyncReplicaIds() {
                return inSyncReplicaIds;
        }

        public void setInSyncReplicaIds(int inSyncReplicaIds) {
                this.inSyncReplicaIds = inSyncReplicaIds;
        }

        public int getRemovingReplicaArrayLength() {
                return removingReplicaArrayLength;
        }

        public void setRemovingReplicaArrayLength(int removingReplicaArrayLength) {
                this.removingReplicaArrayLength = removingReplicaArrayLength;
        }

        public int getAddingReplicaArrayLength() {
                return addingReplicaArrayLength;
        }

        public void setAddingReplicaArrayLength(int addingReplicaArrayLength) {
                this.addingReplicaArrayLength = addingReplicaArrayLength;
        }

        public void setHeadersArrayCount(int headersArrayCount) {
                this.headersArrayCount = headersArrayCount;
        }

        public int getHeadersArrayCount() {
                return headersArrayCount;
        }

        @Override
        public String toString() {
                return "Record{" +
                        "frameVersion=" + frameVersion +
                        ", type=" + type +
                        ", nameLength=" + nameLength +
                        ", taggedFieldsCount=" + taggedFieldsCount +
                        ", name='" + name + '\'' +
                        ", featureLevel=" + featureLevel +
                        ", version=" + version +
                        ", partitionId=" + partitionId +
                        ", replicaArrayLength=" + replicaArrayLength +
                        ", replicaId=" + replicaId +
                        ", leader=" + leader +
                        ", leaderEpoch=" + leaderEpoch +
                        ", partitionEpoch=" + partitionEpoch +
                        ", dirArrayLength=" + dirArrayLength +
                        ", dirUuid='" + dirUuid + '\'' +
                        ", syncReplicaArrayLength=" + syncReplicaArrayLength +
                        ", inSyncReplicaIds=" + inSyncReplicaIds +
                        ", removingReplicaArrayLength=" + removingReplicaArrayLength +
                        ", addingReplicaArrayLength=" + addingReplicaArrayLength +
                        ", length=" + length +
                        ", attributes=" + attributes +
                        ", timeStampDelta=" + timeStampDelta +
                        ", offsetData=" + offsetData +
                        ", keyLength=" + keyLength +
                        ", key=" + Arrays.toString(key) +
                        ", valueLength=" + valueLength +
                        ", headersArrayCount=" + headersArrayCount +
                        ", topicName='" + topicName + '\'' +
                        ", topicUuid='" + topicUuid + '\'' +
                        '}';
        }
}
