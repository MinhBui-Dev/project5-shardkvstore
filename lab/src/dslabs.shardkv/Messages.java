package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
    // TODO: shard store request ...
    private final int configNum;
    private final Address address;
    private final int sequenceNumber;
    private final Command command;
}

@Data
final class ShardStoreReply implements Message {
    // TODO: shard store reply ...
    private final Address address;
    private final int sequenceNumber;
    private final Result result;
}

// TODO: add more messages here ...
@Data
final class ShardMoveRequest implements Message {
    // TODO: shard move request ...
    private final String groupId;
    private final ShardConfig config;
    private final Map<Integer,AMOApplication<Application>> shards;

}

@Data
final class ShardMoveReply implements Message {
    // TODO: shard move ack ...
    private final String groupId;
    private final ShardConfig config;
    private final String groupIdAck;
}
