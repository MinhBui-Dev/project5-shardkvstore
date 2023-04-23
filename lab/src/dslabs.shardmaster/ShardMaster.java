package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;
    private final int numShards;

    // TODO: declare fields for your implementation ...
    private int lastConfigNum;
    private Set<Integer> groupIdSet;
    private Map<Integer, ShardConfig> configMap;
    private int lastAdoptedConfigNum;




    public ShardMaster(int numShards) {
        this.numShards = numShards;

        // TODO: initial fields ...
        groupIdSet = new HashSet<>();
        configMap = new HashMap<>();
        lastAdoptedConfigNum = -1;


    }

    public interface ShardMasterCommand extends Command {
    }

    @Data
    public static final class Join implements ShardMasterCommand {
        private final int groupId;
        private final Set<Address> servers;
    }

    @Data
    public static final class Leave implements ShardMasterCommand {
        private final int groupId;
    }

    @Data
    public static final class Move implements ShardMasterCommand {
        private final int groupId;
        private final int shardNum;
    }

    @Data
    public static final class Query implements ShardMasterCommand {
        private final int configNum;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    public interface ShardMasterResult extends Result {
    }

    @Data
    public static final class Ok implements ShardMasterResult {
    }

    @Data
    public static final class Error implements ShardMasterResult {
    }

    @Data
    public static final class ShardConfig implements ShardMasterResult {
        private final int configNum;

        // groupId -> <group members, shard numbers>
        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
    }

    @Override
    public Result execute(Command command) {
        if (command instanceof Join) {
            Join join = (Join) command;

            // TODO: implement Join ...
            if (groupIdSet != null && groupIdSet.contains(join.groupId)) {
                return new Error();
            }
            groupIdSet.add(join.groupId);

            if (configMap.isEmpty()) {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                        new HashMap<>();
                groupInfo.put(join.groupId,
                        new MutablePair<>(join.servers, new HashSet<>()));
                for (int i = 1; i <= numShards; i++) {
                    groupInfo.get(join.groupId).getRight().add(i);
                }
                configMap.put(INITIAL_CONFIG_NUM,
                        new ShardConfig(INITIAL_CONFIG_NUM, groupInfo));
                lastConfigNum = INITIAL_CONFIG_NUM;
            } else {
                // TODO: implement Balance ...
                int numShardsPerGroup = numShards / groupIdSet.size();

                // Clone the previous groupInfo
                Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                        new HashMap<>();
                for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry : configMap.get(
                        lastConfigNum).groupInfo.entrySet()) {
                    groupInfo.put(entry.getKey(),
                            new MutablePair<>(entry.getValue().getLeft(),
                                    new HashSet<>(
                                            entry.getValue().getRight())));
                }

                // Add the new group
                groupInfo.put(join.groupId,
                        new MutablePair<>(join.servers, new HashSet<>()));

                // Find number of shards to move
                int numShardsToMove = 0;

                // Remove the extra shards from the groups and add them to the new group
                Iterator<Integer> it = groupInfo.keySet().iterator();
                while (it.hasNext()) {
                    int groupId = it.next();
                    int shardInGroup = groupInfo.get(groupId).getRight().size();
                    numShardsToMove +=
                            Math.max(0, numShardsPerGroup - shardInGroup);
                }
                it = groupInfo.keySet().iterator();
                Set<Integer> shardsToMove = new HashSet<>();
                while (it.hasNext() && numShardsToMove > 0) {
                    int groupId = it.next();
                    if (groupInfo.get(groupId).getRight().size() >
                            numShardsPerGroup) {
                        int numShardsToRemove =
                                groupInfo.get(groupId).getRight().size() -
                                        numShardsPerGroup;
                        Iterator<Integer> it2 =
                                groupInfo.get(groupId).getRight().iterator();
                        while (it2.hasNext() && numShardsToRemove > 0 &&
                                numShardsToMove > 0) {
                            shardsToMove.add(it2.next());
                            it2.remove();
                            numShardsToRemove--;
                            numShardsToMove--;
                        }
                    }
                }

                it = groupInfo.keySet().iterator();
                while (it.hasNext()) {
                    balance(groupInfo, shardsToMove, numShardsPerGroup, it);
                }

                configMap.put(++lastConfigNum,
                        new ShardConfig(lastConfigNum, groupInfo));
            }
            return new Ok();
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // TODO: implement Leave ...
            if (groupIdSet != null && !groupIdSet.contains(leave.groupId)) {
                return new Error();
            }
            groupIdSet.remove(leave.groupId);

            Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                    new HashMap<>();
            for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry : configMap.get(
                    lastConfigNum).groupInfo.entrySet()) {
                groupInfo.put(entry.getKey(),
                        new MutablePair<>(entry.getValue().getLeft(),
                                new HashSet<>(entry.getValue().getRight())));
            }
            Set<Integer> shardsToMove = groupInfo.get(leave.groupId).getRight();
            groupInfo.remove(leave.groupId);
            int numShardsPerGroup = numShards / groupIdSet.size();
            Iterator<Integer> it = groupInfo.keySet().iterator();
            while (it.hasNext() && !shardsToMove.isEmpty()) {
                balance(groupInfo, shardsToMove, numShardsPerGroup, it);
            }
            configMap.put(++lastConfigNum,
                    new ShardConfig(lastConfigNum, groupInfo));
            return new Ok();

        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // TODO: implement Move ...
            if (groupIdSet != null && !groupIdSet.contains(move.groupId)) {
                return new Error();
            } else if (move.shardNum < 1 || move.shardNum > numShards) {
                return new Error();
            } else if (configMap.get(lastConfigNum).groupInfo.get(
                    move.groupId).getRight().contains(move.shardNum)) {
                return new Error();
            }
            // Clone the groupInfo
            Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                    new HashMap<>();
            for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry : configMap.get(
                    lastConfigNum).groupInfo.entrySet()) {
                groupInfo.put(entry.getKey(),
                        new MutablePair<>(entry.getValue().getLeft(),
                                new HashSet<>(entry.getValue().getRight())));
            }
            // Remove the shard from the old group
            Iterator<Integer> iterator = groupInfo.keySet().iterator();
            while (iterator.hasNext()) {
                int groupId = iterator.next();
                if (groupInfo.get(groupId).getRight().contains(move.shardNum)) {
                    groupInfo.get(groupId).getRight().remove(move.shardNum);
                    break;
                }
            }
            // Add the shard to the new group
            groupInfo.get(move.groupId).getRight().add(move.shardNum);
            configMap.put(++lastConfigNum,
                    new ShardConfig(lastConfigNum, groupInfo));
            return new Ok();
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // TODO: implement Query ...
            if (!configMap.isEmpty()) {
                // check ID of the query

                if (query.configNum() == -1) {
                    // Return latest config
                    return configMap.get(lastConfigNum);
                } else if (query.configNum() > lastConfigNum) {
                    return configMap.get(lastConfigNum);
                } else {
                    // Return config with number query.configNum()
                    if (query.configNum() - 1 >= lastAdoptedConfigNum ) {
                        lastAdoptedConfigNum = query.configNum() - 1;
                    }
                    return configMap.get(query.configNum());
                }
            } else {
                return new Error();
            }

        }

        throw new IllegalArgumentException();
    }

    private void balance(
            Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo,
            Set<Integer> shardsToMove, int numShardsPerGroup,
            Iterator<Integer> it) {
        int groupId = it.next();
        if (groupInfo.get(groupId).getRight().size() < numShardsPerGroup) {
            int numShardsToAdd = numShardsPerGroup -
                    groupInfo.get(groupId).getRight().size();
            Iterator<Integer> it2 = shardsToMove.iterator();
            while (it2.hasNext() && numShardsToAdd > 0) {
                groupInfo.get(groupId).getRight().add(it2.next());
                it2.remove();
                numShardsToAdd--;
            }
        }
    }

    /* -------------------------------------------------------------------------
    Utils
   -----------------------------------------------------------------------*/
    // TODO: add utils here ...
}
