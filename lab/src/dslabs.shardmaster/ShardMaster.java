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
    
    public ShardMaster(int numShards) {
        this.numShards = numShards;

        // TODO: initial fields ...
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
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // TODO: implement Leave ...
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // TODO: implement Move ...
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // TODO: implement Query ...
        }

        throw new IllegalArgumentException();
    }

    /* -------------------------------------------------------------------------
    Utils
   -----------------------------------------------------------------------*/
    // TODO: add utils here ...
}
