package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosDecision;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.shardkv.ServerTimer.SERVER_RETRY_MILLIS;
import static dslabs.shardmaster.ShardMaster.INITIAL_CONFIG_NUM;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // TODO: declare fields for your implementation ...
    public interface ShardStoreServerCommand extends Command {
    }
    @Data
    public final static class NewConf implements ShardStoreServerCommand {
        private final ShardConfig config;
    }
    @Data
    public final static class ShardMove implements ShardStoreServerCommand {
        private final String groupId;
        private final int configNum;
        private final Map<Integer, AMOApplication<Application>> shards;
    }
    @Data
    public final static class ShardMoveAck implements ShardStoreServerCommand {
        private final String groupId;
        private final int configNum;
    }
    @Data
    public final static class ErrorOutOfDate implements Result {
    }
    // Create and initialize paxos server
    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;

    private Map<Integer, AMOApplication<Application>> shardsMap; // <shardId, KVStore>
    private Set<Integer> shardsNeeded; // shards needed to be moved
    private Set<String> groupsCompleted; // groupIds of groups that have completed move
    private Map<Integer, HashMap<Integer,AMOApplication<Application>>> shardsToMove; // <groupId, HashMap<shardId, Shards>>
    private HashMap<Integer,Set<AMOCommand>> heardCommandsList; // commands heard from client while in transition
    private ShardConfig currentConfig, newConfig;
    private int timeout;

    /* -------------------------------------------------------------------------
    Construction and initialization
   -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;


    }

    @Override
    public void init() {
        // Setup Paxos
        paxosAddress = Address.subAddress(this.address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        // request new config
        currentConfig = null;
        PaxosRequest request = new PaxosRequest(Integer.toString(groupId), INITIAL_CONFIG_NUM, new Query(INITIAL_CONFIG_NUM));
        broadcastToShardMasters(request);
        set(new ServerTimer(), SERVER_RETRY_MILLIS);

        this.shardsNeeded = new HashSet<>();
        this.groupsCompleted = new HashSet<>();
        this.shardsToMove = new HashMap<>();
        this.shardsMap = new HashMap<>();
        this.heardCommandsList = new HashMap<>();
        this.timeout = 0;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Check if shard is in shardsMap
        if (currentConfig == null) {
            PaxosRequest request = new PaxosRequest(Integer.toString(groupId), INITIAL_CONFIG_NUM, new Query(INITIAL_CONFIG_NUM));
            broadcastToShardMasters(request);
            return;
        }
        SingleKeyCommand command = (SingleKeyCommand) m.command();
        int shardId = keyToShard(command.key());
        if (shardsMap.containsKey(shardId)) {

            AMOCommand amoCommand =
                    new AMOCommand(m.command(), m.address(),
                            m.sequenceNumber());
            // check if duplicate
            if (shardsMap.get(shardId).alreadyExecuted(amoCommand)) {
                // duplicate
                ShardStoreReply reply =
                        new ShardStoreReply(m.address(),
                                m.sequenceNumber(),
                                shardsMap.get(shardId).execute(amoCommand));
                send(reply, sender);
            } else {
                // not duplicate
                // check to see if in reconfiguration
                if (shardsToMove.size() > 0) {
                    for (int groupId : shardsToMove.keySet()) {
                        if (shardsToMove.get(groupId).containsKey(shardId)) {
                            return;
                        }
                    }
                }
                process(amoCommand, false);
            }
        } else if (shardsNeeded.contains(shardId)) {
            // in side shardsNeeded, store heard command. will process when shard is moved
            AMOCommand amoCommand = new AMOCommand(m.command(), m.address(), m.sequenceNumber());
            process(amoCommand, false);
        } else if (m.configNum() > currentConfig.configNum()) {
/*            if (m.configNum() == currentConfig.configNum() + 1) {
                AMOCommand amoCommand = new AMOCommand(m.command(), m.address(), m.sequenceNumber());
                process(amoCommand, false);
            }*/
        } else {
            ErrorOutOfDate error = new ErrorOutOfDate();
            ShardStoreReply reply =
                    new ShardStoreReply(m.address(), m.sequenceNumber(), error);
            send(reply, sender);
        }
    }

    private void handleShardMoveRequest(ShardMoveRequest m, Address sender) {

        if (currentConfig != null && m.config().configNum() <= currentConfig.configNum()) {
            ShardMoveReply reply = new ShardMoveReply(m.groupId(),m.config(),Integer.toString(groupId));
            send(reply,sender);
        }
        // check if same config and not duplicate groupId
        if (newConfig != null && m.config().configNum() == newConfig.configNum()) {
            if (!groupsCompleted.contains(m.groupId())) {
                ShardMove shardMove = new ShardMove(m.groupId(),m.config().configNum(),m.shards());
                process(shardMove,false);
            } else {
                ShardMoveReply reply = new ShardMoveReply(m.groupId(),m.config(),Integer.toString(groupId));
                send(reply,sender);
            }
        }
    }

    private void handleShardMoveReply(ShardMoveReply m, Address sender) {
        if (shardsToMove.isEmpty()) {
            return;
        }
        if (m.config().configNum() == newConfig.configNum() && shardsToMove.containsKey(Integer.parseInt(m.groupIdAck()))) {
            ShardMoveAck ack = new ShardMoveAck(m.groupIdAck(),m.config().configNum());
            process(ack, false);
        }
    }

    private void handlePaxosReply(PaxosReply m, Address sender) {

        // TODO: Maybe filter out duplicate replies
        if (m.result() instanceof Error) {
            return;
        }
        ShardConfig config = (ShardConfig) m.result();
        if (currentConfig == null) {
            NewConf newConf = new NewConf(config);
            process(newConf, false);
        } else if (config.configNum() > currentConfig.configNum() && newConfig == null) {
            NewConf newConf = new NewConf((ShardConfig) m.result());
            process(newConf, false);
        }
    }

    private void handlePaxosDecision(PaxosDecision m, Address sender) {
        process(m.command(),true);
    }

    // Process command
    private void process(Command command, boolean replicated) {
        if (command instanceof ShardMove) {
            processShardMove((ShardMove) command, replicated);
        } else if (command instanceof ShardMoveAck) {
            processShardMoveAck((ShardMoveAck) command, replicated);
        } else if (command instanceof NewConf) {
            processNewConf((NewConf) command, replicated);
        } else if (command instanceof AMOCommand) {
            processAMOCommand((AMOCommand) command, replicated);
        } else {
            throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    private void processShardMove(ShardMove m, boolean replicated) {
        if (!replicated) {
            PaxosRequest request = new PaxosRequest(m.groupId()+"-ShardMove", m.configNum(), m);
            this.handleMessage(request, this.paxosAddress);
            return;
        }

        if (groupsCompleted.contains(m.groupId()) || m.configNum() == currentConfig.configNum()) {
            return;
        }
        // Add shards to ShardsMap
        for (int shardId : m.shards.keySet()) {
            AMOApplication shard = null;
            try {
                shard = new AMOApplication<>(AMOApplication.class.cast(m.shards.get(shardId)));
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
            shardsMap.put(shardId, shard);
            shardsNeeded.remove(shardId);

            if (heardCommandsList.containsKey(shardId)) {
                AMOApplication<Application> store = shardsMap.get(shardId);
                for (AMOCommand command : heardCommandsList.get(shardId)) {
                    Result result = store.execute(command);
                    ShardStoreReply reply =
                            new ShardStoreReply(command.address(),
                                    command.sequenceNum(), result);
                    send(reply, command.address());
                }
                heardCommandsList.remove(shardId);
            }
        }

        for (Address address : currentConfig.groupInfo().get(Integer.parseInt(m.groupId())).getLeft()){
            ShardMoveReply reply = new ShardMoveReply(m.groupId(),newConfig,Integer.toString(groupId));
            send(reply,address);
        }
        // Add groupId to completed list
        groupsCompleted.add(m.groupId());

        // Check if all shards have been moved
        if (shardsNeeded.size() == 0 && shardsToMove.size() == 0){
            currentConfig = newConfig;
            newConfig = null;
            groupsCompleted.clear();
            timeout = 0;
        }
        // throw exception
    }

    private void processShardMoveAck(ShardMoveAck m, boolean replicated) {
        if (!replicated) {
            PaxosRequest request = new PaxosRequest(m.groupId()+"-ShardMoveAck", m.configNum(), m);
            this.handleMessage(request, paxosAddress);
            return;
        }

        // TODO: process shard move ack ...
        // remove shards from shardsToMove
        if (shardsToMove.containsKey(Integer.parseInt(m.groupId()))) {
            for (int shardId : shardsToMove.get(Integer.parseInt(m.groupId())).keySet()) {
                shardsMap.remove(shardId);
            }
            shardsToMove.remove(Integer.parseInt(m.groupId()));

            if (shardsNeeded.size() == 0 && shardsToMove.size() == 0){
                currentConfig = newConfig;
                newConfig = null;
            }
        }
    }

    private void processNewConf(NewConf m, boolean replicated) {
        if (!replicated) {
            PaxosRequest request = new PaxosRequest(Integer.toString(groupId)+"-NewConf", m.config().configNum(), m);
            this.handleMessage(request, paxosAddress);
            return;
        }

        // check shardsNeeded or shardsToMove
        newConfig = m.config();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                newConfig.groupInfo();
        if (newGroupInfo.containsKey(groupId)) {
            // check if initial config
            if (newConfig.configNum() == INITIAL_CONFIG_NUM) {
                // initial config - add shards to shardsMap
                for (int shardId : newGroupInfo.get(groupId).getRight()) {
                    KVStore store = new KVStore();
                    shardsMap.put(shardId, new AMOApplication<>(store));
                }
                currentConfig = newConfig;
                newConfig = null;
            } else {
                // not initial config
                // compare with current config for shardsNeeded and shardsToMove
                // check shardsNeeded
                for (int shardId : newGroupInfo.get(groupId).getRight()) {
                    if (!shardsMap.containsKey(shardId)) {
                        shardsNeeded.add(shardId);
                    }
                }

                // check shardsToMove
                moveShards(newGroupInfo);
                if (shardsNeeded.size() == 0 && shardsToMove.size() == 0) {
                    currentConfig = newConfig;
                    newConfig = null;
                }
            }
        } else if (shardsMap.size() > 0) {
            // remove shards from shardsMap
            moveShards(newGroupInfo);

        } else {
            currentConfig = newConfig;
            newConfig = null;
        }
    }

    private void moveShards(
            Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo) {
        for (int destinationGroupId : newGroupInfo.keySet()) {
            if (destinationGroupId != this.groupId) {
                for (int shardId : newGroupInfo.get(destinationGroupId).getRight()) {
                    if (shardsMap.containsKey(shardId)) {
                        if (!shardsToMove.containsKey(destinationGroupId)) {
                            shardsToMove.put(destinationGroupId, new HashMap<>());
                        }
                        shardsToMove.get(destinationGroupId)
                                    .put(shardId, shardsMap.get(shardId));
                    }
                }
                // send shard move request
                if (shardsToMove.size() > 0 && shardsToMove.containsKey(destinationGroupId)) {
                    // broadcast shard move request to all members of destination group
                    ShardMoveRequest request = new ShardMoveRequest(
                            Integer.toString(this.groupId), newConfig,
                            shardsToMove.get(destinationGroupId));
                    for (Address address : newGroupInfo.get(
                            destinationGroupId).getLeft()) {
                        send(request, address);
                    }
                }
            }
        }
    }

    private void processAMOCommand(AMOCommand m, boolean replicated) {
        if (!replicated) {
            PaxosRequest request =
                    new PaxosRequest(m.address().toString(), m.sequenceNum(),
                            m);
            this.handleMessage(request, paxosAddress);
            return;
        }

        // check if we still have the key
        int ShardId = keyToShard(((SingleKeyCommand) m.command()).key());
        if (shardsMap.containsKey(ShardId)) {
            if (shardsToMove.size() > 0) {
                for (int groupId : shardsToMove.keySet()) {
                    if (shardsToMove.get(groupId).containsKey(ShardId)) {
                        ErrorOutOfDate error = new ErrorOutOfDate();
                        ShardStoreReply reply = new ShardStoreReply(m.address(),
                                m.sequenceNum(), error);
                        send(reply, m.address());
                        return;
                    }
                }
            }
            AMOApplication<Application> KVStore = shardsMap.get(ShardId);
            Result result = KVStore.execute(m);
            ShardStoreReply reply =
                    new ShardStoreReply(m.address(), m.sequenceNum(), result);
            send(reply, m.address());
        } else if (shardsNeeded.contains(ShardId)){
            if (!heardCommandsList.containsKey(ShardId)) {
                heardCommandsList.put(ShardId, new HashSet<>());
            }
            heardCommandsList.get(ShardId).add(m);
        } else {
            ErrorOutOfDate error = new ErrorOutOfDate();
            ShardStoreReply reply = new ShardStoreReply(m.address(),
                    m.sequenceNum(), error);
            send(reply, m.address());
        }
    }
    

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // DO NOT CHANGE STATE IN A TIMER HANDLER


    private void onServerTimer(ServerTimer t) {
        if (currentConfig == null) {
            PaxosRequest request =
                    new PaxosRequest(Integer.toString(groupId), INITIAL_CONFIG_NUM, new Query(INITIAL_CONFIG_NUM));
            broadcastToShardMasters(request);
        } else {

            int configNum = currentConfig.configNum();
            int nextConfigNum = configNum + 1;
            //System.out.println("Group " + groupId + " is querying for config " + nextConfigNum+", ShardsNeeded: "+shardsNeeded+", ShardsToMove: "+shardsToMove+ ", currentConfig: "+currentConfig);
            PaxosRequest request = new PaxosRequest(Integer.toString(groupId), configNum, new Query(nextConfigNum));
            broadcastToShardMasters(request);
        }


        if (shardsToMove.size() > 0) {
            // TODO: resend shard move request
            for (int destinationGroupId : shardsToMove.keySet()) {
                ShardMoveRequest request = new ShardMoveRequest(Integer.toString(this.groupId), newConfig, shardsToMove.get(destinationGroupId));
                for (Address address : newConfig.groupInfo().get(destinationGroupId).getLeft()) {
                    send(request, address);
                }
            }
        }
        set(t, SERVER_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...

}
