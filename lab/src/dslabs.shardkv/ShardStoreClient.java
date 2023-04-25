package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS;
import dslabs.shardkv.ShardStoreServer.ErrorOutOfDate;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    //TODO: declare fields for your implementation ...
    ShardConfig currentConfig;
    Command command;
    Result result;
    private int sequenceNumber;
    private int timeout;
    private boolean gettingNewConfig;


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
    }

    @Override
    public synchronized void init() {
        // TODO: initialize fields ...
        currentConfig = null;
        command = null;
        result = null;
        sequenceNumber = 0;
        getNewConfig();
        set(new ClientTimer(), CLIENT_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // TODO: send command ...
        sequenceNumber++;
        this.command = command;
        this.result = null;
        if (currentConfig != null) {
            timeout = 0;
            requestCommand();
        }
    }

    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        while (result == null) {
            wait();
        }
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
        // TODO: handle shard store reply ...
        if (command != null && sequenceNumber == m.sequenceNumber()) {
            if (m.result() instanceof ErrorOutOfDate) {
                if (gettingNewConfig) {
                    return;
                }
                getNewConfig();
            } else {
                AMOResult amoResult = (AMOResult) m.result();
                result = amoResult.result();
                command = null;
                notifyAll();
            }
        }
    }

    // TODO: your message handlers ...
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // TODO: handle paxos reply from shard master ...
        if (m.result() instanceof Error) {
            return;
        }
        if (currentConfig == null || gettingNewConfig && ((ShardConfig) m.result()).configNum() >= currentConfig.configNum()) {
            currentConfig = (ShardConfig) m.result();
            gettingNewConfig = false;
            if (command != null) {
                timeout = 0;
                requestCommand();
            }
        }
    }

    private void requestCommand() {
        ShardStoreRequest request =
                new ShardStoreRequest(currentConfig.configNum(), this.address(),
                        sequenceNumber, this.command);
        String key = ((SingleKeyCommand) command).key();
        int groupId = getGroupIdForShard(key);
        Set<Address> servers = currentConfig.groupInfo().get(groupId).getLeft();
        //System.out.println("Client Request: " + request + " and config: " + currentConfig);
        for (Address server : servers) {
            send(request, server);
        }
    }

    // TODO: add utils here ...
    private void getNewConfig() {
        gettingNewConfig = true;
        PaxosRequest request = new PaxosRequest(this.address().toString(),sequenceNumber, new Query(-1));
        broadcastToShardMasters(request);
    }

    /**
     * Returns the groupId of the shard that the given key belongs to.
     * @param key
     * @return groupId of shard that key belongs to
     */
    private synchronized int getGroupIdForShard(String key){
        int shardId = keyToShard(key);
        int groupId = -1;

        // Find group id for shard from current configuration
        for (Integer integer : currentConfig.groupInfo().keySet()) {

            if (currentConfig.groupInfo().get(integer).getRight()
                             .contains(shardId)) {
                groupId = integer;
                break;
            }
        }
        return groupId;
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
        if (gettingNewConfig || timeout >= 3) {
            getNewConfig();
        } else if (command != null) {
            /*if (this.address().toString().equals("client3")) {
                System.out.println("Client Request: " + command + " and config: " + currentConfig + " and sequence number: " + sequenceNumber);
            }*/
            requestCommand();
            timeout++;
        }
        set(t, CLIENT_RETRY_MILLIS);
    }
    // TODO: add your time handlers ...
}
