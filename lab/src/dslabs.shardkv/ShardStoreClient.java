package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    //TODO: declare fields for your implementation ...


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
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // TODO: send command ...
    }

    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return false;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        return null;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
        // TODO: handle shard store reply ...
    }

    // TODO: your message handlers ...


    // TODO: add utils here ...


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
    }

    // TODO: add your time handlers ...
}
