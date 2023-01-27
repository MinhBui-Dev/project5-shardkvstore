package dslabs.shardkv;

import dslabs.framework.Address;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // TODO: declare fields for your implementation ...

    /* -------------------------------------------------------------------------
    Construction and initialization
   -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // TODO: ...
    }

    @Override
    public void init() {
        // TODO: initialize fields ...
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // TODO: handle paxos request ...
    }

    // TODO: your message handlers ...


    // TODO: your command process ...
    

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // TODO: your time handlers ...
    

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
}
