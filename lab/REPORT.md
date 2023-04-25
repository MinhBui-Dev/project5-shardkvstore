# Project 5: Report
## Intro (Your understanding)
In Lab 4, we set up our KVStore service to be fault-tolerance by implementing a Paxos consensus algorithm. However, our paxos server group could only process one client's request at the time.
In this lab, we strike to improve our request throughput by implementing a distributed sharded KVStore service. We will use a consistent hashing algorithm to shard our key-value pairs across multiple servers groups. 
By doing so, we can achieve a higher throughput by processing multiple client's requests at the same time. These server groups will still be fault-tolerant by using the Paxos consensus algorithm.
In order for client to know which server group to send their request to, we will implement a shardmaster service, which also acts as a load balancer. The shardmaster service will be responsible for assigning shards to server groups. 
The shardmaster service will also be fault-tolerant by using the Paxos consensus algorithm.

## Flow of Control & Code Design
### Shardmaster
The shardmaster service is responsible for assigning shards to server groups. It will also be responsible for rebalancing shards when a server group joins or leaves the system.

#### Query
Query(n) will return the nth configuration. If n is -1 or bigger than the largest known configuration number, it should return the latest configuration.

#### Move
Move(gid, shard) will move the shard to the group gid. This will cause the shardmaster to create a new configuration that includes the new assignment. If the new assignment cannot be completed, Move should return Error().
A few case in which the new assignment cannot be completed:
- gid is not in the current configuration
- shard is -1 or bigger than the largest possible shard number (NShards)
- the shard is already in the group gid

#### Join/Leave
Join(gid, servers) will introduce a new group with group ID gid and a list of servers. The shardmaster will create a new configuration that includes the new group.
Shardmaster will also move shards around to balance the load with the goal to move as few shards as possible. If the new assignment cannot be completed, Join should return Error().
A few case in which the new assignment cannot be completed:
- gid is already in the current configuration
- servers list is empty

Leave(gid) will remove the group with group ID gid. The shardmaster will create a new configuration that does not include the group. 
Shardmaster will also move shards around to balance the load with the goal to move as few shards as possible. If the new assignment cannot be completed, Leave should return Error().
A few case in which the new assignment cannot be completed:
- gid is not in the current configuration

### ShardStoreServer
The shardstore server is responsible for storing key-value pairs and processing client's request using the pair of message `ShardStoreRequest` and `ShardStoreReply`. 
The shardstore server will query the shardmaster server to know which shard it is responsible for using `PaxosRequest` and `PaxosReply` message.
Once shardstore server know what the current shard configuration is, it will communicate with other replicated server groups using `ShardMoveRequest` and `ShardMoveReply` message to move shards around accordingly. 
Any message that shardstore server receives should be pre-process to de-duplicate the message or to check if the message is valid.
Afterward, the message is passed to the paxos subsystem to be processed. The paxos subsystem will propose and reach consensus on the message, thus recording it in the `PaxosLog`.
This way, any state change to the server will always be recorded in the `PaxosLog` and can be replayed/replicated to other servers.

the workflow of the shardstore server is as follow:
in `HandleXXXMessage`:
- pre-process the message such as checking if the client's command has already been processed (`AMOApp.alreadyExecuted()`) or if the message is duplicated. 
- if the message is valid, build the corresponding command call `process(Command, false)`

in `process(Command, bool)`:
- the switch-like structure will call the corresponding function to process the command
```java
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
```
in each `processXXX(Command, bool)`:
- if the command is not replicated, we will propose the command to the paxos subsystem using `PaxosRequest`: `New PaxosRequest(Id, sequence, command)`
- Paxos algo is designed to filter out command that has already been proposed. Thus we don't need to worry about the same command being proposed multiple times by different server in the group. This will work if we designate the ID of the command to be the ID the group or the ID of the client.
- Sequence number could be either the sequence number of the client in case its a client command or the configuration number for other commands.
- if the command is replicated, we will directly process the command according to the command type.
```java
    private void processXXX(XXX command, boolean replicated){
    
        if(!replicated){
            PaxosRequest request=new PaxosRequest(ID,sequence,command);
            this.handleMessage(request,paxosAddress);
            return;
        }

        // process the command and change the state of the server
    }
```
we receive decided command from the Paxos subsystem in `handlePaxosDecision`:
- we will just send the command to process(command, bool). However, this time we will set the bool to true since the command is already replicated.
```java
    private void handlePaxosDecision(PaxosDecision m, Address sender) {
        process(m.command(),true);
    }
```
#### Timer
We only implement 1 timer for the shardstore server. The timer will periodically send a query command to the shardmaster server to get the next configuration.
Also, if there is shardsToMove that we have not received ack for, the server groups will resend `shardmoverequest` messages to the intended groups.

### ShardStoreClient
ShardStoreClient starts out by requesting the shardmaster server for the latest configuration using `PaxosRequest` message and `Query(-1_)` command.
If client received error() from the shardmaster server, it will wait for a while and retry the query on timer. 
The client finds out the corresponding server using the `getGroupIdForShard(key)` function which calls the `keytoshard(key)` function and check the configuration to find out which group is responsible for the shard.
Afterward, the client will send the request to the corresponding shardstore server using `ShardStoreRequest` message. 
The client will check periodically if the result is ready using a timer and resend the request if the result is not ready yet.
If the client received `ErrorOutOfDate()` from the shardstore server, it will query the shardmaster server again to get the latest configuration and retry the request.

## Design Decisions

### ShardStoreSever
- We use a `PaxosLog` to record all the state change of the server. This way, we can consistently replicate the state change across all servers in the group. 
- We also try to de-duplicate the message by checking server state before passing the message to the paxos subsystem. This way, we can avoid unnecessary paxos proposal. 
However, paxos logs act like a queue, thus we can't avoid passing some message to the paxos subsystem while the same message is being decided. The de-duplication is then handled by the paxos subsystem.
- We implement the KVStore wrapped in `AMOApplication` such that for each shard, we have a separate `KVStore`. This way, we can easily move shards around without having to worry about the state of the `KVStore`.
- We drop client's request if the configuration is higher than the current configuration and only send back `ErrorOutOfDate()` to the client if the configuration is lower than the current configuration.
This is intended so that the client can just retry; and hopefully we will have the same configuration as the client by then. If we reply back with `ErrorOutOfDate()`, client might end up getting an even later configuration and have to wait longer.
- Once we perform a shard move, we will actively respond with `ErrorOutOfDate()` to client's request if it is for the shard that we are moving. This results in client getting the latest configuration faster.
- For the case where we are waiting for a shard from another server and receive client's request for that shard, we will process the request through paxos layer and add it to a heard-command list. 
Once that specific shard is move over, we can iterate through the map of heard-command and process the command for client's request right away.
- Using configuration number and groupId+command-type to identify the command and sequence number in paxos request. This way, we can avoid proposing the same command multiple times or paxos subsystem filtering out requests that are not duplicated.

### ShardStoreClient
- We use monotonic sequence number to identify the command. This way, we can avoid sending the same command multiple times to the shardstore server.
- We use a timer to periodically check on the command. if the retry count is higher than the threshold, we will request a new configuration from the shardmaster server and retry the command.
- Using the same timer to periodically check the result for query and retry the query if the result is not ready yet.

### Shardmaster
- All commands are validated before process by the shardmaster server. 
- The server is actually an application that is wrapped in the paxos servers group. Thus, any command that arrives at the actual application has already been decided by the paxos layer.
- To minimize the shards' movement in `Join` command, we calculate the ideal number of shards for each group and only move shards if the number of shards in a group is more than the ideal number. 
However, we also check to make sure that the total number of shards move is equal to the ideal number of shards.
- As for `Leave` command, we also minimize the shards' movement by moving the shards from the leaving group to the group with the least number of shards and distribute the shards evenly.

## Missing Components
We did not implement the `TransactionalKVStore` since we did not have enough time to implement it. 
However, Since we design the shardstore server to have a separate `KVStore` for each shard, implementing `TransactionalKVStore` should be straightforward.
We would need to split up the keyset into multiple keysets for each shard and send the request to the corresponding shardstore server. 
This would also be implemented with 2PC protocol to lock the shards that are involved in the transaction.
The granularity of the lock would be at the shard level in order to support multiple transactions on different shards at the same time.
We could also do a key-level lock if we want to support multiple transactions on the same shard at the same time. 
However, the tradeoff is that we would need to keep track of the locks for each key and shard, thus increasing the complexity of the system.

## References
- [Project 5 Readme](https://github.gatech.edu/omscs7210-spr23/project5-shardkvstore/blob/main/lab/README.md#part-1-shard-master)
- [Matt's strategy on tackling the project](https://github.gatech.edu/gist/mzuba3/3110647835db42c1e32bb2fac8c1d8e2)
- [Ellis Michael's 452 slides](https://ellismichael.com/dslabs/452%20Discussion%20Section%20Slides.pdf)

## Extra (Optional)