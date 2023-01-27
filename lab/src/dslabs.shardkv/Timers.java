package dslabs.shardkv;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // TODO: add fields for client request ...
}

// TODO: add more timers here ...