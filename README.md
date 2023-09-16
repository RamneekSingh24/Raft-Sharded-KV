# Fault-tolerant Sharded Key-Value Storage service

Built as a part of [MIT-6.824 Distributed Systems Labs](https://pdos.csail.mit.edu/6.824/)

- [x] Lab 1: MapReduce (Warmup / Practice exercise)
- [x] Lab 2: Raft Protocol
- [x] Lab 3: Fault-tolerant Key/Value Service
- [x] Lab 4: Sharded Key/Value Service


## Key Features
- `Put / Get / Append` calls
- **Replicated & Fault Tolerant**: Able to serve requests as long as a majority of servers are up and can communicate, inspite of other failures or network partitions.
- **Linearizabile**: Users can assume that they are talking to a single machine and that all the requests are processed in a single global order. A call will also observe the effects of calls that have completed before it starts. 
- **Scalable**: Supports dynamically adding/reconfiguring servers and shards for boosting performance, with zero downtime, i.e. the requests on unaffected keys can keep going on during the reconfiguration. 


## Implementation
### RAFT Consensus Algorithm
![image](https://github.com/RamneekSingh24/Raft-Sharded-KV/assets/74413910/6ab1baea-87fb-48a0-b979-d67c70c118c7)  

The Key Value Service uses the [RAFT Consensus Algorithm](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14) to maintain a replicated, fault tolerant and consistent state across peers.  
As described in the RAFT Paper, we implement a leader election and a replicated log. The log and some other necessary state is persisted for handling fail-overs and maintaining consistency.  

As an optimization for improving memory utilization and recovery times, we implement a **snap-shotting and log compaction mechanism** all while keeping the state consistent across fail-overs.  
We also implement other optimizations like log-conflicted detection (mentioned in the [Extended RAFT paper](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14)) that aims to reduce the number of RPCs between peers.  

### Key Value Service
Next, on top of RAFT we build a simple Key Value storage service that supports Put/Get/Append calls and manages snap-shotting and log compaction whenever the memory used by the log approaches a threshold.  
The service is consistent and gracefully handles failures and client retries. The client request are idempotent and the service makes sure that duplicate requests/retries and only executed once, across server failures and snapshots!

<img width="835" alt="image" src="https://github.com/RamneekSingh24/Raft-Sharded-KV/assets/74413910/e4a529e0-7c28-4ac7-93a2-69aa5e8ba64a">

### Sharding the Key Value Store
Since Linearizability is Local/Composable ([Ref: Section 3.3](http://www.cs.cmu.edu/~awing/publications/CMU-CS-88-120.pdf)), we can scale the system by shading the key space across different and independent group of RAFT Peers.  
We can partition the servers into different replica-groups of RAFT Peers, where each group with handle a set of shards and execute independently. This allows us to scale the throughput by adding more groups/servers.  

We implement a `Shard Controller` service that manages the sharding config, i.e. mapping of shard to replica group. 
The service is again built on top of RAFT to provide high availability and fault tolerance. This service supports `Join`, `Leave` RPCs to add/remove replica groups, `Move` RPC to migrate a shard from one group to another, and `Query` RPC to query the configuration.  
`Join` and `Leave` will evenly distribute the shards across groups.  

The service as a whole provides linearizability to the clients even when shards are being moved around and reconfigured.  
During a reconfiguration of shards, the clients do no perceive any downtime for the unaffected keys!
