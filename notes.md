# Kafka

## 1 Topics

- A particular stream of DATA.
- you can have as many topics as you want.
- it's `id`'ed by it's `name`.
- Kinda like a table in a database.
- but topic cannot be queried.
- we use producers and consumers to work the data stream handling.
- any kind of message format can be used.
- The sequnce of messages is called **data stream**

### 1.1 Partitions & Offsets

- Topics are split into separate partitions
  - Messages within each partition are ordered.
  - Each message within a partition gets an incremental id called `offset`

- Kafka topics are *immutable*; :o
- Data is kept for a limited time (limitation is configurable); :o
- offset only have meaning for a specific partition
  - hence order is guaranteed only within a partition.
- you can have as many topics as you want.

## 2 Producers

- *writes/sends* data to topics (made of partitions)
- knows in advance to which partition to write to (and which broker has it).
- In case of kafka broker failures, Producers will automatically recover.
  - since many partitions are used, this load is balanced to many broker.
- note: producers determines the partition it writes to, not the other way.

### 2.1 Message Keys

- producer can choose to send a `key` with the `message`. **(!important)**
- if `key=null`, data is sent round robin (partition 0 -> 1 -> 2 ...)
  - hence load balancing is acheived.
- if `key!=null`, then all messages will always go to same partition.
- A key is typically sent, if you need message ordering for a specific filed.
  eg: `(key='truck_id')`
- Kafka message structure created by producer:
  - key: binary (can be `null`)
  - value: binary (can be `null`)
  - compression_type: `#{none, gzip, zstd...}`
  - headers(optional): key-value pairs
  - Partiton + offset
  - Timestamp
- misc: take a look kafka-partitioner
- serializes key value into bytes before writing to partition.

## 3 Consumers

- reads data from topic (`id`'ed by `name`) - Pull Model.
  - i.e., request data from topic, (not the other way around)
- data is read in order from low to high `offset` within each partitions
- consumers automatically knows which broker to read from.
- and it also recovers itself on broker failures.
- the (de)serializtion must not be changes during topic's lifetime.
  - create new topic instead

### 3.1 Consumer Groups

- all consumers in an application read data as groups.
- Each consumer within a group reads from exclusive partitions.
- But different groups can read from topic.
- eg, let's say group has 3 consumers. each will have exclusive partiton to read from:
  - consumer-group-app
    - consumer 1
      - partition 0
      - partition 1
    - consumer 2
      - partition 2
    - consumer 3
      - partition 3
      - partition 4
      - partition 5
- if no of consumers > no of partitions then it'll stay in-active.

### 3.2 Consumer offsets

- kafka stores the offsets at which a consumer group has been reading.
- these offsets are commited in kafka `topic` named `__consumer_offsets`
- the consumers periodically commits the offset.
- **Delivery semantics**:
  - At least once
    - offsets are commited after message is processed
  - At most once
    - offsets are commited as soon as message is received.
  - Exactly once

## 4 Brokers

- A kafka cluster is composed of multiple brokers (just servers).
- Each broker has an ID (int).
- Each broker contains certain topic partitions.
  - data is spread across brokers.
- After connection to any broker (bootstrap server), the client is connected to the entire cluster.
  - Each brokers knows about all brokers, topics and partitions (metadata)
  - This makes all brokers has bootstrap server.
  - Hence connecting to one broker will connect the clinet to the entire cluster, cause that single broker will hava all the info.

### 4.1 how brokers related to topics & partitions

- note: data is fully distributed. (horizontal scalling)
- eg:
  - let's say we have 2 topics `TOPIC A` and `TOPIC B`
  - TOPIC A has 3 partitions(0,1,2)
  - TOPIC B has 2 partitions(0,1)
  - and we have 3 brokers(101,102,103)
  - b101
    - tA.p0
    - tB.p1
  - b102
    - tA.p2
    - tB.p0
  - b103
    - tA.p1

### 4.2 Topic Replication Factor

- usually, replication factor must be set to > 1.
- to have topics & partitions replicated/duplicated/copied across different brokers. (ISR -> in-sync-replica)
- this is helpful when a broker fails, it's topic partition can then be accessed from other brokers

- **CONCEPT OF LEADER FOR PARTITION**
  - At any time only one broker can be a leader for a given partition.
  - producers can only send data to the leader broker for that partition.
  - same applies for consumers. (though it is configuarable.)

## 5 Producer Acknowledgements (acks)

- producers can choose to receive ack of data write:
  - `acks=0`: wont wait for act
  - `acks=1`: will wait for leader ack
  - `acks=all`: will wailt for all ack (leader + replicas)

## 6 Kafka topic durability

- for topic replication factor = 3, topic data durability = 2. (can withstand 2 broker loss). since if we have 3 broker(replicas) and 2 fails we will still have 1(durability).
- so, topic durability = N - 1, where N = topic replication factor.

## 7 Zookeeper/Kraft

- Kafka 2.x can't work without zookeeper.
- it manages brokers (keeps a list of them)
- it has leader (Writer) and the rest of the servers are followers (readers).
- it by design operates on odd number of servers(1,3,5,7)
- it helps in performing leader election for partitions.
- it sends notifications to kafka in case of changes.

- kraft will replace zookeeper from kafka 4.x
