# Fluent::Plugin::Kafka

TODO: Write a gem description
TODO: Also, I need to write tests

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-kafka'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-kafka

## Usage

### Input plugin (@type 'kafka')

    <source>
      @type  kafka
      host   <broker host>
      port   <broker port: default=9092>
      topics <listening topics(separate with comma',')>
      format <input text type (text|json|ltsv|msgpack)>
      message_key <key (Optional, for text format only, default is message)>
      add_prefix <tag prefix (Optional)>
      add_suffix <tag suffix (Optional)>
      max_bytes           (integer)    :default => nil (Use default of Poseidon)
      max_wait_ms         (integer)    :default => nil (Use default of Poseidon)
      min_bytes           (integer)    :default => nil (Use default of Poseidon)
      socket_timeout_ms   (integer)    :default => nil (Use default of Poseidon)
    </source>

Supports following Poseidon::PartitionConsumer options.

- max_bytes — default: 1048576 (1MB) — Maximum number of bytes to fetch
- max_wait_ms — default: 100 (100ms) — How long to block until the server sends us data.
- min_bytes — default: 1 (Send us data as soon as it is ready) — Smallest amount of data the server should send us.
- socket_timeout_ms - default: 10000 (10s) - How long to wait for reply from server. Should be higher than max_wait_ms.

Supports a start of processing from the assigned offset for specific topics.

    <source>
      @type  kafka
      host   <broker host>
      port   <broker port: default=9092>
      format <input text type (text|json|ltsv|msgpack)>
      <topic>
        topic       <listening topic>
        partition   <listening partition: default=0>
        offset      <listening start offset: default=-1>
      </topic>
      <topic>
        topic       <listening topic>
        partition   <listening partition: default=0>
        offset      <listening start offset: default=-1>
      </topic>
    </source>

See also [Poseidon::PartitionConsumer](http://www.rubydoc.info/github/bpot/poseidon/Poseidon/PartitionConsumer) for more detailed documentation about Poseidon.

### Input plugin (@type 'kafka_group', supports kafka group)

    <source>
      @type   kafka_group
      brokers <list of broker-host:port, separate with comma, must set>
      zookeepers <list of broker-host:port, separate with comma, must set>
      zookeeper_path <broker path in zookeeper> :default => /brokers/ids # Set path in zookeeper for brokers
      consumer_group <consumer group name, must set>
      topics <listening topics(separate with comma',')>
      format <input text type (text|json|ltsv|msgpack)>
      message_key <key (Optional, for text format only, default is message)>
      add_prefix <tag prefix (Optional)>
      add_suffix <tag suffix (Optional)>
      max_bytes           (integer)    :default => nil (Use default of Poseidon)
      max_wait_ms         (integer)    :default => nil (Use default of Poseidon)
      min_bytes           (integer)    :default => nil (Use default of Poseidon)
      socket_timeout_ms   (integer)    :default => nil (Use default of Poseidon)
    </source>

Supports following Poseidon::PartitionConsumer options.

- max_bytes — default: 1048576 (1MB) — Maximum number of bytes to fetch
- max_wait_ms — default: 100 (100ms) — How long to block until the server sends us data.
- min_bytes — default: 1 (Send us data as soon as it is ready) — Smallest amount of data the server should send us.
- socket_timeout_ms - default: 10000 (10s) - How long to wait for reply from server. Should be higher than max_wait_ms.

See also [Poseidon::PartitionConsumer](http://www.rubydoc.info/github/bpot/poseidon/Poseidon/PartitionConsumer) for more detailed documentation about Poseidon.

### Output plugin (non-buffered)

    <match *.**>
      @type               kafka

      # Brokers: you can choose either brokers or zookeeper.
      brokers             <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,.. # Set brokers directly
      zookeeper           <zookeeper_host>:<zookeeper_port> # Set brokers via Zookeeper
      zookeeper_path      <broker path in zookeeper> :default => /brokers/ids # Set path in zookeeper for kafka
      default_topic       <output topic>
      default_partition_key (string)   :default => nil
      output_data_type    (json|ltsv|msgpack|attr:<record name>|<formatter name>)
      output_include_tag  (true|false) :default => false
      output_include_time (true|false) :default => false
      max_send_retries    (integer)    :default => 3
      required_acks       (integer)    :default => 0
      ack_timeout_ms      (integer)    :default => 1500
      compression_codec   (none|gzip|snappy) :default => none
    </match>

Supports following Poseidon::Producer options.

- max_send_retries — default: 3 — Number of times to retry sending of messages to a leader.
- required_acks — default: 0 — The number of acks required per request.
- ack_timeout_ms — default: 1500 — How long the producer waits for acks.
- compression_codec - default: none - The codec the producer uses to compress messages.

See also [Poseidon::Producer](http://www.rubydoc.info/github/bpot/poseidon/Poseidon/Producer) for more detailed documentation about Poseidon.

This plugin supports compression codec "snappy" also.
Install snappy module before you use snappy compression.

    $ gem install snappy

#### Load balancing

Messages will be sent broker in a round-robin manner as default by Poseidon, but you can set `default_partition_key` in config file to route messages to a specific broker.
If key name `partition_key` exists in a message, this plugin set its value of partition_key as key.

|default_partition_key|partition_key| behavior |
|-|-|
|Not set|Not exists| All messages are sent in round-robin |
|Set| Not exists| All messages are sent to specific broker |
|Not set| Exists | Messages which have partition_key record are sent to specific broker, others are sent in round-robin|
|Set| Exists | Messages which have partition_key record are sent to specific broker with parition_key, others are sent to specific broker with default_parition_key|


### Buffered output plugin

    <match *.**>
      @type               kafka_buffered

      # Brokers: you can choose either brokers or zookeeper.
      brokers             <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,.. # Set brokers directly
      zookeeper           <zookeeper_host>:<zookeeper_port> # Set brokers via Zookeeper
      zookeeper_path      <broker path in zookeeper> :default => /brokers/ids # Set path in zookeeper for kafka
      default_topic       <output topic>
      default_partition_key (string)   :default => nil
      flush_interval      <flush interval (sec) :default => 60>
      buffer_type         (file|memory)
      output_data_type    (json|ltsv|msgpack|attr:<record name>|<formatter name>)
      output_include_tag  (true|false) :default => false
      output_include_time (true|false) :default => false
      max_send_retries    (integer)    :default => 3
      required_acks       (integer)    :default => 0
      ack_timeout_ms      (integer)    :default => 1500
      compression_codec   (none|gzip|snappy) :default => none
    </match>

Supports following Poseidon::Producer options.

- max_send_retries — default: 3 — Number of times to retry sending of messages to a leader.
- required_acks — default: 0 — The number of acks required per request.
- ack_timeout_ms — default: 1500 — How long the producer waits for acks.
- compression_codec - default: none - The codec the producer uses to compress messages.

See also [Poseidon::Producer](http://www.rubydoc.info/github/bpot/poseidon/Poseidon/Producer) for more detailed documentation about Poseidon.

This plugin supports compression codec "snappy" also.
Install snappy module before you use snappy compression.

    $ gem install snappy

#### Load balancing

Messages will be sent broker in a round-robin manner as default by Poseidon, but you can set `default_partition_key` in config file to route messages to a specific broker.
If key name `partition_key` exists in a message, this plugin set its value of partition_key as key.

|default_partition_key|partition_key| behavior |
|-|-|
|Not set|Not exists| All messages are sent in round-robin |
|Set| Not exists| All messages are sent to specific broker |
|Not set| Exists | Messages which have partition_key record are sent to specific broker, others are sent in round-robin|
|Set| Exists | Messages which have partition_key record are sent to specific broker with parition_key, others are sent to specific broker with default_parition_key|

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
