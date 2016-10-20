# fluent-plugin-kafka, a plugin for [Fluentd](http://fluentd.org)

[![Build Status](https://travis-ci.org/htgc/fluent-plugin-kafka.svg?branch=master)](https://travis-ci.org/htgc/fluent-plugin-kafka)

A fluentd plugin to both consume and produce data for Apache Kafka.

TODO: Also, I need to write tests

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-kafka'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-kafka

## Requirements

- Ruby 2.1 or later
- Input plugins work with kafka v0.9 or later
- Output plugins work with kafka v0.8 or later

## Usage

### Input plugin (@type 'kafka')

Consume events by single consumer.

    <source>
      @type kafka

      brokers <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
      topics <listening topics(separate with comma',')>
      format <input text type (text|json|ltsv|msgpack)> :default => json
      message_key <key (Optional, for text format only, default is message)>
      add_prefix <tag prefix (Optional)>
      add_suffix <tag suffix (Optional)>

      # Optionally, you can manage topic offset by using zookeeper
      offset_zookeeper    <zookeer node list (<zookeeper1_host>:<zookeeper1_port>,<zookeeper2_host>:<zookeeper2_port>,..)>
      offset_zk_root_node <offset path in zookeeper> default => '/fluent-plugin-kafka'

      # ruby-kafka consumer options
      max_bytes     (integer) :default => nil (Use default of ruby-kafka)
      max_wait_time (integer) :default => nil (Use default of ruby-kafka)
      min_bytes     (integer) :default => nil (Use default of ruby-kafka)
    </source>

Supports a start of processing from the assigned offset for specific topics.

    <source>
      @type kafka

      brokers <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
      format <input text type (text|json|ltsv|msgpack)>
      <topic>
        topic     <listening topic>
        partition <listening partition: default=0>
        offset    <listening start offset: default=-1>
      </topic>
      <topic>
        topic     <listening topic>
        partition <listening partition: default=0>
        offset    <listening start offset: default=-1>
      </topic>
    </source>

See also [ruby-kafka README](https://github.com/zendesk/ruby-kafka#consuming-messages-from-kafka) for more detailed documentation about ruby-kafka.

### Input plugin (@type 'kafka_group', supports kafka group)

Consume events by kafka consumer group features..

    <source>
      @type kafka_group

      brokers <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
      consumer_group <consumer group name, must set>
      topics <listening topics(separate with comma',')>
      format <input text type (text|json|ltsv|msgpack)> :default => json
      message_key <key (Optional, for text format only, default is message)>
      add_prefix <tag prefix (Optional)>
      add_suffix <tag suffix (Optional)>

      # ruby-kafka consumer options
      max_bytes               (integer) :default => nil (Use default of ruby-kafka)
      max_wait_time           (integer) :default => nil (Use default of ruby-kafka)
      min_bytes               (integer) :default => nil (Use default of ruby-kafka)
      offset_commit_interval  (integer) :default => nil (Use default of ruby-kafka)
      offset_commit_threshold (integer) :default => nil (Use default of ruby-kafka)
      start_from_beginning    (bool)    :default => true
    </source>

See also [ruby-kafka README](https://github.com/zendesk/ruby-kafka#consuming-messages-from-kafka) for more detailed documentation about ruby-kafka options.

### Output plugin (non-buffered)

This plugin uses ruby-kafka producer for writing data. For performance and reliability concerns, use `kafka_bufferd` output instead.

    <match *.**>
      @type kafka

      # Brokers: you can choose either brokers or zookeeper.
      brokers        <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,.. # Set brokers directly
      zookeeper      <zookeeper_host>:<zookeeper_port> # Set brokers via Zookeeper
      zookeeper_path <broker path in zookeeper> :default => /brokers/ids # Set path in zookeeper for kafka

      default_topic         (string) :default => nil
      default_partition_key (string) :default => nil
      output_data_type      (json|ltsv|msgpack|attr:<record name>|<formatter name>) :default => json
      output_include_tag    (bool) :default => false
      output_include_time   (bool) :default => false
      exclude_topic_key     (bool) :default => false
      exclude_partition_key (bool) :default => false

      # ruby-kafka producer options
      max_send_retries  (integer)     :default => 1
      required_acks     (integer)     :default => -1
      ack_timeout       (integer)     :default => nil (Use default of ruby-kafka)
      compression_codec (gzip|snappy) :default => nil
    </match>

Supports following ruby-kafka::Producer options.

- max_send_retries - default: 1 - Number of times to retry sending of messages to a leader.
- required_acks - default: -1 - The number of acks required per request.
- ack_timeout - default: nil - How long the producer waits for acks. The unit is seconds.
- compression_codec - default: nil - The codec the producer uses to compress messages.

See also [Kafka::Client](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Client) for more detailed documentation about ruby-kafka.

This plugin supports compression codec "snappy" also.
Install snappy module before you use snappy compression.

    $ gem install snappy

snappy gem uses native extension, so you need to install several packages before.
On Ubuntu, need development packages and snappy library.

    $ sudo apt-get install build-essential autoconf automake libtool libsnappy-dev

#### Load balancing

Messages will be assigned a partition at random as default by ruby-kafka, but messages with the same partition key will always be assigned to the same partition by setting `default_partition_key` in config file.
If key name `partition_key` exists in a message, this plugin set its value of partition_key as key.

|default_partition_key|partition_key| behavior |
| --- | --- | --- |
|Not set|Not exists| All messages are assigned a partition at random |
|Set| Not exists| All messages are assigned to the specific partition |
|Not set| Exists | Messages which have partition_key record are assigned to the specific partition, others are assigned a partition at random |
|Set| Exists | Messages which have partition_key record are assigned to the specific partition with parition_key, others are assigned to the specific partition with default_parition_key |


### Buffered output plugin

This plugin uses ruby-kafka producer for writing data. This plugin works with recent kafka versions.

    <match *.**>
      @type kafka_buffered

      # Brokers: you can choose either brokers or zookeeper.
      brokers             <broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,.. # Set brokers directly
      zookeeper           <zookeeper_host>:<zookeeper_port> # Set brokers via Zookeeper
      zookeeper_path      <broker path in zookeeper> :default => /brokers/ids # Set path in zookeeper for kafka

      default_topic         (string) :default => nil
      default_partition_key (string) :default => nil
      output_data_type      (json|ltsv|msgpack|attr:<record name>|<formatter name>) :default => json
      output_include_tag    (bool) :default => false
      output_include_time   (bool) :default => false
      exclude_topic_key     (bool) :default => false
      exclude_partition_key (bool) :default => false
      get_kafka_client_log  (bool) :default => false

      # See fluentd document for buffer related parameters: http://docs.fluentd.org/articles/buffer-plugin-overview

      # ruby-kafka producer options
      max_send_retries    (integer)     :default => 1
      required_acks       (integer)     :default => -1
      ack_timeout         (integer)     :default => nil (Use default of ruby-kafka)
      compression_codec   (gzip|snappy) :default => nil (No compression)
    </match>

`<formatter name>` of `output_data_type` uses fluentd's formatter plugins. See [formatter article](http://docs.fluentd.org/articles/formatter-plugin-overview).

ruby-kafka sometimes returns `Kafka::DeliveryFailed` error without good information.
In this case, `get_kafka_client_log` is useful for identifying the error cause.    
ruby-kafka's log is routed to fluentd log so you can see ruby-kafka's log in fluentd logs.

Supports following ruby-kafka's producer options.

- max_send_retries - default: 1 - Number of times to retry sending of messages to a leader.
- required_acks - default: -1 - The number of acks required per request.
- ack_timeout - default: nil - How long the producer waits for acks. The unit is seconds.
- compression_codec - default: nil - The codec the producer uses to compress messages.

See also [Kafka::Client](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Client) for more detailed documentation about ruby-kafka.


This plugin supports compression codec "snappy" also.
Install snappy module before you use snappy compression.

    $ gem install snappy

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
