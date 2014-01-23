# Fluent::Plugin::Kafka-poseidon

TODO: Write a gem description
TODO: Also, I need to write tests

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-kafka-poseidon'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-kafka-poseidon

## Usage

### in_kafka_poseidon

fluentd.conf
    <source>
      type   kafka-poseidon
      host   <broker host>
      port   <broker port: default=9092>
      topics <listening topics(separate with comma',')>
    </source>

### out_kafka_poseidon (non-buffered)

fluentd.conf
    <match *.**>
      type          kafka-poseidon
      brokers       <broker1_host>:<broker1_ip>,<broker2_host>:<broker2_ip>,..
      default_topic <output topic>
    </match>

### out_kafka_poseidon_buffered

fluentd.conf
    <match *.**>
      type            kafka-poseidon-buffered
      brokers         <broker1_host>:<broker1_ip>,<broker2_host>:<broker2_ip>,..
      default_topic   <output topic>
      flush_interval  <flush interval (sec) :default => 60>
      buffer_type     (file|memory)
    </match>

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
