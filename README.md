## Kafka Topics

This is a module to query a Kafka cluster in the same way the kafka-topics
cli tool does, but with 100% less shelling out. 

### Why?

Being able to check if topics are in sync before continuing with a rolling
restart is nice. Being able to respond to topics being present and correctly
partitioned during an automated deploy is nice. Being able to script automatic
healing based on cluster state is nice. Doing all of that somewhere you can't
just install Kafka and use the cli is also nice.

### How do I use it in my playbooks?

The documentation lives in the source so that Ansible can use it, 
but one example:

    kafka_topics:
      hosts: 'zookeeper.dev:2181'
      command: list

will list all topics in the Kafka cluster using that ZooKeeper instance.

### How do I test it?

Install the requirements listed in `requirements.txt` and run `py.test`.

### How do I add stuff?

Ideally, your thing fits as another method on KafkaUtil and can have tests
written in all 3 sections of test_kafka_topics.py.

Submitting an issue is swell too.

### What am I allowed to do with it?

It's licensed under the same terms as Ansible
