#! /usr/bin/env python
from ansible.module_utils.basic import AnsibleModule
import json

DOCUMENTATION = """
---
module: kafka
short_description: list, describe, and check the sync status of Kafka topics
options:
    hosts:
        description:
            - List of ZooKeeper servers used by Kafka (format '[server]:[port]')
        required: true
    command:
        description:
            - Command to run. Options:
                list: retrieve a list of topics
                describe: retrieve detailed data about topics
                in_sync: check if all topic replicas in sync
                in_sync_verbose: retrieve detailed data about sync state
        required: true
    topics:
        description:
            - The topics to operate on.
        default: [] (this operates on all topics on the cluster)
        required: false
requirements:
    - kazoo >= 2.1
    - python >= 2.6
author: "Jason Saxon(@to-json)"
"""

EXAMPLES = """
# List all topics in a kafka cluster
- kafka:
    hosts: 'localhost:2181'
    command: list

# get the topic description data for all topics in a kafka cluster
- kafka:
    hosts: 'localhost:2181'
    command: describe

# get the topic description data for 2 specific topics in a kafka cluster
- kafka:
    hosts: 'localhost:2181'
    command: describe
    topics: ['topic', 'another_one']

# check if all topics have the expected number of in sync replicas
- kafka:
    hosts: 'localhost:2181'
    command: in_sync

# check if 2 specific topics have the expected number of in sync replicas
- kafka:
    hosts: 'localhost:2181'
    command: in_sync
    topics: ['topic', 'another_one']

# check if all topics have the expected number of in sync replicas and
# retrieve data about topics with replicas that are out of sync
- kafka:
    hosts: 'localhost:2181'
    command: in_sync_verbose

# check if 2 specific topics have the expected number of in sync replicas and
# retrieve data about topics with replicas that are out of sync
- kafka:
    hosts: 'localhost:2181'
    command: in_sync_verbose
    topics: ['topic', 'another_one']
"""

try:
    from kazoo.client import KazooClient
    KAZOO_INSTALLED = True
except ImportError:
    KAZOO_INSTALLED = False


def main():
    module = AnsibleModule(
        argument_spec=dict(
            command=dict(required=True, choices=['list',
                                                 'describe',
                                                 'in_sync',
                                                 'in_sync_verbose']),
            state=dict(default="present", choices=['present', 'absent']),
            hosts=dict(required=True, type='str'),
            topics=dict(required=False, default=None, type='list')
        ),
        supports_check_mode=False
    )

    if not KAZOO_INSTALLED:
        module.fail_json(msg='kazoo >= 2.1 is required to use this module')

    def execute(zk_client, params):
        """Run the function requested in the module parameters
        and return the output"""
        with KafkaUtil(zk_client, params) as k:
            topic_command_dict = {
                "list": k.list,
                "describe": k.describe,
                "in_sync": k.in_sync,
                "in_sync_verbose": k.in_sync_verbose
            }
            return topic_command_dict[params['command']]()

    client = KazooClient(module.params['hosts'])

    try:
        module.exit_json(**execute(client, module.params))
    except Exception as e:
        module.fail_json(msg="Caught unhandled exception {}".format(e))


class KafkaUtil:
    """KafkaUtil provides functions to query ZooKeeper for information
    about Kafka topics.

    Args:
        zk_client: ZooKeeper client
        module: Ansible module object

    Attributes:
        zk: ZooKeeper client
        topics: (optional) list of topics to operate on
        params: the params dict this util was invoked with

    Interface:
        list(): a list of topics.
        describe(): detailed data about topics.
        in_sync(): are all topic replicas in sync?
        in_sync_verbose(): detailed data about sync state.
    """
    TOPICS = '/brokers/topics/'

    def __init__(self, zk_client, params):
        self.zk = zk_client
        self.topics = params.get('topics', [])
        self.params = params

    def list(self):
        """List the kafka topics.

        Returns a list of all Kafka topics in this Zookeeper Cluster, in a
        dict formatted for Ansible.
        """
        topics = self._list_topics()
        count = len(topics)
        msg = None
        if count == 0:
            msg = 'No topics to retrieve'
        else:
            msg = 'Retrieved topics'

        return {'msg': msg,
                'topics': topics,
                'count': count}

    def describe(self):
        """Describe the current state of Kafka topics

        Only describes the provided topics if topics is defined in the
        module parameters.

        Returns a machine-readable version of the data supplied by
        'kafka-topics --describe', in a dict formatted for Ansible
        """
        def _describe_topics(topics):
            return {topic: self._describe_topic(topic)
                    for topic in topics}

        data = self._with_topics(_describe_topics, self.topics)

        msg = None
        if data:
            if self.topics:
                if any([v['state'] for k, v in data.items()]):
                    msg = "Retrieved topic description for requested topics"
                else:
                    msg = "No data found for requested topics"
            else:
                msg = "Retrieved topic description for all topics"
        else:
            msg = "No topics to describe"

        return {'msg': msg, 'data': data}

    def in_sync(self, topics=None):
        """Check the sync status of some or all of the topics in the cluster

        Only checks the provided topics if topics is defined in the module
        parameters, otherwise checks all topics in the cluster. Unlike describe,
        this method does not confirm that the topic exists

        Returns the boolean sync status and a friendly message version of that
        status, in a dict formatted for Ansible
        """
        def _topics_in_sync_verbose(topics):
            return {topic: self._topic_in_sync(topic)
                    for topic in topics}
        sync_data = self._with_topics(_topics_in_sync_verbose, self.topics)
        synced = all([data['in_sync'] for topic, data in sync_data.items()])

        msg = None
        if sync_data:
            if synced:
                if self.topics:
                    msg = "Topics: {} are in sync (or null)".format(self.topics)
                else:
                    msg = "All topics are in sync"
            else:
                msg = "Some topics are out of sync"
        else:
            msg = "No topics to check"

        return {'msg': msg, 'in_sync': synced}

    def in_sync_verbose(self, topics=None):
        """Check the sync status of some or all of the topics in the cluster

        Args:
            topics (list, optional): a list of topics to be described
        Only checks the provided topics if topics is defined in the
        module parameters. Unlike describe, this method does not confirm
        that the topic exists

        Returns the boolean sync status and a friendly message version of that,
        along with a dict of the deviant topics and their out-of-sync replicas,
        in a dict formatted for Ansible
        """
        def _topics_in_sync_verbose(topics):
            return {topic: self._topic_in_sync(topic)
                    for topic in topics}
        sync_data = self._with_topics(_topics_in_sync_verbose, self.topics)
        synced = all([data['in_sync'] for topic, data in sync_data.items()])

        msg = None
        if sync_data:
            if synced:
                if self.topics:
                    msg = "Topics: {} are in sync (or null)".format(self.topics)
                else:
                    msg = "All topics are in sync"
            else:
                msg = "Some topics are out of sync"
        else:
            msg = "No topics to check"

        return {"msg": msg, "in_sync": synced, "sync_data": sync_data}

    def _with_topics(self, f, topics):
        if topics:
            return f(topics)
        else:
            return f(self._list_topics())

    def __enter__(self):
        self.zk.start()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.zk.stop()

    def _topic_in_sync(self, topic):
        diffs = self.topic_divergence(topic)
        if diffs:
            return {"in_sync": False,
                    "out_of_sync_replicas": diffs}
        else:
            return {"in_sync": True,
                    "out_of_sync_replicas": {}}

    def topic_divergence(self, topic):
        parts = self._topic_partitions_state(topic)
        state = self._topic_state(topic)
        expectations = {}
        if state:
            expectations = state['partitions']
        diffs = {}
        for partition, expectation in expectations.items():
            isr = parts[partition]['isr']
            diff = list(set(expectation).difference(isr))
            if diff:
                diffs[partition] = diff
        if diffs:
            return diffs
        else:
            return None

    def _get_data(self, path):
        if self.zk.exists(path):
            return json.loads(self.zk.get(path)[0])
        else:
            return None

    def _list_topics(self):
        return self.zk.get_children(self.TOPICS)

    def _topic_path(self, topic):
        return self.TOPICS + topic + '/'

    def _partition_state_path(self, topic, partition):
        return self._topic_path(topic) + "partitions/" \
            + str(partition) + "/state"

    def _topic_state(self, topic):
        return self._get_data(self._topic_path(topic))

    def _partition_state(self, topic, partition):
        return self._get_data(self._partition_state_path(topic, partition))

    def _topic_partitions_state(self, topic):

        state = self._topic_state(topic) if self._topic_state(topic) else {}
        partition_data = state.get("partitions", {})
        partitions = []
        if partition_data:
            partitions = partition_data.keys()
        return {partition: self._partition_state(topic, partition)
                for partition in partitions}

    def _describe_topic(self, topic):
        return {"state": self._topic_state(topic),
                "partitions": self._topic_partitions_state(topic)}

if __name__ == '__main__':
    main()
