import pytest
from kafka import KafkaUtil
from kazoo.exceptions import NoNodeError
# from kazoo.protocol.states import ZnodeStat


def with_separator(path):
    """Takes a string representing a ZooKeeper path

    Returns that string with a leading and trailing slash if it did not
    already have either of those"""
    if path[0] == '/' and path[-1] == '/':
        return path
    elif path[0] == '/':
        return path + '/'
    elif path[-1] == '/':
        return '/' + path
    else:
        return '/' + path + '/'


def get_tree(zk, path):
    """Takes a ZooKeeper client and a base path
    Returns a full tree from ZooKeeper, represented as a dict
    Used for test case preparation
    """
    def _child_path(path, child):
        return "{}{}/".format(with_separator(path), child)

    def _get_tree(zk, path):
        data = zk.get(path)
        children = zk.get_children(path)
        out = {path: data}
        if children:
            child_data = {inner_path: data
                          for child in children
                          for inner_path, data
                          in _get_tree(zk, _child_path(path, child)).items()}
            out.update(child_data)
        return out
    # Throw away the ZNodeData because the tests do not need it
    return {k: (v[0], True)
            for k, v in _get_tree(zk, with_separator(path)).items()}


class MockZKClient:
    """Takes a string of hosts
    Returns a mock ZooKeeper client that can have data inserted into it to
    represent the ZooKeeper file tree"""
    def __init__(self, hosts):
        self.hosts = hosts

    def set_data(self, data):
        self.data = data

    def get(self, path):
        try:
            return self.data[with_separator(path)]
        except KeyError:
            raise NoNodeError((), {})

    def get_children(self, path):
        path = with_separator(path)
        return [key.split(path)[1].split('/')[0]
                for key in self.data.keys()
                if key != path and
                key.startswith(path) and
                len(key.rstrip('/').split(path)[1].split('/')) == 1]

    def exists(self, path):
        return bool(self.data.get(with_separator(path), None))

    def start(self):
        pass

    def stop(self):
        pass


HOSTS = "0.0.0.0:2181"

EMPTY_CLUSTER_DATA = {'/brokers/topics/':
                      (None, True)}


@pytest.fixture
def empty_cluster():
    zk = MockZKClient(HOSTS)
    zk.data = EMPTY_CLUSTER_DATA
    return KafkaUtil(zk, {"hosts": HOSTS})


def test_list_empty_cluster(empty_cluster):
    with empty_cluster as k:
        out = k.list()
        expected = {"count": 0,
                    "topics": [],
                    "msg": "No topics to retrieve"}
        assert(out == expected)


def test_describe_empty_cluster(empty_cluster):
    with empty_cluster as k:
        out = k.describe()
        expected = {"data": {},
                    "msg": "No topics to describe"}
        assert(out == expected)


def test_in_sync_empty_cluster(empty_cluster):
    with empty_cluster as k:
        out = k.in_sync()
        expected = {"in_sync": True,
                    "msg": "No topics to check"}
        assert(out == expected)


def test_in_sync_verbose_empty_cluster(empty_cluster):
    with empty_cluster as k:
        out = k.in_sync_verbose()
        expected = {"in_sync": True,
                    "sync_data": {},
                    "msg": "No topics to check"}
        assert(out == expected)


@pytest.fixture
def empty_cluster_with_topic_params():
    zk = MockZKClient(HOSTS)
    zk.data = EMPTY_CLUSTER_DATA
    return KafkaUtil(zk, {"hosts": HOSTS, "topics": ["not_here"]})

# There is no test for list here because list is not affected by the "topics"
# Ansible playbook parameter


def test_describe_topics_empty_cluster(empty_cluster_with_topic_params):
    with empty_cluster_with_topic_params as k:
        out = k.describe()
        expected = {"data": {'not_here': {'partitions': {}, 'state': None}},
                    "msg":  "No data found for requested topics"}
        assert(out == expected)


def test_in_sync_topics_empty_cluster(empty_cluster_with_topic_params):
    with empty_cluster_with_topic_params as k:
        out = k.in_sync()
        expected = {"in_sync": True,
                    "msg": "Topics: ['not_here'] are in sync (or null)"}
        assert(out == expected)


def test_in_sync_topics_verbose_empty_cluster(empty_cluster_with_topic_params):
    with empty_cluster_with_topic_params as k:
        out = k.in_sync_verbose()
        expected = {"in_sync": True,
                    "sync_data": {'not_here': {'in_sync': True,
                                               'out_of_sync_replicas': {}}},
                    "msg": "Topics: ['not_here'] are in sync (or null)"}
        assert(out == expected)


CLEAN_CLUSTER_DATA = {'/brokers/topics/': (None, True),                                                                                                                 # noqa
                      '/brokers/topics/another_one/': ('{"version":1,"partitions":{"1":[1001,1002],"0":[1003,1001]}}', True),                                           # noqa
                      '/brokers/topics/another_one/partitions/': (None, True),                                                                                          # noqa
                      '/brokers/topics/another_one/partitions/0/': (None, True),                                                                                        # noqa
                      '/brokers/topics/another_one/partitions/0/state/': ('{"controller_epoch":1,"leader":1003,"version":1,"leader_epoch":0,"isr":[1003,1001]}', True), # noqa
                      '/brokers/topics/another_one/partitions/1/': (None, True),                                                                                        # noqa
                      '/brokers/topics/another_one/partitions/1/state/': ('{"controller_epoch":1,"leader":1001,"version":1,"leader_epoch":0,"isr":[1001,1002]}', True), # noqa
                      '/brokers/topics/topic/': ('{"version":1,"partitions":{"1":[1003,1002],"0":[1002,1001]}}', True),                                                 # noqa
                      '/brokers/topics/topic/partitions/': (None, True),                                                                                                # noqa
                      '/brokers/topics/topic/partitions/0/': (None, True),                                                                                              # noqa
                      '/brokers/topics/topic/partitions/0/state/': ('{"controller_epoch":1,"leader":1002,"version":1,"leader_epoch":0,"isr":[1002,1001]}', True),       # noqa
                      '/brokers/topics/topic/partitions/1/': (None, True),                                                                                              # noqa
                      '/brokers/topics/topic/partitions/1/state/': ('{"controller_epoch":1,"leader":1003,"version":1,"leader_epoch":0,"isr":[1003,1002]}', True)}       # noqa


@pytest.fixture
def clean_cluster():
    zk = MockZKClient(HOSTS)
    zk.data = CLEAN_CLUSTER_DATA
    return KafkaUtil(zk, {"hosts": HOSTS})


def test_list_clean_cluster(clean_cluster):
    with clean_cluster as k:
        out = k.list()
        expected = {"count": 2,
                    "topics": ['topic', 'another_one'],
                    "msg": "Retrieved topics"}
        assert(out == expected)


def test_describe_clean_cluster(clean_cluster):
    with clean_cluster as k:
        out = k.describe()
        expected = {'data': {'another_one': {'partitions':
                                             {u'0': {u'controller_epoch': 1,
                                                     u'isr': [1003, 1001],
                                                     u'leader': 1003,
                                                     u'leader_epoch': 0,
                                                     u'version': 1},
                                              u'1': {u'controller_epoch': 1,
                                                     u'isr': [1001, 1002],
                                                     u'leader': 1001,
                                                     u'leader_epoch': 0,
                                                     u'version': 1}},
                                             'state': {u'partitions':
                                                       {u'0': [1003, 1001],
                                                        u'1': [1001, 1002]},
                                                       u'version': 1}},
                             'topic': {'partitions':
                                       {u'0': {u'controller_epoch': 1,
                                               u'isr': [1002, 1001],
                                               u'leader': 1002,
                                               u'leader_epoch': 0,
                                               u'version': 1},
                                        u'1': {u'controller_epoch': 1,
                                               u'isr': [1003, 1002],
                                               u'leader': 1003,
                                               u'leader_epoch': 0,
                                               u'version': 1}},
                                       'state': {u'partitions':
                                                 {u'0': [1002, 1001],
                                                  u'1': [1003, 1002]},
                                                 u'version': 1}}},
                    'msg': 'Retrieved topic description for all topics'}

        assert(out == expected)


def test_in_sync_clean_cluster(clean_cluster):
    with clean_cluster as k:
        out = k.in_sync()
        expected = {"in_sync": True,
                    "msg": "All topics are in sync"}
        assert(out == expected)


def test_in_sync_verbose_clean_cluster(clean_cluster):
    with clean_cluster as k:
        out = k.in_sync_verbose()
        expected = {"in_sync": True,
                    'sync_data':
                    {'another_one':
                     {'in_sync': True, 'out_of_sync_replicas': {}},
                     'topic': {'in_sync': True, 'out_of_sync_replicas': {}}},
                    "msg": "All topics are in sync"}
        assert(out == expected)


@pytest.fixture
def clean_cluster_with_topic_params():
    zk = MockZKClient(HOSTS)
    zk.data = CLEAN_CLUSTER_DATA
    return KafkaUtil(zk, {"hosts": HOSTS, "topics": ["topic"]})

# There is no test for list here because list is not affected by the "topics"
# Ansible playbook parameter


def test_describe_topics_clean_cluster(clean_cluster_with_topic_params):
    with clean_cluster_with_topic_params as k:
        out = k.describe()
        expected = {'data': {'topic': {'partitions':
                                       {u'0': {u'controller_epoch': 1,
                                               u'isr': [1002, 1001],
                                               u'leader': 1002,
                                               u'leader_epoch': 0,
                                               u'version': 1},
                                        u'1': {u'controller_epoch': 1,
                                               u'isr': [1003, 1002],
                                               u'leader': 1003,
                                               u'leader_epoch': 0,
                                               u'version': 1}},
                                       'state': {u'partitions':
                                                 {u'0': [1002, 1001],
                                                  u'1': [1003, 1002]},
                                                 u'version': 1}}},
                    'msg': 'Retrieved topic description for requested topics'}

        assert(out == expected)


def test_in_sync_topics_clean_cluster(clean_cluster_with_topic_params):
    with clean_cluster_with_topic_params as k:
        out = k.in_sync()
        expected = {"in_sync": True,
                    "msg": "Topics: ['topic'] are in sync (or null)"}
        assert(out == expected)


def test_in_sync_topics_verbose_clean_cluster(clean_cluster_with_topic_params):
    with clean_cluster_with_topic_params as k:
        out = k.in_sync_verbose()
        expected = {"in_sync": True,
                    "sync_data": {'topic': {'in_sync': True,
                                            'out_of_sync_replicas': {}}},
                    "msg": "Topics: ['topic'] are in sync (or null)"}
        assert(out == expected)


DIRTY_CLUSTER_DATA = {'/brokers/topics/': (None, True),                                                                                                                 # noqa
                      '/brokers/topics/another_one/': ('{"version":1,"partitions":{"1":[1001,1002],"0":[1003,1001]}}', True),                                           # noqa
                      '/brokers/topics/another_one/partitions/': (None, True),                                                                                          # noqa
                      '/brokers/topics/another_one/partitions/0/': (None, True),                                                                                        # noqa
                      '/brokers/topics/another_one/partitions/0/state/': ('{"controller_epoch":1,"leader":1003,"version":1,"leader_epoch":0,"isr":[1003,1001]}', True), # noqa
                      '/brokers/topics/another_one/partitions/1/': (None, True),                                                                                        # noqa
                      '/brokers/topics/another_one/partitions/1/state/': ('{"controller_epoch":2,"leader":1002,"version":1,"leader_epoch":1,"isr":[1002]}', True),      # noqa
                      '/brokers/topics/topic/': ('{"version":1,"partitions":{"1":[1003,1002],"0":[1002,1001]}}', True),                                                 # noqa
                      '/brokers/topics/topic/partitions/': (None, True),                                                                                                # noqa
                      '/brokers/topics/topic/partitions/0/': (None, True),                                                                                              # noqa
                      '/brokers/topics/topic/partitions/0/state/': ('{"controller_epoch":2,"leader":1002,"version":1,"leader_epoch":1,"isr":[1002]}', True),            # noqa
                      '/brokers/topics/topic/partitions/1/': (None, True),                                                                                              # noqa
                      '/brokers/topics/topic/partitions/1/state/': ('{"controller_epoch":1,"leader":1003,"version":1,"leader_epoch":0,"isr":[1003,1002]}', True)}       # noqa


@pytest.fixture
def dirty_cluster():
    zk = MockZKClient(HOSTS)
    zk.data = DIRTY_CLUSTER_DATA
    return KafkaUtil(zk, {"hosts": HOSTS})


def test_list_dirty_cluster(dirty_cluster):
    with dirty_cluster as k:
        out = k.list()
        expected = {"count": 2,
                    "topics": ['topic', 'another_one'],
                    "msg": "Retrieved topics"}
        assert(out == expected)


def test_describe_dirty_cluster(dirty_cluster):
    with dirty_cluster as k:
        out = k.describe()
        expected = {'data': {'another_one': {'partitions':
                                             {u'0': {u'controller_epoch': 1,
                                                     u'isr': [1003, 1001],
                                                     u'leader': 1003,
                                                     u'leader_epoch': 0,
                                                     u'version': 1},
                                              u'1': {u'controller_epoch': 2,
                                                     u'isr': [1002],
                                                     u'leader': 1002,
                                                     u'leader_epoch': 1,
                                                     u'version': 1}},
                                             'state': {u'partitions':
                                                       {u'0': [1003, 1001],
                                                        u'1': [1001, 1002]},
                                                       u'version': 1}},
                             'topic': {'partitions':
                                       {u'0': {u'controller_epoch': 2,
                                               u'isr': [1002],
                                               u'leader': 1002,
                                               u'leader_epoch': 1,
                                               u'version': 1},
                                        u'1': {u'controller_epoch': 1,
                                               u'isr': [1003, 1002],
                                               u'leader': 1003,
                                               u'leader_epoch': 0,
                                               u'version': 1}},
                                       'state': {u'partitions':
                                                 {u'0': [1002, 1001],
                                                  u'1': [1003, 1002]},
                                                 u'version': 1}}},
                    'msg': 'Retrieved topic description for all topics'}

        assert(out == expected)


def test_in_sync_dirty_cluster(dirty_cluster):
    with dirty_cluster as k:
        out = k.in_sync()
        expected = {"in_sync": False,
                    "msg": "Some topics are out of sync"}
        assert(out == expected)


def test_in_sync_verbose_dirty_cluster(dirty_cluster):
    with dirty_cluster as k:
        out = k.in_sync_verbose()
        expected = {
            "in_sync": False,
                     'sync_data':
                     {'another_one': {'in_sync': False,
                                      'out_of_sync_replicas': {u'1': [1001]}},
                      'topic': {'in_sync': False,
                                'out_of_sync_replicas': {u'0': [1001]}}},
                    "msg": "Some topics are out of sync"}
        assert(out == expected)


@pytest.fixture
def dirty_cluster_with_topic_params():
    zk = MockZKClient(HOSTS)
    zk.data = DIRTY_CLUSTER_DATA
    return KafkaUtil(zk, {"hosts": HOSTS, "topics": ["topic"]})

# There is no test for list here because list is not affected by the "topics"
# Ansible playbook parameter


def test_describe_topics_dirty_cluster(dirty_cluster_with_topic_params):
    with dirty_cluster_with_topic_params as k:
        out = k.describe()
        expected = {'data': {'topic': {'partitions':
                                       {u'0': {u'controller_epoch': 2,
                                               u'isr': [1002],
                                               u'leader': 1002,
                                               u'leader_epoch': 1,
                                               u'version': 1},
                                        u'1': {u'controller_epoch': 1,
                                               u'isr': [1003, 1002],
                                               u'leader': 1003,
                                               u'leader_epoch': 0,
                                               u'version': 1}},
                                       'state': {u'partitions':
                                                 {u'0': [1002, 1001],
                                                  u'1': [1003, 1002]},
                                                 u'version': 1}}},
                    'msg': 'Retrieved topic description for requested topics'}

        assert(out == expected)


def test_in_sync_topics_dirty_cluster(dirty_cluster_with_topic_params):
    with dirty_cluster_with_topic_params as k:
        out = k.in_sync()
        expected = {"in_sync": False,
                    "msg": "Some topics are out of sync"}
        assert(out == expected)


def test_in_sync_topics_verbose_dirty_cluster(dirty_cluster_with_topic_params):
    with dirty_cluster_with_topic_params as k:
        out = k.in_sync_verbose()
        expected = {"in_sync": False,
                    'sync_data': {'topic':
                                  {'in_sync': False,
                                   'out_of_sync_replicas': {'0': [1001]}}},
                    "msg": "Some topics are out of sync"}
        assert(out == expected)
