"""
A successful basic paxos run:

  proposer  |  acceptors  |  learners
  -----------------------------------
1) promise ------->X             |
2)   X<------- promised          |
3) accept -------->X             |
4)   |           learn --------->X
5)   |             X <------ learned
6)   X<------- accepted          |

promise/promised and accept/accepted are each combined as RPC request/response
pairs with the proposer acting as client and the acceptors as servers.

Since the promise and accept messages need only be successfully acknowledged by
a quorum of acceptors, the proposer waits for each individual response arrival,
rather than for the RPC completion as a whole. When it has enough, it moves the
process forward.

The learners must store in memory the data in every 'learn' message, but they
move it from a status of in-process to committed once they have received
affirmative messages from a quorum for a single proposition.
"""

import time

import junction
import junction.errors


class Proposer(object):
    def __init__(self, group_id, cluster_size, hub):
        self._started = False
        self._hub = hub
        self._groupid = group_id
        self._clustersize = cluster_size
        self._acceptor_count = None
        self._numbers = {}

    def start(self):
        if self._started: return
        self._started = True

        # offer up this functionality on the junction network
        self._hub.accept_rpc(
                'divconq.paxos.propose',
                (1 << 64) - 1,
                self._groupid,
                'propose',
                self.propose,
                schedule=True)

    def propose(self, key, value, overwrite=False, timeout=30.0):
        if timeout is not None:
            stoptime = time.time() + timeout

        if key in self._numbers:
            self._numbers[key] += 1
        else:
            self._numbers[key] = 1
        num = self._numbers[key]

        promise = self._hub.send_rpc(
                'divconq.paxos.accept',
                self._groupid,
                'promise',
                (key, num),
                {})

        success, results = await_quorum(
                promise, lambda x: x[0]['success'], self._clustersize,
                timeout and (stoptime - time.time()))
        results = [r[0] for r in results]

        if not success:
            results = [r['promised'] for r in results if not r['success']]
            self._numbers[key] = max(results)
            return False

        if not overwrite:
            results = [r['value'] for r in results
                    if r['success'] and r['value']]
            if results:
                results.sort(reverse=True)
                value = results[0][1]

        proposal = self._hub.send_rpc(
                'divconq.paxos.accept',
                self._groupid,
                'accept',
                (key, num, value),
                {})

        return await_quorum(proposal, None, self._clustersize,
                timeout and stoptime - time.time())[0]


class Acceptor(object):
    def __init__(self, group_id, hub):
        self._promised = {}
        self._values = {}
        self._started = False
        self._hub = hub
        self._groupid = group_id

    def start(self):
        if self._started: return
        self._started = True

        self._hub.accept_rpc(
                'divconq.paxos.accept',
                (1 << 64) - 1,
                self._groupid,
                'promise',
                self._handle_promise,
                schedule=False)

        self._hub.accept_rpc(
                'divconq.paxos.accept',
                (1 << 64) - 1,
                self._groupid,
                'accept',
                self._handle_accept,
                schedule=True)

    def _handle_promise(self, key, num):
        if self._promised.get(key, 0) >= num:
            return {
                'success': False,
                'promised': self._promised[key]
            }, self._hub._ident

        self._promised[key] = num
        return {
            'success': True,
            'value': self._values.get(key),
        }, self._hub._ident

    def _handle_accept(self, key, num, value):
        if self._promised.get(key, 0) > num:
            self._hub.publish(
                    'divconq.paxos.learn',
                    self._groupid,
                    'unlearn',
                    (key, num),
                    {})
            return False

        self._values[key] = (num, value)
        self._hub.rpc(
                'divconq.paxos.learn',
                self._groupid,
                'learn',
                (key, num, value),
                {})
        return True


class Learner(object):
    def __init__(self, group_id, cluster_size, hub):
        self._started = False
        self._hub = hub
        self._groupid = group_id
        self._clustersize = cluster_size
        self._learning = {}
        self._learned = {}

    def start(self):
        if self._started: return
        self._started = True

        #TODO: get up-to-speed from any peers

        self._hub.accept_rpc(
                'divconq.paxos.learn',
                (1 << 64) - 1,
                self._groupid,
                'learn',
                self._handle_learn,
                schedule=False)

        self._hub.accept_publish(
                'divconq.paxos.learn',
                (1 << 64) - 1,
                self._groupid,
                'unlearn',
                self._handle_unlearn,
                schedule=False)

    def _handle_learn(self, key, num, value):
        prev_num, good, bad = self._learning.get(key, (0, 0, 0))
        if prev_num > num:
            # this proposal is already obsolete
            return
        if prev_num < num:
            # reset for the new higher proposal num
            good = bad = 0

        good += 1
        self._learning[key] = (num, good, bad)

        if good > self._clustersize // 2:
            # a quorum of successes
            self._learned[key] = value
            del self._learning[key]

        return True

    def _handle_unlearn(self, key, num):
        prev_num, good, bad = self._learning.get(key, (0, 0, 0))
        if prev_num > num:
            # this proposal is already obsolete
            return
        if prev_num < num:
            # reset for the new higher proposal num
            good = bad = 0

        bad += 1
        self._learning[key] = (num, good, bad)

        if bad > self._clustersize // 2:
            # quorum of fail. decent band name.
            del self._learning[key]


class Server(object):
    def __init__(self, group_id, cluster_size, hub,
            proposer=True, acceptor=True, learner=True):
        if (not isinstance(group_id, (int, long))
                or group_id >= 1 << 64
                or group_id < 0):
            raise ValueError("group_id must be an unsigned 64-bit integer")

        self._started = False
        self._groupid = group_id
        self._hub = hub

        if proposer:
            self._proposer = Proposer(group_id, cluster_size, hub)
        else:
            self._proposer = None

        if acceptor:
            self._acceptor = Acceptor(group_id, hub)
        else:
            self._proposer = None

        if learner:
            self._learner = Learner(group_id, cluster_size, hub)
        else:
            self._learner = None

    def start(self):
        if self._started: return
        self._started = True

        if self._proposer:
            self._proposer.start()
        if self._acceptor:
            self._acceptor.start()
        if self._learner:
            self._learner.start()


class QuorumUnavailable(junction.errors.HandledError):
    'fewer than (<cluster-size> // 2) + 1 acceptors are online'
    code = -0x6e9ec5a6d2a90fff # hex(hash("divconq.paxos.QuorumUnavailable"))

class OperationTimedOut(junction.errors.HandledError):
    code = 0x46758d2a059baa16 # hex(hash("divconq.paxos.OperationTimedOut"))


def await_quorum(rpc, passfail, cluster_size, timeout=None):
    quorum = (cluster_size // 2) + 1
    if timeout is not None:
        timeout += time.time()
    kwarg = {}

    if rpc.target_count < quorum:
        raise QuorumUnavailable()

    while 1:
        results = rpc.partial_results
        goodbad = map(passfail, results) if passfail else results
        if goodbad.count(True) > quorum:
            return True, results
        if rpc.complete or goodbad.count(False) > quorum:
            return False, results

        if timeout is not None:
            kwarg['timeout'] = timeout - time.time()
        if rpc.arrival.wait(**kwarg):
            raise OperationTimedOut()
