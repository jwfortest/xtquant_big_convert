"""Native callback -> owned queue -> Redis stream -> xtdata callback tests.

No market/account connection. Optional real Redis test uses an explicit test URL.
"""
import itertools
import json
import os
import threading
import time
from collections import defaultdict

import pytest

from bigqmt_signal_trader.l2_push import L2SubscriptionManager, copy_l2_batch
from bigqmt_signal_trader.l2_session import L2ClientSession
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class MemoryRedis:
    def __init__(self):
        self.streams = defaultdict(list)
        self.condition = threading.Condition()
        self.counter = 0
        self.fail_write = False
        self.fail_read = False
        self.write_entered = threading.Event()
        self.write_release = threading.Event()
        self.write_release.set()

    def pipeline(self, transaction=False):
        owner = self
        class Pipeline:
            def xadd(self, key, fields, maxlen, approximate):
                self.args = key, fields, maxlen
                assert approximate is False
                return self
            def expire(self, key, ttl):
                assert ttl > 0
                return self
            def execute(self):
                owner.write_entered.set()
                assert owner.write_release.wait(3)
                if owner.fail_write:
                    raise ConnectionError('synthetic Redis outage')
                key, fields, maxlen = self.args
                with owner.condition:
                    owner.counter += 1
                    owner.streams[key].append(('%d-0' % owner.counter, fields))
                    owner.streams[key] = owner.streams[key][-maxlen:]
                    owner.condition.notify_all()
                return [str(owner.counter), 1]
        return Pipeline()

    def xread(self, streams, count, block):
        if self.fail_read:
            raise ConnectionError('synthetic disconnect')
        def fetch():
            result = []
            for key, cursor in streams.items():
                after = tuple(map(int, cursor.split('-')))
                rows = [(sid, fields) for sid, fields in self.streams[key]
                        if tuple(map(int, sid.split('-'))) > after][:count]
                if rows:
                    result.append((key, rows))
            return result
        with self.condition:
            result = fetch()
            if not result:
                self.condition.wait(block/1000)
                result = fetch()
            return result


class Context:
    def __init__(self):
        self.callbacks = {}
        self.closed = []
        self.serial = itertools.count(1)
        self.inline = None
        self.reject = False
        self.fail_close = False

    def subscribe_quote(self, code, period, dividend_type, result_type, callback):
        assert period.startswith('l2') and result_type == 'list'
        assert dividend_type == 'none'
        if self.reject:
            return -1
        sid = next(self.serial)
        self.callbacks[sid] = (code, period, callback)
        if self.inline is not None:
            callback({code: self.inline})
        return sid

    def unsubscribe_quote(self, sid):
        if self.fail_close:
            raise RuntimeError('synthetic unsubscribe failure')
        self.closed.append(sid)
        self.callbacks.pop(sid, None)

    def push(self, rows, sid=None):
        sid = sid or next(iter(self.callbacks))
        code, period, callback = self.callbacks[sid]
        callback({code: rows})


def wait_for(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'condition did not become true'
        time.sleep(0.005)


@pytest.fixture
def rig():
    source, wire = Context(), MemoryRedis()
    manager = L2SubscriptionManager(source, wire, 'TEST', copy_budget_seconds=1)
    handlers = BigQmtRpcHandlers('TEST', None, None, l2_subscription_manager=manager)
    session = L2ClientSession(handlers.handle, wire, itertools.count(1).__next__)
    yield source, wire, manager, handlers, session
    wire.write_release.set()
    session.stop()
    manager.stop()


def test_preserves_all_records_duplicates_and_integer_times(rig):
    source, wire, manager, handlers, session = rig
    seen = []
    sid = session.subscribe('000001.SZ', 'l2transaction', seen.append)
    rows = [{'time': 20260911145959123, 'tradeIndex': 5, 'volume': 100},
            {'time': 20260911145959123, 'tradeIndex': 5, 'volume': 100},
            {'time': 20260911145959124, 'tradeIndex': 6, 'volume': 200}]
    source.push(rows)
    source.push(rows)
    wait_for(lambda: len(seen) == 2)
    assert seen == [{'000001.SZ': rows}, {'000001.SZ': rows}]
    assert session.status()[0]['delivered_records'] == 6
    assert not manager.status()['streams'][0]['fault']
    session.unsubscribe(sid)
    assert source.closed == [1]


def test_column_batches_and_ten_level_arrays_are_not_collapsed():
    data, _ = copy_l2_batch({'x': {'time': [1, 2], 'price': [4, 5],
                                  'askPrice': [[10]*10, [11]*10]}}, 'x')
    assert len(data['x']) == 2 and data['x'][1]['askPrice'] == [11]*10
    single, _ = copy_l2_batch({'x': {'time': 1, 'bidPrice': list(range(10))}}, 'x')
    assert len(single['x']) == 1 and single['x'][0]['bidPrice'] == list(range(10))


@pytest.mark.parametrize('payload', [
    {'x': {'time': [1, 2], 'price': [5]}}, {'wrong': []},
    {'x': [{'time': 1, 'price': float('nan')}]}, {'x': 'invalid'},
])
def test_invalid_batches_fail_explicitly(payload):
    with pytest.raises((ValueError, TypeError)):
        copy_l2_batch(payload, 'x')


def test_copy_limits_do_not_silently_truncate():
    with pytest.raises(ValueError):
        copy_l2_batch({'x': [{'time': i} for i in range(5)]}, 'x', max_records=2)
    with pytest.raises(ValueError):
        copy_l2_batch({'x': [{'time': 1, 'large': 'x'*1000}]}, 'x', max_bytes=100)


def test_callback_ownership_survives_native_buffer_reuse(rig):
    source, wire, manager, handlers, session = rig
    seen = []
    session.subscribe('000001.SZ', 'l2quote', seen.append)
    wire.write_release.clear()
    native = {'time': [1, 2], 'askPrice': [[10]*10, [11]*10]}
    source.push(native)
    assert wire.write_entered.wait(1)
    native['askPrice'][0][0] = 999
    native['time'][0] = 999
    wire.write_release.set()
    wait_for(lambda: bool(seen))
    assert seen[0]['000001.SZ'][0]['time'] == 1
    assert seen[0]['000001.SZ'][0]['askPrice'][0] == 10


def test_inline_first_native_callback_is_not_lost(rig):
    source, wire, manager, handlers, session = rig
    source.inline = [{'time': 17, 'volume': 100}]
    seen = []
    session.subscribe('600000.SH', 'l2order', seen.append)
    wait_for(lambda: len(seen) == 1)
    assert seen[0]['600000.SH'][0]['time'] == 17


def test_refcounts_and_l2_period_isolation(rig):
    source, wire, manager, handlers, session = rig
    a = manager.subscribe('a', '1', '000001.SZ', 'l2quote')
    b = manager.subscribe('b', '1', '000001.SZ', 'l2quote')
    c = manager.subscribe('b', '2', '000001.SZ', 'l2transaction')
    assert a['epoch'] == b['epoch'] != c['epoch']
    assert len(source.callbacks) == 2
    manager.unsubscribe('a', '1')
    assert not source.closed
    manager.unsubscribe('b', '1')
    assert source.closed == [1]
    assert manager.keepalive('a', '1')['missing']


def test_idempotency_and_changed_id_rejected(rig):
    source, wire, manager, handlers, session = rig
    a = manager.subscribe('a', '1', '000001.SZ', 'l2order')
    assert manager.subscribe('a', '1', '000001.SZ', 'l2order')['epoch'] == a['epoch']
    with pytest.raises(ValueError):
        manager.subscribe('a', '1', '600000.SH', 'l2order')
    assert len(source.callbacks) == 1


def test_joiner_starts_at_registration_boundary_and_receives_future_batches(rig):
    source, wire, manager, handlers, session = rig
    first_seen, second_seen = [], []
    first = session.subscribe('000001.SZ', 'l2order', first_seen.append)
    source.push([{'time': 1}])
    wait_for(lambda: len(first_seen) == 1)
    second = session.subscribe('000001.SZ', 'l2order', second_seen.append)
    source.push([{'time': 2}])
    wait_for(lambda: len(first_seen) == 2 and len(second_seen) == 1)
    assert second_seen[0]['000001.SZ'][0]['time'] == 2
    session.unsubscribe(first)
    assert source.closed == []
    session.unsubscribe(second)
    assert source.closed == [1]


def test_heartbeat_expiry_releases_only_expired_references(rig):
    source, wire, manager, handlers, session = rig
    manager.subscribe('a', '1', '000001.SZ', 'l2order')
    manager.subscribe('b', '2', '000001.SZ', 'l2order')
    manager._clients[('a', '1')]['clients'][('a', '1')]['seen'] -= 100
    manager.reap_expired()
    assert source.closed == [] and manager.keepalive('a', '1')['missing']
    manager._clients[('b', '2')]['clients'][('b', '2')]['seen'] -= 100
    manager.reap_expired()
    assert source.closed == [1]


def test_invalid_owner_and_non_redis_client_fail_before_subscribing(rig):
    source, wire, manager, handlers, session = rig
    with pytest.raises(ValueError):
        manager.subscribe(None, None, '000001.SZ', 'l2order')
    class Client:
        transport_name = 'zmq'
    with pytest.raises(RuntimeError, match='requires Redis'):
        BigQmtXtData(Client()).subscribe_quote('000001.SZ', 'l2order')
    assert source.callbacks == {}


def test_missing_l2_server_support_is_an_explicit_error():
    handlers = BigQmtRpcHandlers('TEST', None, None)
    with pytest.raises(RuntimeError, match='unavailable'):
        handlers.handle('subscribe_l2_quote', {})


def test_native_rejection_rolls_back_and_late_callbacks_cannot_enter_new_epoch(rig):
    source, wire, manager, handlers, session = rig
    source.reject = True
    with pytest.raises(RuntimeError):
        manager.subscribe('a', '1', '000001.SZ', 'l2order')
    assert manager.status()['streams'] == []
    source.reject = False
    first = manager.subscribe('a', '1', '000001.SZ', 'l2order')
    old = source.callbacks[1][2]
    manager.unsubscribe('a', '1')
    second = manager.subscribe('a', '1', '000001.SZ', 'l2order')
    old({'000001.SZ': [{'time': 1}]})
    assert second['epoch'] != first['epoch']
    assert manager.status()['streams'][0]['sequence'] == 0


def test_queue_overflow_is_latched_and_native_callback_never_waits_for_redis():
    source, wire = Context(), MemoryRedis()
    manager = L2SubscriptionManager(source, wire, 'TEST', queue_batches=1, copy_budget_seconds=1)
    try:
        manager.subscribe('a', '1', '000001.SZ', 'l2order')
        wire.write_release.clear()
        source.push([{'time': 1}])
        assert wire.write_entered.wait(1)
        source.push([{'time': 2}])
        before = time.perf_counter()
        source.push([{'time': 3}])
        assert time.perf_counter() - before < 0.1
        state = manager.keepalive('a', '1')
        assert state['fault'] and state['dropped_batches'] == 1
        assert manager.status()['queue_bytes'] > 0  # in-flight batch still reserved
    finally:
        wire.write_release.set()
        manager.stop()
    assert manager.status()['queue_bytes'] == 0


def test_writer_failure_reaches_client_even_when_no_next_frame(rig):
    source, wire, manager, handlers, session = rig
    seen = []
    session.subscribe('000001.SZ', 'l2order', seen.append)
    wire.fail_write = True
    source.push([{'time': 1}])
    wait_for(lambda: bool(manager.status()['streams'][0]['fault']))
    session._heartbeat_once()
    assert session.status()[0]['fault'] and seen == []


def test_stop_waits_for_entered_copy_and_releases_all_reservations(monkeypatch):
    from bigqmt_signal_trader import l2_push
    entered, release = threading.Event(), threading.Event()
    original = l2_push.copy_l2_batch
    def delayed(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        return original(*args, **kwargs)
    monkeypatch.setattr(l2_push, 'copy_l2_batch', delayed)
    source, wire = Context(), MemoryRedis()
    manager = L2SubscriptionManager(source, wire, 'TEST', copy_budget_seconds=1)
    manager.subscribe('a', '1', '000001.SZ', 'l2order')
    producer = threading.Thread(target=lambda: source.push([{'time': 1}]))
    producer.start()
    assert entered.wait(1)
    closer = threading.Thread(target=manager.stop)
    closer.start()
    try:
        assert closer.is_alive()
    finally:
        release.set()
        producer.join(2)
        closer.join(2)
    assert not closer.is_alive() and not manager._worker.is_alive()
    assert manager.status()['queue_bytes'] == 0


def test_retention_gap_stops_delivery_and_short_disconnect_replays(rig):
    source, wire, manager, handlers, session = rig
    wire.fail_read = True
    seen = []
    session.subscribe('000001.SZ', 'l2order', seen.append)
    source.push([{'time': 1}])
    source.push([{'time': 2}])
    wait_for(lambda: manager.status()['streams'][0]['published'] == 2)
    wire.fail_read = False
    wait_for(lambda: len(seen) == 2)
    wire.fail_read = True
    time.sleep(0.25)  # let already-entered bounded xread return before trimming
    manager.stream_maxlen = 1
    for i in range(3, 6):
        source.push([{'time': i}])
    wait_for(lambda: manager.status()['streams'][0]['published'] == 5)
    wire.fail_read = False
    wait_for(lambda: bool(session.status()[0]['fault']))
    assert 'gap' in session.status()[0]['fault']
    assert len(seen) == 2


def test_server_restart_and_callback_errors_are_not_silent(rig):
    source, wire, manager, handlers, session = rig
    def broken(data):
        raise RuntimeError('application failed')
    sid = session.subscribe('000001.SZ', 'l2order', broken)
    source.push([{'time': 1}])
    wait_for(lambda: bool(session.status()[0]['fault']))
    assert 'user callback' in session.status()[0]['fault']
    session.unsubscribe(sid)
    sid = session.subscribe('600000.SH', 'l2order')
    manager.unsubscribe(session.client_id, str(sid))
    session._heartbeat_once()
    assert 'lost/restarted' in session.status()[0]['fault']


def test_reaper_retries_failed_native_unsubscribe(rig):
    source, wire, manager, handlers, session = rig
    manager.subscribe('a', '1', '000001.SZ', 'l2order')
    source.fail_close = True
    manager.unsubscribe('a', '1')
    assert manager.status()['streams'][0]['fault']
    source.fail_close = False
    manager.reap_expired()
    assert manager.status()['streams'] == [] and source.closed == [1]


def test_l2_registration_always_uses_qmt_thread_even_with_listener_wildcard(rig):
    source, wire, manager, handlers, session = rig
    service = RedisPubSubRpcService(wire, handlers, process_in_listener=True, listener_methods=['*'])
    assert not service._should_process_in_listener({'method': 'subscribe_l2_quote'})
    assert not service._should_process_in_listener({'method': 'unsubscribe_l2_quote'})


def test_xtdata_l2_never_enters_poller_or_primes_a_snapshot(rig):
    source, wire, manager, handlers, session = rig
    class Client:
        transport_name = 'redis'
        def _redis(self):
            return wire
        def call(self, method, params, use_formula=False):
            assert use_formula is False
            return handlers.handle(method, params)
    data = BigQmtXtData(Client())
    seen = []
    try:
        sid = data.subscribe_quote('000001.SZ', period='l2transaction', callback=seen.append)
        assert data._bar_pollers == {} and data._quote_session is None and seen == []
        source.push([{'time': 1}, {'time': 2}])
        wait_for(lambda: len(seen) == 1)
        assert len(seen[0]['000001.SZ']) == 2
        assert data.l2_subscription_status()[0]['delivered_records'] == 2
        assert data.stop_all_subscriptions() == 1
        assert source.closed == [1]
    finally:
        data.stop_all_subscriptions()


@pytest.mark.parametrize('kwargs', [{'count': 1}, {'start_time': '20260101'}, {'end_time': '20260102'}])
def test_l2_history_request_cannot_silently_become_live_only(kwargs):
    with pytest.raises(ValueError, match='live-only'):
        BigQmtXtData(object()).subscribe_quote('000001.SZ', 'l2order', **kwargs)


def test_real_redis_stream_roundtrip():
    url = os.environ.get('BIGQMT_TEST_REDIS_URL')
    if not url:
        pytest.skip('set BIGQMT_TEST_REDIS_URL to a dedicated test Redis DB')
    import redis
    wire = redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    source = Context()
    manager = L2SubscriptionManager(source, wire, 'L2_TEST_ONLY', copy_budget_seconds=1)
    handlers = BigQmtRpcHandlers('L2_TEST_ONLY', None, None, l2_subscription_manager=manager)
    session = L2ClientSession(handlers.handle, wire, itertools.count(1).__next__)
    keys = []
    try:
        seen = []
        sid = session.subscribe('000001.SZ', 'l2transaction', seen.append)
        keys.append(manager.keepalive(session.client_id, sid)['stream'])
        for i in range(20):
            source.push([{'time': 20260911145959123, 'tradeIndex': i*2},
                         {'time': 20260911145959124, 'tradeIndex': i*2+1}])
        wait_for(lambda: len(seen) == 20)
        assert [r['tradeIndex'] for b in seen for r in b['000001.SZ']] == list(range(40))
        assert seen[0]['000001.SZ'][0]['time'] == 20260911145959123
        assert not session.status()[0]['fault']
    finally:
        session.stop()
        manager.stop()
        for key in keys:
            wire.delete(key)  # only this test's UUID stream, never FLUSHDB
        wire.close()
