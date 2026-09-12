"""Native QMT L2 callbacks over bounded Redis Streams (Python 3.6 compatible).

Market callbacks only copy/enqueue. A dedicated writer serializes and sends all
records, without latest-row coalescing. Faults latch: never silently fall back to
polling or call an incomplete stream healthy. See docs/L2_REALTIME_PUSH.md.
"""
import json
import logging
import math
import queue
import threading
import time
import uuid

from .code_utils import normalize_stock_code

L2_PERIODS = frozenset(('l2quote', 'l2quoteaux', 'l2transaction', 'l2order',
                        'l2transactioncount', 'l2orderqueue'))
log = logging.getLogger(__name__)


def copy_l2_batch(data, code, max_bytes=262144, max_records=4096,
                  copy_budget_seconds=0.005):
    """Own the callback data and normalize row/column batches without truncation.

    Column orientation is identified by an array-valued time/stime field, not
    by an array-valued bidPrice (a single ten-level quote also contains arrays).
    Raw timestamps and market sequence numbers are never cast to float.
    """
    budget = [0, 0]
    deadline = time.perf_counter() + copy_budget_seconds

    def clone(value, depth=0):
        budget[0] += 64  # conservative per-node ownership reservation
        budget[1] += 1
        if depth > 12 or budget[0] > max_bytes or budget[1] > 20000:
            raise ValueError('L2 callback exceeds copy capacity')
        if budget[1] % 64 == 0 and time.perf_counter() > deadline:
            raise ValueError('L2 callback exceeds copy time budget')
        if value is None or type(value) in (bool, int):
            return value
        if isinstance(value, str):
            budget[0] += len(value) * 4
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError('non-finite L2 value')
            return float(value)
        if isinstance(value, dict):
            return {clone(str(k), depth + 1): clone(v, depth + 1) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [clone(v, depth + 1) for v in value]
        if hasattr(value, 'tolist'):
            return clone(value.tolist(), depth + 1)
        if hasattr(value, 'item'):
            return clone(value.item(), depth + 1)
        raise TypeError('unsupported L2 value: %s' % type(value).__name__)

    if not isinstance(data, dict) or code not in data:
        raise ValueError('L2 callback must contain subscribed code %s' % code)
    raw = clone(data[code])
    if isinstance(raw, dict):
        times = next((raw[k] for k in ('time', 'stime', 'timetag')
                      if isinstance(raw.get(k), list)), None)
        if times is None:
            rows = [raw] if raw else []
        else:
            count = len(times)
            if any(not isinstance(v, list) or len(v) != count for v in raw.values()):
                raise ValueError('inconsistent L2 column lengths')
            rows = [{k: v[i] for k, v in raw.items()} for i in range(count)]
            budget[0] += count * 64
    elif isinstance(raw, list) and all(isinstance(row, dict) for row in raw):
        rows = raw
    else:
        raise ValueError('unsupported L2 callback shape')
    if len(rows) > max_records or budget[0] > max_bytes:
        raise ValueError('L2 batch exceeds capacity')
    if time.perf_counter() > deadline:
        raise ValueError('L2 callback exceeds copy time budget')
    return {code: rows}, budget[0]


class L2SubscriptionManager:
    """Native subscribe/unsubscribe/reap must be called on the QMT thread."""
    def __init__(self, context, redis_client, account_id, max_subscriptions=128,
                 queue_batches=1024, queue_bytes=16777216, batch_bytes=262144,
                 stream_maxlen=256, stream_ttl_seconds=90,
                 heartbeat_timeout_seconds=30, copy_budget_seconds=0.005):
        self.context, self.redis = context, redis_client
        self.account_id = str(account_id)
        self.max_subscriptions = max(1, int(max_subscriptions))
        self.queue_bytes = max(1, int(queue_bytes))
        self.batch_bytes = max(1, int(batch_bytes))
        self.stream_maxlen = max(1, int(stream_maxlen))
        self.ttl = max(10, int(stream_ttl_seconds))
        self.heartbeat_timeout = max(3, float(heartbeat_timeout_seconds))
        self.copy_budget = max(0.0001, float(copy_budget_seconds))
        self._entries, self._clients = {}, {}
        self._lock = threading.RLock()
        self._queue = queue.Queue(maxsize=max(1, int(queue_batches)))
        self._budget_lock = threading.Lock()
        self._reserved = 0
        self._stop = threading.Event()
        self._closing = False
        self._worker = None

    def _start_writer(self):
        if self._stop.is_set():
            raise RuntimeError('L2 manager is stopped')
        if self._worker is None:
            self._worker = threading.Thread(target=self._write_loop,
                                             name='bigqmt-l2-writer', daemon=True)
            self._worker.start()

    def subscribe(self, client_id, sub_id, code, period):
        code = normalize_stock_code(code)
        period = str(period).lower()
        if period not in L2_PERIODS:
            raise ValueError('unsupported native L2 period')
        owner, key = (str(client_id or ''), str(sub_id or '')), (code, period)
        if not all(owner):
            raise ValueError('client_id and sub_id are required')
        with self._lock:
            if self._closing:
                raise RuntimeError('L2 manager is stopping')
            if owner in self._clients:
                entry = self._clients[owner]
                if entry['key'] != key:
                    raise ValueError('L2 subscription id already used')
                entry['clients'][owner]['seen'] = time.monotonic()
                return self._info(entry, owner)
            entry = self._entries.get(key)
            if len(self._clients) >= self.max_subscriptions * 16:
                raise RuntimeError('L2 client subscription capacity reached')
            if entry is not None and entry['fault']:
                raise RuntimeError('L2 stream faulted; unsubscribe existing handles first')
            if entry is None:
                if len(self._entries) >= self.max_subscriptions:
                    raise RuntimeError('native L2 subscription capacity reached')
                self._start_writer()
                epoch = uuid.uuid4().hex
                entry = dict(key=key, code=code, period=period, epoch=epoch,
                    stream='bigqmt:l2:%s:%s' % (self.account_id, epoch),
                    sequence=0, published=0, records=0, dropped=0, fault='',
                    active=True, handle=None, clients={}, lock=threading.Lock())
                self._entries[key] = entry
                # Install ownership before subscribe: QMT can callback inline.
                entry['clients'][owner] = dict(seen=time.monotonic(), start=0)
                self._clients[owner] = entry
                try:
                    handle = self.context.subscribe_quote(code, period, 'none',
                                                          'list', self._callback(entry))
                    if not isinstance(handle, int) or isinstance(handle, bool) or handle <= 0:
                        raise RuntimeError('QMT rejected native L2 subscription: %r' % handle)
                    entry['handle'] = handle
                except Exception:
                    entry['active'] = False
                    self._clients.pop(owner, None)
                    self._entries.pop(key, None)
                    raise
            else:
                with entry['lock']:
                    entry['clients'][owner] = dict(seen=time.monotonic(), start=entry['sequence'])
                self._clients[owner] = entry
            return self._info(entry, owner)

    def _info(self, entry, owner):
        return dict(stream=entry['stream'], epoch=entry['epoch'], code=entry['code'],
                    period=entry['period'], start_sequence=entry['clients'][owner]['start'],
                    sequence=entry['sequence'], published=entry['published'],
                    records=entry['records'], dropped_batches=entry['dropped'],
                    fault=entry['fault'], mode='native_l2_redis_stream')

    def keepalive(self, client_id, sub_id):
        owner = (str(client_id), str(sub_id))
        with self._lock:
            entry = self._clients.get(owner)
            if entry is None:
                return {'missing': True}
            entry['clients'][owner]['seen'] = time.monotonic()
            return self._info(entry, owner)

    def unsubscribe(self, client_id, sub_id):
        owner = (str(client_id), str(sub_id))
        with self._lock:
            entry = self._clients.pop(owner, None)
            if entry is None:
                return
            entry['clients'].pop(owner, None)
            if not entry['clients']:
                self._close_entry(entry)

    def _close_entry(self, entry):
        entry['active'] = False  # late callbacks cannot enter a new generation
        # Finish an already-entered bounded copy before closing its generation.
        # The writer keeps running until stop has crossed all these barriers.
        with entry['lock']:
            pass
        try:
            if entry['handle'] is not None:
                self.context.unsubscribe_quote(entry['handle'])
        except Exception as exc:
            entry['fault'] = 'native unsubscribe failed: %s' % exc
            log.error(entry['fault'])
            return  # keep handle; reaper retries, never forget a live native sub
        self._entries.pop(entry['key'], None)

    def reap_expired(self):
        with self._lock:
            now = time.monotonic()
            for owner, entry in list(self._clients.items()):
                if now - entry['clients'][owner]['seen'] > self.heartbeat_timeout:
                    self.unsubscribe(*owner)
            for entry in list(self._entries.values()):
                if not entry['clients']:
                    self._close_entry(entry)

    def status(self):
        with self._lock:
            return {'queue_bytes': self._reserved, 'queue_batches': self._queue.qsize(),
                'streams': [dict(code=e['code'], period=e['period'], epoch=e['epoch'],
                    clients=len(e['clients']), sequence=e['sequence'], published=e['published'],
                    records=e['records'], dropped_batches=e['dropped'], fault=e['fault'])
                    for e in self._entries.values()]}

    def _fault(self, entry, reason):
        # The callback must not wait on logging/network locks. Diagnostics are
        # returned by status/keepalive and reported by the external client.
        if not entry['fault']:
            entry['fault'] = str(reason)
        entry['dropped'] += 1

    def _callback(self, entry):
        def receive(raw):
            if not entry['active'] or self._stop.is_set():
                return
            if entry['fault']:
                entry['dropped'] += 1
                return
            if not entry['lock'].acquire(False):
                self._fault(entry, 'concurrent native L2 callbacks; ordering not guaranteed')
                return
            reserved = 0
            try:
                data, size = copy_l2_batch(raw, entry['code'], self.batch_bytes,
                                          copy_budget_seconds=self.copy_budget)
                if not data[entry['code']]:
                    return
                size += 512  # reserve envelope/queue bookkeeping as well
                entry['sequence'] += 1
                if not self._budget_lock.acquire(False):
                    raise RuntimeError('L2 queue reservation contention')
                try:
                    if self._reserved + size > self.queue_bytes:
                        raise RuntimeError('L2 queue byte capacity exceeded')
                    self._reserved += size
                    reserved = size
                finally:
                    self._budget_lock.release()
                packet = dict(epoch=entry['epoch'], sequence=entry['sequence'],
                    code=entry['code'], period=entry['period'], data=data,
                    received_wall_time=time.time(), received_perf_counter=time.perf_counter())
                self._queue.put_nowait((entry, packet, size))
                reserved = 0  # writer owns reservation through network completion
                entry['records'] += len(data[entry['code']])
            except Exception as exc:
                self._fault(entry, '%s: %s' % (type(exc).__name__, exc))
            finally:
                if reserved:
                    with self._budget_lock:
                        self._reserved -= reserved
                entry['lock'].release()
        return receive

    def _write_loop(self):
        while not self._stop.is_set() or not self._queue.empty():
            try:
                entry, packet, size = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                if entry['active'] and not self._stop.is_set():
                    blob = json.dumps(packet, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
                    pipe = self.redis.pipeline(transaction=False)
                    pipe.xadd(entry['stream'], {'payload': blob},
                              maxlen=self.stream_maxlen, approximate=False)
                    pipe.expire(entry['stream'], self.ttl)
                    pipe.execute()
                    entry['published'] = packet['sequence']
            except Exception as exc:
                self._fault(entry, 'Redis L2 write failed: %s' % exc)
            finally:
                with self._budget_lock:
                    self._reserved -= size
                self._queue.task_done()

    def stop(self):
        with self._lock:
            self._closing = True
            self._clients.clear()
            for entry in list(self._entries.values()):
                entry['clients'].clear()
                self._close_entry(entry)
            self._stop.set()
        if self._worker is not None and self._worker is not threading.current_thread():
            self._worker.join(1.0)
        if self._entries:
            raise RuntimeError('L2 native unsubscribe failed; retry stop before replacing manager')
