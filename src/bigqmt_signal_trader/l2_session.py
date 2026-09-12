"""One Redis stream reader and one batched heartbeat per xtdata L2 session."""
import json
import logging
import threading
import time
import uuid

log = logging.getLogger(__name__)


def _text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else str(value)


class L2ClientSession:
    def __init__(self, rpc_call, redis_client, sub_id_func,
                 heartbeat_seconds=3.0, client_id=None):
        self._rpc, self.redis, self._next_id = rpc_call, redis_client, sub_id_func
        self.client_id = client_id or uuid.uuid4().hex
        self.heartbeat_seconds = float(heartbeat_seconds)
        self._lock = threading.RLock()
        self._entries = {}
        self._stop = threading.Event()
        self._reader = self._heartbeat = None

    def subscribe(self, code, period, callback=None):
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError('L2 session is stopped')
            sid = self._next_id()
        try:
            info = self._rpc('subscribe_l2_quote', dict(client_id=self.client_id,
                sub_id=str(sid), stock_code=code, period=period))
            if not isinstance(info, dict) or info.get('mode') != 'native_l2_redis_stream':
                raise RuntimeError('server does not support native L2 push; upgrade server')
            entry = dict(info, sid=sid, callback=callback, cursor='0-0',
                last_sequence=int(info['start_sequence']), delivered_batches=0,
                delivered_records=0, fault=info.get('fault') or '', active=True,
                last_ack=time.monotonic(), lock=threading.RLock())
            with self._lock:
                if self._stop.is_set():
                    raise RuntimeError('L2 session stopped while subscribing')
                self._entries[sid] = entry
                if self._reader is None:
                    self._reader = threading.Thread(target=self._read_loop,
                        name='bigqmt-l2-reader', daemon=True)
                    self._heartbeat = threading.Thread(target=self._heartbeat_loop,
                        name='bigqmt-l2-keepalive', daemon=True)
                    self._reader.start()
                    self._heartbeat.start()
            return sid
        except Exception:
            # Includes RPC timeout: a source subscription may already exist.
            # Release the same id; server heartbeat expiry is the final fallback.
            try:
                self._rpc('unsubscribe_l2_quote', dict(client_id=self.client_id, sub_id=str(sid)))
            except Exception:
                log.exception('L2 subscribe cleanup failed; waiting for server lease expiry')
            raise

    def has_subscription(self, sid):
        with self._lock:
            return sid in self._entries

    def _fault(self, entry, reason):
        if not entry['fault']:
            entry['fault'] = str(reason)
            log.error('L2 %s %s stopped delivering: %s; unsubscribe and resubscribe',
                      entry['code'], entry['period'], reason)

    def _deliver(self, entry, msg_id, fields):
        # Unsubscribe waits for an already-entered user callback; once it
        # returns, no stale callback can fire for that local handle.
        with entry['lock']:
            if not entry['active'] or entry['fault']:
                return
            packet = json.loads(fields.get(b'payload', fields.get('payload')))
            if (packet.get('epoch') != entry['epoch'] or packet.get('code') != entry['code']
                    or packet.get('period') != entry['period']):
                self._fault(entry, 'L2 stream identity mismatch')
                return
            seq = int(packet['sequence'])
            if seq <= entry['last_sequence']:
                entry['cursor'] = _text(msg_id)  # old prefix or transport duplicate only
                return
            if seq != entry['last_sequence'] + 1:
                self._fault(entry, 'L2 stream gap: expected %d, got %d (retention/overflow)'
                            % (entry['last_sequence'] + 1, seq))
                return
            data = packet['data']
            rows = data.get(entry['code']) if isinstance(data, dict) else None
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                self._fault(entry, 'invalid L2 callback payload')
                return
            try:
                if entry['callback'] is not None:
                    entry['callback'](data)
            except Exception as exc:
                self._fault(entry, 'user callback failed: %s' % exc)
                return
            entry['cursor'] = _text(msg_id)
            entry['last_sequence'] = seq
            entry['delivered_batches'] += 1
            entry['delivered_records'] += len(rows)

    def _snapshot(self):
        with self._lock:
            return list(self._entries.values())

    def _read_loop(self):
        failed = False
        while not self._stop.is_set():
            entries = [e for e in self._snapshot() if e['active'] and not e['fault']]
            streams = {}
            by_stream = {}
            for entry in entries:
                key = entry['stream']
                cursor = entry['cursor']
                if key not in streams or tuple(map(int, cursor.split('-'))) < tuple(map(int, streams[key].split('-'))):
                    streams[key] = cursor
                by_stream.setdefault(key, []).append(entry)
            if not streams:
                self._stop.wait(0.1)
                continue
            try:
                # Redis COUNT is per stream. One batch per stream bounds each
                # reply and gives busy symbols fair turns without a polling delay:
                # XREAD wakes immediately when any subscribed stream has data.
                batches = self.redis.xread(streams, count=1, block=200)
                if failed:
                    log.warning('L2 Redis reader reconnected; retained cursors will be checked for gaps')
                failed = False
                for key, messages in batches:
                    for msg_id, fields in messages:
                        for entry in by_stream.get(_text(key), []):
                            if self._stop.is_set():
                                return
                            try:
                                self._deliver(entry, msg_id, fields)
                            except Exception as exc:
                                self._fault(entry, 'L2 payload decode failed: %s' % exc)
            except Exception as exc:
                if not failed and not self._stop.is_set():
                    log.warning('L2 Redis disconnected, retaining cursors: %s', exc)
                failed = True
                self._stop.wait(0.5)

    def _heartbeat_once(self):
        entries = self._snapshot()
        if not entries:
            return
        result = self._rpc('l2_keepalive', dict(client_id=self.client_id,
                                               sub_ids=[str(e['sid']) for e in entries]))
        states = result.get('subscriptions', {})
        for entry in entries:
            state = states.get(str(entry['sid']), {'missing': True})
            with entry['lock']:
                entry['last_ack'] = time.monotonic()
                if state.get('missing') or state.get('epoch') != entry['epoch']:
                    self._fault(entry, 'server subscription lost/restarted; continuity unknown')
                elif state.get('fault'):
                    self._fault(entry, state['fault'])

    def _heartbeat_loop(self):
        failures = 0
        while not self._stop.wait(self.heartbeat_seconds):
            try:
                self._heartbeat_once()
                failures = 0
            except Exception as exc:
                failures += 1
                if failures == 1 or failures & (failures - 1) == 0:
                    log.warning('L2 heartbeat failed (attempt %d): %s', failures, exc)

    def status(self):
        now = time.monotonic()
        return [dict(sid=e['sid'], code=e['code'], period=e['period'], epoch=e['epoch'],
            last_sequence=e['last_sequence'], delivered_batches=e['delivered_batches'],
            delivered_records=e['delivered_records'], fault=e['fault'],
            heartbeat_age_seconds=now-e['last_ack'],
            healthy=not e['fault'] and now-e['last_ack'] < max(10, self.heartbeat_seconds*3))
            for e in self._snapshot()]

    def unsubscribe(self, sid):
        with self._lock:
            entry = self._entries.get(sid)
        if entry is None:
            return 0
        with entry['lock']:
            entry['active'] = False
        # Keep an inactive entry on failure so an explicit retry remains possible.
        self._rpc('unsubscribe_l2_quote', dict(client_id=self.client_id, sub_id=str(sid)))
        with self._lock:
            self._entries.pop(sid, None)
        return 0

    def stop(self):
        self._stop.set()
        errors = []
        for entry in self._snapshot():
            try:
                self.unsubscribe(entry['sid'])
            except Exception as exc:
                errors.append(exc)
        for worker in (self._reader, self._heartbeat):
            if worker is not None and worker is not threading.current_thread():
                worker.join(1.0)
        if errors:
            raise RuntimeError('L2 stop: unsubscribe failed; server lease will expire: %s' % errors[0])
