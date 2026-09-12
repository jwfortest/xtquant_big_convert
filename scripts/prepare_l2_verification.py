"""Prepare a separate, trading-disabled QMT entry; never edits a broker install."""
import argparse
import hashlib
from pathlib import Path
import shutil


BODY = '''
_service = None
_manager = None
_redis = None
_ticks = 0

def init(C):
    global _service, _manager, _redis
    import os, sys, datetime
    os.environ['BIGQMT_LOG_ENABLED'] = '0'
    for path in _EXTRA_PATHS:
        if path not in sys.path:
            sys.path.append(path)
    import redis
    rpc = _load_local_module('bigqmt_signal_trader.redis_rpc')
    l2 = _load_local_module('bigqmt_signal_trader.l2_push')
    class Source:
        def subscribe_quote(self, code, period, dividend, kind, callback):
            if code not in ('000001.SZ', '600000.SH') or period not in ('l2quote', 'l2transaction', 'l2order'):
                raise ValueError('verification is limited to two stocks and three L2 periods')
            return C.subscribe_quote(code, period, dividend, kind, callback)
        def unsubscribe_quote(self, handle):
            return C.unsubscribe_quote(handle)
    class Handlers(rpc.BigQmtRpcHandlers):
        def _handle_ping(self, params):
            result = super()._handle_ping(params)
            result.update(verification_only=True, pid=os.getpid(),
                python=sys.version, context_type=type(C).__name__, timer_callbacks=_ticks)
            return result
    _redis = redis.Redis(host='127.0.0.1', port=_PORT, db=_DB,
                         socket_connect_timeout=1.5, socket_timeout=1.5)
    _redis.ping()
    _manager = l2.L2SubscriptionManager(Source(), _redis, _NAMESPACE, max_subscriptions=6)
    handlers = Handlers(_NAMESPACE, None, None, allow_order_methods=False,
        allowed_methods=['ping', 'subscribe_l2_quote', 'unsubscribe_l2_quote',
                         'l2_keepalive', 'l2_subscription_status'], l2_subscription_manager=_manager)
    _service = rpc.RedisPubSubRpcService(_redis, handlers, account_id=_NAMESPACE,
        process_in_listener=True, listener_methods=['ping', 'l2_keepalive', 'l2_subscription_status'])
    try:
        _service.start()
        C.run_time('adjust', '500nMilliSecond',
                   (datetime.datetime.now()+datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S'))
    except Exception:
        stop(C)
        raise
    print('[L2_REALTIME] ready; native subscriptions start only when test client connects; orders disabled')

def adjust(C):
    global _ticks
    if _service is not None:
        _ticks += 1
        if _ticks == 1:
            print('[L2_REALTIME] timer active')
        _service.drain_pending(max_items=8)
        _manager.reap_expired()
        import time
        time.sleep(0.001)

def handlebar(C):
    import time
    time.sleep(0.001)

def stop(C):
    global _service, _manager
    if _service is not None:
        _service.stop()
        _service = None
    if _manager is not None:
        _manager.stop()
        _manager = None
    if _redis is not None:
        _redis.connection_pool.disconnect()
    print('[L2_REALTIME] stopped; native subscriptions released')
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='.local/l2-realtime-stage')
    parser.add_argument('--port', type=int, default=16379)
    parser.add_argument('--db', type=int, default=12)
    parser.add_argument('--namespace', default='L2_VERIFY_ONLY')
    parser.add_argument('--extra-path', action='append', default=[],
                        help='Matching QMT stdlib/extensions directory or redis-py wheel; repeatable')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = Path(args.output).resolve()
    if output == root or output == root / 'src':
        parser.error('output must be a separate staging directory')
    output.mkdir(parents=True, exist_ok=True)
    package_files = sorted(p for p in (root/'src/bigqmt_signal_trader').rglob('*.py')
                           if '__pycache__' not in p.parts)
    digest = hashlib.sha256(b''.join(p.read_bytes() for p in package_files)).hexdigest()[:12]
    package = 'bigqmt_l2_verify_' + digest
    # Copy only this package into the caller-selected staging directory.
    for path in (root/'src/bigqmt_signal_trader').rglob('*'):
        if path.is_file() and '__pycache__' not in path.parts:
            target = output/package/path.relative_to(root/'src/bigqmt_signal_trader')
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
    source = (root/'src/BIGQMT_REDIS_DRYRUN.py').read_text(encoding='utf-8')
    marker = '\n_stop_previous_rpc_service()\n'
    if marker not in source:
        raise RuntimeError('upstream QMT loader changed; refusing to include trading startup')
    loader = source.split(marker)[0].replace('#coding:gbk', '#coding:utf-8', 1)
    # A content-addressed package avoids stale modules from another QMT strategy
    # without purging or replacing that running strategy's sys.modules entries.
    loader = loader.replace('bigqmt_signal_trader', package)
    settings = dict(_SOURCE_ROOT=str(output), _PORT=args.port, _DB=args.db,
                    _NAMESPACE=args.namespace, _EXTRA_PATHS=[str(Path(p).resolve()) for p in args.extra_path])
    text = loader + '\n' + '\n'.join('%s = %r' % pair for pair in settings.items()) + '\n' + BODY.replace('bigqmt_signal_trader', package)
    entry = output/'BIGQMT_L2_REDIS_VERIFY.py'
    compile(text, str(entry), 'exec')
    entry.write_text(text, encoding='utf-8')
    print('Prepared ' + str(entry))
    print('QMT editor bootstrap (save as a separate strategy):')
    print('#coding:gbk\np = %r\nexec(compile(open(p, "rb").read(), p, "exec"), globals(), globals())' % str(entry))


if __name__ == '__main__':
    main()
