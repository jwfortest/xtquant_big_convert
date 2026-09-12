"""Bounded, two-stock live L2 verification client. No account/trading requests."""
import argparse
import datetime
import json
import os
from pathlib import Path
import threading
import time

os.environ['BIGQMT_AUTO_SYNC'] = '0'
os.environ['BIGQMT_FORMULA_ENABLED'] = '0'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=16379)
    parser.add_argument('--db', type=int, default=12)
    parser.add_argument('--namespace', default='L2_VERIFY_ONLY')
    parser.add_argument('--seconds', type=float, default=60)
    parser.add_argument('--output', default='.local/state/l2-realtime-result.json')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error('seconds must be between 1 and 300')
    import redis
    from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtData
    wire = redis.Redis(host='127.0.0.1', port=args.port, db=args.db,
                       socket_connect_timeout=2, socket_timeout=2)
    client = BigQmtRpcClient(account_id=args.namespace, redis_client=wire,
        redis_config={'host': '127.0.0.1', 'port': args.port, 'db': args.db,
                      'transport': 'redis', 'formula_server': {'enabled': False}},
        timeout_seconds=4, transport='redis')
    xtdata = BigQmtXtData(client)
    lock = threading.Lock()
    received = {}
    report = dict(started_at=datetime.datetime.now().isoformat(), source='real_qmt_if_connected',
                  subscriptions=[], errors=[], samples={})

    def callback(code, period):
        key = code + '/' + period
        def collect(data):
            rows = data[code]
            with lock:
                state = received.setdefault(key, dict(batches=0, records=0, first=None, last=None))
                state['batches'] += 1
                state['records'] += len(rows)
                if rows:
                    if state['first'] is None:
                        state['first'] = rows[0]
                    state['last'] = rows[-1]
        return collect
    try:
        ping = client.call('ping', use_formula=False)
        report['server'] = ping
        if not ping.get('verification_only') or ping.get('allow_order_methods'):
            raise RuntimeError('requires the separate, trading-disabled L2 verification entry')
        for code in ('000001.SZ', '600000.SH'):
            for period in ('l2quote', 'l2transaction', 'l2order'):
                try:
                    sid = xtdata.subscribe_quote(code, period, callback=callback(code, period))
                    report['subscriptions'].append(dict(code=code, period=period, sid=sid))
                except Exception as exc:
                    report['errors'].append('%s/%s: %s' % (code, period, exc))
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            time.sleep(min(0.25, max(0, deadline-time.monotonic())))
        report['server_streams'] = client.call('l2_subscription_status', {}, use_formula=False)
        report['client_streams'] = xtdata.l2_subscription_status()
    except (Exception, KeyboardInterrupt) as exc:
        report['errors'].append(type(exc).__name__ + ': ' + str(exc))
    finally:
        try:
            xtdata.stop_all_subscriptions()
        except Exception as exc:
            report['errors'].append('cleanup: ' + str(exc))
        with lock:
            report['samples'] = received
        report['finished_at'] = datetime.datetime.now().isoformat()
        report['conclusion'] = 'observations_only; inspect raw times/fields and continuity, no performance certification'
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        print('Saved ' + str(output.resolve()))
        print(json.dumps({key: dict(batches=row['batches'], records=row['records'])
                          for key, row in received.items()}, ensure_ascii=False))
        if not received:
            print('No live callbacks: permissions cannot be inferred outside market hours.')
        wire.close()
    return 1 if report['errors'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
