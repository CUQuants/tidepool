import asyncio
import json

import pyarrow.dataset as ds
import pytest

from src.okx_collector import OKXCollector


def book(action, seq, prev=-1, bids=None, asks=None):
    return {'arg': {'channel': 'books', 'instId': 'BTC-USDT'}, 'action': action,
            'data': [{'ts': '1700000000000', 'seqId': seq, 'prevSeqId': prev,
                      'bids': bids or [], 'asks': asks or []}]}


def test_normalized_book_and_trade_are_persisted(tmp_path):
    async def scenario():
        c = OKXCollector(['BTC/USDT'], {'hot_dir': str(tmp_path)})
        await c.handle_okx(book('snapshot', 10, bids=[['100', '2', '0', '1']], asks=[['101', '3', '0', '1']]))
        await c.handle_okx(book('update', 11, 10, bids=[['100', '0', '0', '0'], ['99', '4', '0', '1']]))
        await c.handle_okx({'arg': {'channel': 'trades', 'instId': 'BTC-USDT'}, 'data': [
            {'ts': '1700000000001', 'tradeId': '123', 'side': 'buy', 'px': '101', 'sz': '0.5'}]})
        await c.writer.stop()
    asyncio.run(scenario())
    root = tmp_path / 'okx'
    rows = ds.dataset(root / 'book_snapshot', format='parquet').to_table().to_pylist()
    assert [(r['symbol'], r['bid_1_price'], r['bid_1_qty']) for r in rows] == [
        ('BTC/USDT', 100.0, 2.0), ('BTC/USDT', 99.0, 4.0)]
    trade = ds.dataset(root / 'trade', format='parquet').to_table().to_pylist()[0]
    assert (trade['ts'], trade['trade_id'], trade['qty'], trade['side']) == (1700000000001000000, 123, 0.5, 'buy')
    assert rows[0]['checksum'] is None


def test_sequence_gap_requires_new_snapshot(tmp_path):
    async def scenario():
        c = OKXCollector(['BTC/USDT'], {'hot_dir': str(tmp_path)})
        await c.handle_okx(book('update', 2, 1, bids=[['99', '1']]))
        assert not c.orderbooks
        await c.handle_okx(book('snapshot', 10, bids=[['100', '1']]))
        await c.handle_okx(book('update', 12, 11, bids=[['99', '1']]))
        assert not c.orderbooks
        assert c.gaps[('BTC/USDT', 'book')][0] == 'disconnect'
        await c.handle_okx(book('update', 13, 12, bids=[['99', '1']]))
        assert not c.orderbooks
        await c.handle_okx(book('snapshot', 14, bids=[['98', '1']]))
        assert c.orderbooks['BTC/USDT']['bids'] == {98.0: 1.0}
        await c.writer.stop()
    asyncio.run(scenario())


def test_deep_level_moves_into_top_25(tmp_path):
    async def scenario():
        c = OKXCollector(['BTC/USDT'], {'hot_dir': str(tmp_path)})
        bids = [[str(price), '1'] for price in range(100, 70, -1)]
        await c.handle_okx(book('snapshot', 1, bids=bids))
        await c.handle_okx(book('update', 2, 1, bids=[['100', '0']]))
        await c.writer.stop()
    asyncio.run(scenario())
    rows = ds.dataset(tmp_path / 'okx' / 'book_snapshot', format='parquet').to_table().to_pylist()
    assert rows[-1]['bid_25_price'] == 75.0


def test_gateway_outage_opens_gaps_and_rejects_updates(tmp_path):
    async def scenario():
        c = OKXCollector(['BTC/USDT'], {'hot_dir': str(tmp_path)})
        await c.handle_okx(book('snapshot', 1, bids=[['100', '1']]))
        await c.handle_okx({'arg': {'channel': 'trades', 'instId': 'BTC-USDT'}, 'data': [
            {'ts': '1700000000001', 'tradeId': '123', 'side': 'buy', 'px': '101', 'sz': '0.5'}]})
        await c.handle_okx({'event': 'upstream_status', 'status': 'reconnecting'})
        assert not c.orderbooks
        assert c.gaps[('BTC/USDT', 'book')][0] == 'disconnect'
        assert c.gaps[('BTC/USDT', 'trade')][0] == 'disconnect'
        await c.handle_okx(book('update', 2, 1, bids=[['99', '1']]))
        assert not c.orderbooks
        await c.writer.stop()
    asyncio.run(scenario())


def test_full_queue_never_keeps_a_partial_book(tmp_path):
    async def scenario():
        c = OKXCollector(['BTC/USDT'], {'hot_dir': str(tmp_path), 'queue_maxsize': 1})
        await c.handle_okx(book('snapshot', 1, bids=[['100', '1']]))
        await c.handle_okx(book('update', 2, 1, bids=[['99', '1']]))
        assert not c.orderbooks
        assert ('BTC/USDT', 'book') in c.gaps
        await c.writer.stop()
    asyncio.run(scenario())


def test_sdk_receives_gateway_frames(tmp_path):
    pytest.importorskip('proxy_client')
    import websockets
    from proxy_client.websocket import ProxyWebSocketClient

    async def scenario():
        async def gateway(ws):
            await ws.recv()
            await ws.send(json.dumps({'event': 'auth', 'code': '0', 'operator_id': 'test'}))
            while True:
                try:
                    request = json.loads(await ws.recv())
                except websockets.exceptions.ConnectionClosed:
                    break
                arg = request['args'][0]
                await ws.send(json.dumps({'event': 'subscribe', 'arg': arg, 'id': request['id']}))
                if arg['channel'] == 'books':
                    await ws.send(json.dumps(book('snapshot', 10, bids=[['100', '2']])))
        server = await websockets.serve(gateway, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        def client():
            return ProxyWebSocketClient.for_operator(f'http://127.0.0.1:{port}', 'key', 'secret',
                                                      'test', 'Test', exchange='okx', reconnect=None,
                                                      heartbeat_interval=None)
        collector = OKXCollector(['BTC/USDT'], {'hot_dir': str(tmp_path)}, client_factory=client)
        task = asyncio.create_task(collector.run())
        try:
            for _ in range(100):
                if 'BTC/USDT' in collector.orderbooks:
                    break
                await asyncio.sleep(0.01)
            assert collector.orderbooks['BTC/USDT']['bids'] == {100.0: 2.0}
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            server.close()
            await server.wait_closed()
    asyncio.run(scenario())
