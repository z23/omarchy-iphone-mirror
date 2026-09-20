import asyncio
import json
from pathlib import Path
import queue
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch
from lifecycle import Runtime
from mirror import (
    DirectPlayer, Mirror, PlayerStats, TrackedTransport, annexb_is_key,
    hypr_client_snapshot, mpv_property_snapshot, queue_band,
)

class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_expected_player_exit_during_cleanup_is_not_failure(self):
        with tempfile.TemporaryDirectory() as root:
            runtime=Runtime(Path(root)/'runtime').acquire()
            try:
                app=Mirror(runtime)
                app.cleaning_up=True
                app.stop('player-exited')
                self.assertIsNone(app.error)
            finally:
                runtime.close()

    async def test_run_does_not_cancel_cleanup_already_started(self):
        with tempfile.TemporaryDirectory() as root:
            runtime=Runtime(Path(root)/'runtime').acquire()
            app=Mirror(runtime)
            completed=[]
            async def capture():
                app.cleaning_up=True
                app.stop_event.set()
                await asyncio.sleep(.02)
                completed.append(True)
            app.capture=capture
            try:
                await app.run()
                self.assertEqual(completed,[True])
                self.assertEqual(runtime.state['state'],'stopped')
            finally:
                runtime.close()

    async def test_capture_start_and_stop_keep_tunnel_until_cleanup(self):
        events=[]
        with tempfile.TemporaryDirectory() as root:
            runtime=Runtime(Path(root)/'runtime').acquire()
            app=Mirror(runtime)
            class Tunnel:
                def __init__(self, **kw): pass
                async def __aenter__(self):
                    events.append('tunnel-open')
                    return Mock(service=Mock(address=['::1']))
                async def __aexit__(self,*args): events.append('tunnel-close')
            async def start(**kw):
                return {'connection':{'options':{'avcMediaStreamOptionClientSessionID':{'uuid':kw['client_session_id']}},
                                      'streamConfig':{}}}
            async def stop(sid): events.append('device-stop')
            async def close(): events.append('display-close')
            service=Mock(connect=AsyncMock(), start_video_stream=AsyncMock(side_effect=start),
                         stop_media_stream=AsyncMock(side_effect=stop), close=AsyncMock(side_effect=close))
            class Bridge:
                error = None
                audio_muted = True
                def __init__(self,*args,**kwargs):
                    self.ready=asyncio.Event()
                async def run(self):
                    self.ready.set()
                    await asyncio.Event().wait()
                async def close(self): events.append('input-close')
            class Player:
                def __init__(self,*args,on_ready,**kwargs):
                    self.player=Mock(pid=123)
                    on_ready(self)
                def live_stats(self):
                    return {'q': 0, 'max_q': 0, 'skip_cycles': 0, 'wait_key': False}
                def note_queue_band(self):
                    return None
                def close(self): events.append('player-close')
            class Receiver:
                def __init__(self,*args,**kwargs): self._pli_tasks=set()
                async def _udp_recv_and_pipe(self,transport):
                    self._transcoder_cls(b'',b'',b'')
                    await asyncio.Event().wait()
                async def _rtcp_send_loop(self,transport): await asyncio.Event().wait()
            transport=Mock(port=1000,close=Mock(side_effect=lambda:events.append('transport-close')))
            with patch('connection.select_connection',AsyncMock(return_value=('usb',None))), \
                 patch('pymobiledevice3.remote.userspace_tunnel.UserspaceRsdTunnel',Tunnel), \
                 patch('pymobiledevice3.remote.core_device.display_service.DisplayService',return_value=service), \
                 patch('pymobiledevice3.remote.core_device.screen_stream.open_media_receiver',return_value=(transport,'::2')), \
                 patch('pymobiledevice3.remote.core_device.vnc_server.VncStreamServer',Receiver), \
                 patch('mirror.start_system_audio',AsyncMock(return_value=None)), \
                 patch('mirror.DirectPlayer',Player), patch('mirror.InputBridge',Bridge):
                task=asyncio.create_task(app.capture())
                try:
                    await asyncio.wait_for(app.player_ready.wait(),2)
                    # Let the bridge complete setup, then request a normal stop.
                    for _ in range(100):
                        if runtime.state['state']=='running': break
                        await asyncio.sleep(.001)
                    self.assertEqual(runtime.state['state'],'running')
                    app.stop()
                    await asyncio.wait_for(task,3)
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task,return_exceptions=True)
                    runtime.close()
            self.assertEqual(events,['tunnel-open','input-close','device-stop','player-close','transport-close','display-close','tunnel-close'])
            self.assertIsNone(app.error)


class BacklogProbeTests(unittest.TestCase):
    def test_annexb_key_detection(self):
        self.assertFalse(annexb_is_key(b''))
        self.assertFalse(annexb_is_key(b'\x00\x00\x00\x01' + bytes([1 << 1])))
        self.assertTrue(annexb_is_key(b'\x00\x00\x00\x01' + bytes([19 << 1, 0])))

    def test_stats_fill_ms_is_time_since_empty(self):
        stats = PlayerStats()
        stats.on_write(10, 0, 0)
        time.sleep(0.05)
        snap = stats.snapshot(au_bytes=40, au_key=False, q=120,
                              mpv_alive=True, mpv_returncode=None)
        self.assertGreaterEqual(snap['fill_ms'], 40)
        self.assertEqual(snap['q'], 120)
        self.assertEqual(snap['au_bytes'], 40)
        self.assertFalse(snap['au_key'])
        self.assertTrue(snap['mpv_alive'])
        self.assertIsNone(snap['mpv_returncode'])

    def test_stats_one_second_rates_ignore_older_events(self):
        stats = PlayerStats()
        stats.on_feed(100, 1)
        stats._recent.appendleft((time.monotonic() - 2, 'f', 999))
        stats.on_write(50, 3, 0)
        snap = stats.snapshot(au_bytes=1, au_key=True, q=1,
                              mpv_alive=True, mpv_returncode=None)
        self.assertEqual(snap['feeds_1s'], 1)
        self.assertEqual(snap['writes_1s'], 1)
        self.assertEqual(snap['feed_bytes_1s'], 100)
        self.assertEqual(snap['write_bytes_1s'], 50)
        self.assertEqual(snap['max_write_block_ms'], 3)
        self.assertTrue(snap['au_key'])

    def _player(self, maxsize=1):
        player = DirectPlayer.__new__(DirectPlayer)
        player._inq = queue.Queue(maxsize=maxsize)
        player._stop = threading.Event()
        player.stats = PlayerStats()
        player.backlog_snapshot = None
        player.ipc_path = Path('/tmp/iphone-mirror-no-mpv.sock')
        player.player = Mock(poll=lambda: None, returncode=None)
        player.on_stop = Mock()
        player.on_keyframe_request = Mock()
        player._wait_key = False
        player.skips = 0
        player.skip_cycles = 0
        player._last_pli = 0.0
        player._queue_band = 0
        return player

    def test_queue_band_thresholds(self):
        self.assertEqual(queue_band(0), 0)
        self.assertEqual(queue_band(29), 0)
        self.assertEqual(queue_band(30), 30)
        self.assertEqual(queue_band(59), 30)
        self.assertEqual(queue_band(60), 60)
        self.assertEqual(queue_band(90), 90)
        self.assertEqual(queue_band(120), 90)

    def test_live_stats_include_queue_and_skip_counts(self):
        player = self._player(maxsize=8)
        player._inq.put_nowait(b'held')
        player.skip_cycles = 2
        player.skips = 40
        player._wait_key = True
        snap = player.live_stats()
        self.assertEqual(snap['q'], 1)
        self.assertEqual(snap['skip_cycles'], 2)
        self.assertEqual(snap['skips'], 40)
        self.assertTrue(snap['wait_key'])
        self.assertIn('max_write_block_ms', snap)
        self.assertEqual(snap['feeds'], 0)

    def test_queue_band_logs_once_per_climb(self):
        player = self._player(maxsize=80)
        for _ in range(30):
            player._inq.put_nowait(b'x')
        with self.assertLogs('iphone-mirror', level='WARNING') as logs:
            first = player.note_queue_band()
            second = player.note_queue_band()
        self.assertEqual(first['band'], 30)
        self.assertIsNone(second)
        self.assertTrue(any('player-queue' in line for line in logs.output))
        while True:
            try:
                player._inq.get_nowait()
            except queue.Empty:
                break
        player.note_queue_band()
        self.assertEqual(player._queue_band, 0)

    def test_full_queue_skips_to_idr_instead_of_stopping(self):
        player = self._player()
        player._inq.put_nowait(b'held')
        p_frame = b'\x00\x00\x00\x01' + bytes([1 << 1, 0])
        player.feed(p_frame)
        player.on_stop.assert_not_called()
        player.on_keyframe_request.assert_called()
        self.assertTrue(player._wait_key)
        self.assertEqual(player.skip_cycles, 1)
        self.assertEqual(player.backlog_snapshot['recovery'], 'skip-idr')
        self.assertFalse(player.backlog_snapshot['au_key'])
        player.feed(p_frame)
        self.assertEqual(player._inq.qsize(), 0)
        key = b'\x00\x00\x00\x01' + bytes([20 << 1, 0])
        player.feed(key)
        self.assertFalse(player._wait_key)
        self.assertEqual(player._inq.qsize(), 1)
        player.on_stop.assert_not_called()

    def test_skip_idr_does_not_queue_p_frames_until_key(self):
        player = self._player(maxsize=8)
        player._wait_key = True
        player.feed(b'\x00\x00\x00\x01' + bytes([1 << 1, 0]))
        self.assertEqual(player._inq.qsize(), 0)
        self.assertTrue(player._wait_key)
        player.feed(b'\x00\x00\x00\x01' + bytes([19 << 1, 0]))
        self.assertFalse(player._wait_key)
        self.assertEqual(player._inq.qsize(), 1)

    def test_mpv_snapshot_empty_without_socket(self):
        self.assertEqual(mpv_property_snapshot('/tmp/iphone-mirror-missing.sock'), {})

    def test_hypr_snapshot_empty_without_pid(self):
        self.assertEqual(hypr_client_snapshot(None), {})
        self.assertEqual(hypr_client_snapshot(0), {})

    def test_snapshot_values_are_counts_not_payloads(self):
        stats = PlayerStats()
        stats.on_feed(1234, 4)
        snap = stats.snapshot(au_bytes=1234, au_key=False, q=4,
                              mpv_alive=True, mpv_returncode=None)
        for key, value in snap.items():
            if key == 'mpv_returncode':
                continue
            self.assertIsInstance(value, (int, bool, list))


class BacklogSessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_merges_rtp_into_backlog_and_state(self):
        with tempfile.TemporaryDirectory() as root:
            runtime = Runtime(Path(root) / 'runtime').acquire()
            try:
                app = Mirror(runtime)
                player = DirectPlayer.__new__(DirectPlayer)
                player.backlog_snapshot = {
                    'q': 120, 'feeds_1s': 80, 'writes_1s': 1, 'au_bytes': 900,
                    'au_key': False, 'mpv_alive': True, 'mpv_returncode': None,
                }
                app.player = player
                transport = TrackedTransport(Mock())
                transport._packets = 50
                transport._gaps = 2
                transport._max_interarrival_ms = 40
                transport._max_loop_delay_ms = 5
                app._transport = transport
                app.stop('player-backlog')
                self.assertEqual(app.error, 'player-backlog')
                self.assertEqual(app.backlog['q'], 120)
                self.assertEqual(app.backlog['rtp']['gaps'], 2)
                self.assertEqual(app.backlog['rtp']['packets'], 50)
                runtime.update('error', error=app.error, backlog=app.backlog)
                saved = json.loads((runtime.root / 'state.json').read_text())
                self.assertEqual(saved['backlog']['rtp']['gaps'], 2)
            finally:
                runtime.close()

    async def test_rtp_snapshot_counts_gaps_and_skips_first_interarrival(self):
        class Fake:
            def __init__(self):
                self.packets = [
                    bytes([0, 0, 0, 1]) + b'\x00' * 8,
                    bytes([0, 0, 0, 3]) + b'\x00' * 8,
                    bytes([0, 0, 0, 4]) + b'\x00' * 8,
                ]
            async def recv(self):
                return self.packets.pop(0)
        transport = TrackedTransport(Fake())
        await transport.recv()
        await transport.recv()
        await transport.recv()
        snap = transport.snapshot()
        self.assertEqual(snap['packets'], 3)
        self.assertEqual(snap['gaps'], 1)
        self.assertGreaterEqual(snap['max_interarrival_ms'], 0)

    async def test_rtp_snapshot_ignores_rtcp_sequence(self):
        class Fake:
            def __init__(self):
                self.packets = [
                    bytes([0, 0, 0, 1]) + b'\x00' * 8,
                    bytes([0, 201, 0, 99]) + b'\x00' * 8,  # PT 201 RR
                    bytes([0, 0, 0, 2]) + b'\x00' * 8,
                ]
            async def recv(self):
                return self.packets.pop(0)
        transport = TrackedTransport(Fake())
        await transport.recv()
        await transport.recv()
        age_after_rtcp = time.monotonic() - transport.last_packet
        await transport.recv()
        snap = transport.snapshot()
        self.assertEqual(snap['packets'], 2)
        self.assertEqual(snap['gaps'], 0)
        self.assertLess(age_after_rtcp, 0.5)

if __name__=='__main__':unittest.main()
