"""On-demand USB mirror. No global hooks, VNC listener, or saved input."""
import argparse
import asyncio
from collections import deque
import contextlib
import json
import logging
import os
from pathlib import Path
import queue
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

from audio import start_system_audio
from lifecycle import AlreadyRunning, Runtime, close_session, connect_service
from usb_input import InputBridge

log = logging.getLogger('iphone-mirror')
CONNECT_TIMEOUT = 30
IMAGE_PREP_TIMEOUT = 90
IMAGE_FAILURE_MESSAGES = {
    'cached-developer-image-missing': 'Cached developer image is missing. Set up the phone before retrying.',
    'cached-developer-image-invalid': 'Cached developer image is invalid. Replace the local cache before retrying.',
    'cached-developer-image-build-mismatch': 'Cached developer image has the wrong build. Replace the local cache before retrying.',
    'developer-image-mount-unverified': 'Developer image mount could not be verified. Check the phone before retrying.',
}

HEVC_KEY_NAL = frozenset((19, 20, 21))
QUEUE_BANDS = (30, 60, 90)
MPV_BACKLOG_PROPS = (
    'pause', 'core-idle', 'paused-for-cache', 'idle-active',
    'decoder-frame-drop-count', 'vo-delayed-frame-count',
    'hwdec-current', 'current-vo', 'video-sync', 'untimed',
    'estimated-vf-fps',
)


def _json_safe(value):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return value if value == value else None
    if isinstance(value, str) and len(value) < 64:
        return value
    return None


def mpv_property_snapshot(ipc_path, names=MPV_BACKLOG_PROPS, timeout=0.25):
    """Read selected MPV properties. Counts and short enums only; no frames."""
    result = {}
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect(str(ipc_path))
    except OSError:
        sock.close()
        return result
    try:
        for index, name in enumerate(names, 1):
            sock.sendall((json.dumps({'command': ['get_property', name], 'request_id': index})+'\n').encode())
        data = b''
        deadline = time.monotonic() + timeout
        while len(result) < len(names) and time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
            except TimeoutError:
                break
            if not chunk:
                break
            data += chunk
            while b'\n' in data:
                line, data = data.split(b'\n', 1)
                if not line:
                    continue
                try:
                    reply = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rid = reply.get('request_id')
                if not isinstance(rid, int) or not (1 <= rid <= len(names)):
                    continue
                name = names[rid - 1]
                result[name] = _json_safe(reply.get('data')) if reply.get('error') == 'success' else None
    finally:
        sock.close()
    return result


def hypr_client_snapshot(pid):
    """Mapped/hidden/workspace for the player pid. No titles."""
    if not isinstance(pid, int) or pid <= 0 or not shutil.which('hyprctl'):
        return {}
    try:
        proc = subprocess.run(['hyprctl', 'clients', '-j'], capture_output=True, text=True, timeout=0.5)
        clients = json.loads(proc.stdout)
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError, UnicodeError):
        return {}
    if not isinstance(clients, list):
        return {}
    for client in clients:
        if client.get('pid') != pid:
            continue
        workspace = client.get('workspace') if isinstance(client.get('workspace'), dict) else {}
        monitor = client.get('monitor')
        ws_id = workspace.get('id')
        return {
            'mapped': bool(client.get('mapped')),
            'hidden': bool(client.get('hidden')),
            'fullscreen': bool(client.get('fullscreen')),
            'floating': bool(client.get('floating')),
            'monitor': monitor if isinstance(monitor, int) else None,
            'workspace_id': ws_id if isinstance(ws_id, int) else None,
        }
    return {'present': False}


def queue_band(q):
    """Coarse queue high-water band for journal noise control. Counts only."""
    band = 0
    for threshold in QUEUE_BANDS:
        if q >= threshold:
            band = threshold
    return band


def annexb_is_key(data):
    """True if Annex-B bytes include an IDR or CRA NAL. Counts only, no payload log."""
    i = 0
    n = len(data)
    while i + 5 <= n:
        if data[i:i+4] == b'\x00\x00\x00\x01':
            if ((data[i+4] >> 1) & 0x3F) in HEVC_KEY_NAL:
                return True
            i += 5
            continue
        i += 1
    return False


class PlayerStats:
    """Thread-safe ingress/drain counters for a player-backlog snapshot."""

    def __init__(self):
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self._last_empty = self._t0
        self._feeds = 0
        self._writes = 0
        self._max_q = 0
        self._max_write_block_ms = 0.0
        self._last_write_block_ms = 0.0
        self._recent = deque()
        self._q_hist = deque(maxlen=12)

    def _trim(self, now):
        cutoff = now - 1.0
        while self._recent and self._recent[0][0] < cutoff:
            self._recent.popleft()

    def on_feed(self, nbytes, qsize):
        now = time.monotonic()
        with self._lock:
            self._feeds += 1
            self._recent.append((now, 'f', nbytes))
            self._max_q = max(self._max_q, qsize)
            self._q_hist.append(qsize)
            self._trim(now)

    def on_write(self, nbytes, block_ms, qsize):
        now = time.monotonic()
        with self._lock:
            self._writes += 1
            self._recent.append((now, 'w', nbytes))
            self._last_write_block_ms = block_ms
            self._max_write_block_ms = max(self._max_write_block_ms, block_ms)
            if qsize == 0:
                self._last_empty = now
            self._trim(now)

    def snapshot(self, *, au_bytes, au_key, q, mpv_alive, mpv_returncode):
        now = time.monotonic()
        with self._lock:
            self._trim(now)
            feeds_1s = sum(1 for _, kind, _ in self._recent if kind == 'f')
            writes_1s = sum(1 for _, kind, _ in self._recent if kind == 'w')
            feed_bytes_1s = sum(n for _, kind, n in self._recent if kind == 'f')
            write_bytes_1s = sum(n for _, kind, n in self._recent if kind == 'w')
            hist = list(self._q_hist)
            if not hist or hist[-1] != q:
                hist = hist + [q]
            return {
                'q': q,
                'max_q': max(self._max_q, q),
                'q_hist': hist[-12:],
                'fill_ms': int((now - self._last_empty) * 1000),
                'session_ms': int((now - self._t0) * 1000),
                'feeds': self._feeds,
                'writes': self._writes,
                'feeds_1s': feeds_1s,
                'writes_1s': writes_1s,
                'feed_bytes_1s': feed_bytes_1s,
                'write_bytes_1s': write_bytes_1s,
                'max_write_block_ms': int(self._max_write_block_ms),
                'last_write_block_ms': int(self._last_write_block_ms),
                'au_bytes': int(au_bytes),
                'au_key': bool(au_key),
                'mpv_alive': bool(mpv_alive),
                'mpv_returncode': mpv_returncode,
            }

    def live(self, qsize):
        with self._lock:
            return {
                'q': int(qsize),
                'max_q': max(self._max_q, qsize),
                'max_write_block_ms': int(self._max_write_block_ms),
                'feeds': self._feeds,
                'writes': self._writes,
            }


class DirectPlayer:
    def __init__(self, vps=None, sps=None, pps=None, *, ipc_path, on_stop, on_ready,
                 on_frame=None, on_decode_error=None, on_keyframe_request=None):
        self.ipc_path = str(ipc_path)
        self._status_reader = None
        self._status_writer = None
        self.width, self.height = 400, 870
        self._inq = queue.Queue(maxsize=120)
        self._stop = threading.Event()
        self.on_stop = on_stop
        self.on_keyframe_request = on_keyframe_request
        self.stats = PlayerStats()
        self.backlog_snapshot = None
        self.ipc_path = Path(ipc_path)
        self._wait_key = False
        self.skips = 0
        self.skip_cycles = 0
        self._last_pli = 0.0
        self._queue_band = 0
        self.player = subprocess.Popen([
            'mpv', '--no-config', '--profile=low-latency',
            '--force-window=immediate', '--idle=yes', '--keep-open=yes',
            # Vulkan gpu-next (waylandvk) wedged Wayland pings (Hyprland ANR).
            '--gpu-api=opengl', '--gpu-context=wayland',
            '--title=iPhone — Mirror', '--geometry=400x870',
            # Portrait default; InputBridge follows the phone and may swap this.
            '--input-ipc-server='+str(ipc_path), '--osc=no',
            '--cursor-autohide=no', '--input-vo-keyboard=yes',
            '--video-margin-ratio-bottom=0.08',
            '--input-cursor=yes', '--window-dragging=no',
            '--input-builtin-dragging=no', '--input-builtin-bindings=no',
            '--load-scripts=no', '--no-audio', '--untimed', '--cache=no',
            '--demuxer-readahead-secs=0', '--demuxer-lavf-format=hevc',
            '--demuxer-lavf-probesize=32', '--demuxer-lavf-analyzeduration=0',
            # Do not discard the first keyframe during probing.
            '--demuxer-lavf-o=fflags=+flush_packets,framerate=60',
            '--vd-lavc-threads=1', '--interpolation=no',
            '--input-default-bindings=no', '--input-terminal=no',
            '--no-terminal', '--msg-level=all=warn', '-',
        ], stdin=subprocess.PIPE, bufsize=0)
        self._thread = threading.Thread(target=self._write, daemon=True)
        self._thread.start()
        if vps is not None:
            self.configure(vps, sps, pps)
            on_ready(self)

    def configure(self, vps, sps, pps, **kwargs):
        from pymobiledevice3.remote.core_device.hevc_av import remove_emulation_prevention, parse_sps
        state = parse_sps(remove_emulation_prevention(sps[2:]))
        self.width, self.height = state.pic_width_in_luma_samples, state.pic_height_in_luma_samples
        # A retry reuses MPV, but must accept the new stream's parameter sets
        # even if the previous stream ended while waiting for an IDR.
        self._drain_queue()
        self._wait_key = False
        self._last_pli = 0.0
        self._queue_band = 0
        self.feed(b''.join(b'\x00\x00\x00\x01'+n for n in (vps,sps,pps)))
        return self

    async def status(self, text, *, ended=False):
        if self._status_writer is None:
            for _ in range(100):
                if self.player.poll() is not None:
                    return
                try:
                    self._status_reader, self._status_writer = await asyncio.open_unix_connection(self.ipc_path)
                    break
                except (FileNotFoundError, ConnectionRefusedError):
                    await asyncio.sleep(.05)
            else:
                raise RuntimeError('player-ipc-unavailable')
        reader, writer = self._status_reader, self._status_writer
        try:
            commands = []
            if ended:
                # Keep stdin open: the next attempt supplies fresh HEVC parameter
                # sets and a keyframe to this same player and window.
                commands += [['disable-section', 'usb-input'], ['osd-overlay', 61, 'none', '']]
            # A persistent overlay survives MPV's transition to idle mode.
            if text:
                ass = (r'{\an7\pos(0,0)\bord0\1c&H000000&\p1}'
                       r'm 0 0 l 400 0 400 870 0 870{\p0}' + '\n' +
                       r'{\an5\pos(200,435)\fs18\bord1\1c&HFFFFFF&}' + text.replace('\n', r'\N'))
                if ended:
                    ass += ('\n' + r'{\an7\pos(0,0)\bord0\1c&H555555&\p1}'
                            r'm 90 520 l 310 520 310 580 90 580{\p0}' + '\n' +
                            r'{\an5\pos(200,550)\fs18\bord0\1c&HFFFFFF&}Retry Connection')
                commands.append(['osd-overlay', 62, 'ass-events', ass, 400, 870])
            else:
                commands.append(['osd-overlay', 62, 'none', ''])
            for request_id, command in enumerate(commands, 1):
                writer.write((json.dumps({'command': command, 'request_id': request_id})+'\n').encode())
                await writer.drain()
                async with asyncio.timeout(2):
                    while True:
                        line = await reader.readline()
                        if not line:
                            raise RuntimeError('player-ipc-closed')
                        reply = json.loads(line)
                        if reply.get('request_id') == request_id:
                            if reply.get('error') != 'success':
                                raise RuntimeError('player-status-failed')
                            break
        except BaseException:
            writer.close()
            self._status_reader = self._status_writer = None
            raise
        # MPV owns overlays per IPC client. Keep this client connected until
        # the window closes, including while capture and cleanup are running.

    async def wait_retry(self, stop_event):
        reader, writer = await asyncio.open_unix_connection(self.ipc_path)
        try:
            for request_id, command in enumerate((
                ['define-section', 'mirror-retry',
                 'MBTN_LEFT script-binding mirror-retry\nENTER script-binding mirror-retry\nCLOSE_WIN quit', 'force'],
                ['enable-section', 'mirror-retry', 'exclusive'],
            ), 81):
                writer.write((json.dumps({'command': command, 'request_id': request_id})+'\n').encode())
                await writer.drain()
                async with asyncio.timeout(2):
                    while True:
                        line = await reader.readline()
                        if not line:
                            raise RuntimeError('retry-ipc-closed')
                        reply = json.loads(line)
                        if reply.get('request_id') == request_id:
                            if reply.get('error') != 'success':
                                raise RuntimeError('retry-bind-failed')
                            break
            while not stop_event.is_set() and self.player.poll() is None:
                try:
                    line = await asyncio.wait_for(reader.readline(), .1)
                except TimeoutError:
                    continue
                if not line:
                    return False
                event = json.loads(line)
                if event.get('event') == 'client-message':
                    args = event.get('args', [])
                    if (len(args) >= 4 and args[:2] == ['key-binding', 'mirror-retry']
                            and args[2][:1] in ('p', 'd')):
                        if args[3] == 'ENTER':
                            return True
                        # Property notifications can follow the click event.
                        # Query current coordinates, not the last notification.
                        for request_id, name in ((91, 'mouse-pos'), (92, 'osd-dimensions')):
                            writer.write((json.dumps({'command': ['get_property', name],
                                                      'request_id': request_id})+'\n').encode())
                        await writer.drain()
                        values = {}
                        async with asyncio.timeout(2):
                            while len(values) < 2:
                                line = await reader.readline()
                                if not line:
                                    return False
                                reply = json.loads(line)
                                if reply.get('request_id') in (91, 92):
                                    values[reply['request_id']] = reply.get('data') or {}
                        if retry_hit(values[91], values[92]):
                            return True
            return False
        finally:
            with contextlib.suppress(OSError, TimeoutError, ValueError):
                writer.write((json.dumps({'command': ['disable-section', 'mirror-retry'],
                                          'request_id': 99})+'\n').encode())
                await writer.drain()
                async with asyncio.timeout(2):
                    while line := await reader.readline():
                        if json.loads(line).get('request_id') == 99:
                            break
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    def live_stats(self):
        snap = self.stats.live(self._inq.qsize())
        snap['skip_cycles'] = self.skip_cycles
        snap['skips'] = self.skips
        snap['wait_key'] = bool(self._wait_key)
        return snap

    def note_queue_band(self):
        """Log rising queue bands once per climb. Counts only."""
        q = self._inq.qsize()
        if q == 0:
            self._queue_band = 0
            return None
        band = queue_band(q)
        if band <= self._queue_band:
            return None
        self._queue_band = band
        snap = self.live_stats()
        snap['band'] = band
        log.warning('player-queue %s', json.dumps(snap, separators=(',', ':')))
        return snap

    def _drain_queue(self):
        drained = 0
        while True:
            try:
                self._inq.get_nowait()
            except queue.Empty:
                return drained
            drained += 1

    def _request_key(self, force=False):
        now = time.monotonic()
        if not force and now - self._last_pli < 0.5:
            return
        self._last_pli = now
        cb = self.on_keyframe_request
        if cb is None:
            return
        with contextlib.suppress(Exception):
            cb()

    def _begin_skip(self, data, key):
        if self.player.poll() is not None:
            self.on_stop('player-exited')
            return
        if self.backlog_snapshot is None:
            snap = self.stats.snapshot(
                au_bytes=len(data), au_key=key,
                q=self._inq.maxsize, mpv_alive=True,
                mpv_returncode=self.player.returncode)
            mpv = mpv_property_snapshot(self.ipc_path)
            if mpv:
                snap['mpv'] = mpv
            snap['recovery'] = 'skip-idr'
            self.backlog_snapshot = snap
        drained = self._drain_queue()
        if not self._wait_key:
            self._wait_key = True
            self.skip_cycles += 1
            log.warning('player-backlog skip-idr cycle=%d drained=%d au_key=%s',
                        self.skip_cycles, drained, key)
        self.skips += 1
        self._request_key(force=True)

    def feed(self, data):
        if self._stop.is_set():
            return
        key = annexb_is_key(data)
        if self._wait_key and not key:
            self.skips += 1
            self._request_key()
            return
        try:
            self._inq.put_nowait(data)
        except queue.Full:
            self._begin_skip(data, key)
            if not self._wait_key or not key:
                return
            try:
                self._inq.put_nowait(data)
            except queue.Full:
                return
            self._wait_key = False
        else:
            if key:
                self._wait_key = False
        self.stats.on_feed(len(data), self._inq.qsize())

    def _write(self):
        try:
            while not self._stop.is_set():
                try:
                    data = self._inq.get(timeout=.2)
                except queue.Empty:
                    if self.player.poll() is not None:
                        if not self._stop.is_set():
                            self.on_stop(None if self.player.returncode == 0 else 'player-exited')
                        return
                    continue
                remaining = memoryview(data)
                blocked = 0.0
                while remaining and not self._stop.is_set():
                    started = time.monotonic()
                    n = self.player.stdin.write(remaining)
                    blocked += (time.monotonic() - started) * 1000
                    if not n:
                        raise BrokenPipeError()
                    remaining = remaining[n:]
                self.stats.on_write(len(data), blocked, self._inq.qsize())
        except (BrokenPipeError, OSError):
            if not self._stop.is_set():
                self.on_stop(None)

    def close(self):
        if self._status_writer is not None:
            self._status_writer.close()
            self._status_reader = self._status_writer = None
        self._stop.set()
        if self.player.poll() is None:
            self.player.terminate()
            try:
                self.player.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.player.kill()
                self.player.wait(timeout=1)
        with contextlib.suppress(Exception):
            self.player.stdin.close()
        self._thread.join(timeout=1)

def retry_hit(mouse, dimensions):
    w, h = dimensions.get('w', 0), dimensions.get('h', 0)
    if not w or not h or not mouse.get('hover', False):
        return False
    x, y = mouse.get('x', -1) * 400 / w, mouse.get('y', -1) * 870 / h
    return 90 <= x <= 310 and 520 <= y <= 580


class TrackedTransport:
    def __init__(self, transport):
        self.transport = transport
        self.last_packet = time.monotonic()
        self._last_rtp = self.last_packet
        self._lock = threading.Lock()
        self._packets = 0
        self._gaps = 0
        self._last_seq = None
        self._last_return = None
        self._max_interarrival_ms = 0.0
        self._last_interarrival_ms = 0.0
        self._max_loop_delay_ms = 0.0
        self._last_loop_delay_ms = 0.0
        self._recent = deque()

    async def recv(self):
        entered = time.monotonic()
        with self._lock:
            if self._last_return is not None:
                delay_ms = (entered - self._last_return) * 1000
                self._last_loop_delay_ms = delay_ms
                if delay_ms > self._max_loop_delay_ms:
                    self._max_loop_delay_ms = delay_ms
        data = await self.transport.recv()
        now = time.monotonic()
        with self._lock:
            self._last_return = now
            self.last_packet = now
            pt = data[1] & 0x7F if len(data) >= 2 else None
            if pt is None or 64 <= pt <= 95:
                return data
            if self._packets:
                inter = (now - self._last_rtp) * 1000
                self._last_interarrival_ms = inter
                if inter > self._max_interarrival_ms:
                    self._max_interarrival_ms = inter
            self._last_rtp = now
            self._packets += 1
            self._recent.append((now, len(data)))
            cutoff = now - 1.0
            while self._recent and self._recent[0][0] < cutoff:
                self._recent.popleft()
            if len(data) >= 4:
                seq = int.from_bytes(data[2:4], 'big')
                if self._last_seq is not None:
                    expected = (self._last_seq + 1) & 0xFFFF
                    if seq != expected:
                        gap = (seq - expected) & 0xFFFF
                        self._gaps += gap if gap < 0x8000 else 1
                self._last_seq = seq
        return data

    def snapshot(self):
        now = time.monotonic()
        with self._lock:
            cutoff = now - 1.0
            while self._recent and self._recent[0][0] < cutoff:
                self._recent.popleft()
            return {
                'packets': self._packets,
                'gaps': self._gaps,
                'packets_1s': len(self._recent),
                'bytes_1s': sum(n for _, n in self._recent),
                'last_packet_age_ms': int((now - self.last_packet) * 1000),
                'max_interarrival_ms': int(self._max_interarrival_ms),
                'last_interarrival_ms': int(self._last_interarrival_ms),
                'max_loop_delay_ms': int(self._max_loop_delay_ms),
                'last_loop_delay_ms': int(self._last_loop_delay_ms),
            }

    def __getattr__(self, name):
        return getattr(self.transport, name)

class Mirror:
    def __init__(self, runtime, serial=None, connection='auto'):
        self.runtime, self.serial = runtime, serial
        self.connection = connection
        self.stop_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()
        self.player_ready = asyncio.Event()
        self.connection_ready = asyncio.Event()
        self.player = None
        self.bridge = None
        self.error = None
        self.session_id = None
        self.cleaning_up = False
        self.window = None
        self.connected = False
        self.stage = 'window'
        self.loop = asyncio.get_running_loop()
        self._audio = None
        self._audio_available = False
        self._redraw_audio = False
        self._transport = None
        self.backlog = None

    def _audio_output_stopped(self):
        try:
            self.loop.call_soon_threadsafe(self._mark_audio_unavailable)
        except RuntimeError:
            pass

    def _mark_audio_unavailable(self):
        if self.cleaning_up or not self._audio_available:
            return
        self._audio_available = False
        if self._audio is not None:
            self._audio.set_muted(True)
        bridge = self.bridge
        if bridge is not None:
            bridge.audio_available = False
            bridge.audio_muted = True
            self._redraw_audio = True
        self._publish_audio_status(muted=True)

    def _publish_audio_status(self, muted=None):
        state = self.runtime.state.get('state') or 'running'
        if state in ('stopping', 'stopped'):
            return
        if muted is None:
            muted = True if not self._audio_available else (
                self.bridge.audio_muted if self.bridge is not None else True)
        extra = {'audio_muted': bool(muted), 'audio_available': bool(self._audio_available)}
        if self.player is not None and self.player.player.poll() is None:
            extra['player_pid'] = self.player.player.pid
        error = self.bridge.error if self.bridge is not None else self.runtime.state.get('error')
        self.runtime.update(state, error=error, **extra)

    def _on_audio_toggle(self, muted):
        if not self._audio_available:
            muted = True
        if self._audio is not None:
            self._audio.set_muted(muted)
        self._publish_audio_status(muted=muted)

    def _player_status(self):
        player = self.player
        live = getattr(player, 'live_stats', None)
        note = getattr(player, 'note_queue_band', None)
        extra = {}
        if callable(live):
            extra['player'] = live()
        if callable(note):
            note()
        return extra

    def _backlog_snapshot(self):
        snap = {}
        if self.player is not None and self.player.backlog_snapshot:
            snap.update(self.player.backlog_snapshot)
        if self._transport is not None:
            snap['rtp'] = self._transport.snapshot()
        proc = getattr(self.player, 'player', None) if self.player is not None else None
        pid = getattr(proc, 'pid', None)
        hypr = hypr_client_snapshot(pid)
        if hypr:
            snap['hypr'] = hypr
        return snap

    def stop(self, error=None):
        if error and self.error is None and not self.stop_event.is_set() and not self.cleaning_up:
            self.error = error
            if error == 'player-backlog' and self.backlog is None:
                with contextlib.suppress(Exception):
                    self.backlog = self._backlog_snapshot()
                    log.warning('player-backlog %s', json.dumps(self.backlog, separators=(',', ':')))
        if error is None:
            # A user, signal, or closed player requests application shutdown.
            # Keep this separate from a failed connection attempt because the
            # attempt event is cleared before the retry screen becomes active.
            self.loop.call_soon_threadsafe(self.shutdown_event.set)
        self.loop.call_soon_threadsafe(self.stop_event.set)

    def ready(self, player):
        self.player = player
        self.player_ready.set()
        self.runtime.update('starting', player_pid=player.player.pid)

    async def controls(self, reader, writer):
        try:
            line = await asyncio.wait_for(reader.readline(), 2)
            request = json.loads(line)
            command = request.get('command')
            if command == 'stop':
                self.stop()
            elif command == 'focus':
                if self.player is None or self.player.player.poll() is not None:
                    raise RuntimeError('not-ready')
                if not shutil.which('hyprctl'):
                    raise RuntimeError('focus-not-supported')
                pid = int(self.player.player.pid)
                for arguments in ((f'hl.dsp.focus({{ window = "pid:{pid}" }})',),
                                  ('focuswindow',f'pid:{pid}')):
                    proc = await asyncio.create_subprocess_exec(
                        'hyprctl', 'dispatch', *arguments,
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                    if not await asyncio.wait_for(proc.wait(), 2):
                        break
                else:
                    raise RuntimeError('focus-failed')
            elif command == 'reload-ui':
                if self.bridge is None or self.bridge.writer is None:
                    raise RuntimeError('not-ready')
                # Rebuild the toolbar without changing the media connection.
                await self.bridge.draw_toolbar()
            else:
                raise ValueError('unknown-command')
            reply = {'ok': True}
        except Exception:
            reply = {'ok': False, 'error': 'Command could not be completed. Check application status.'}
        try:
            writer.write((json.dumps(reply)+'\n').encode())
            await writer.drain()
        finally:
            writer.close()

    async def capture(self, selected=None):
        from connection import select_connection, get_tunnel
        from pymobiledevice3.remote.core_device.display_service import DisplayService
        from pymobiledevice3.remote.core_device.screen_stream import open_media_receiver
        from pymobiledevice3.remote.core_device.vnc_server import VncStreamServer

        # Reuse only the pinned RTP/HEVC receiver methods, not upstream serve().
        # Upstream serve() closes media transport early and cancels all loop tasks.
        # Our orchestration owns and cancels only the tasks it creates.
        self.stage = 'device-discovery'
        mode, serial = selected if selected is not None else await select_connection(self.connection,self.serial)
        self.runtime.update('starting', connection=mode, requested_connection=self.connection, serial=serial)
        self.stage = 'tunnel'
        async with get_tunnel(mode,serial) as rsd:
            service = None
            transport = None
            receiver = None
            tasks = []
            input_task = None
            audio = None
            try:
                self.stage = 'display-service'
                service = await connect_service(lambda: DisplayService(rsd))
                raw, receiver_ip = open_media_receiver(service, (8*1024*1024,4*1024*1024))
                transport = TrackedTransport(raw)
                self._transport = transport
                self.session_id = uuid.uuid4()
                self.stage = 'video-request'
                answer = await asyncio.wait_for(service.start_video_stream(
                    receiver_ip=receiver_ip, receiver_port=transport.port,
                    sender_ip=rsd.service.address[0], display_id=1,
                    client_session_id=self.session_id, allow_rtcp_fb=False,
                    ltrp_enabled=False), 12)
                sid = answer['connection']['options']['avcMediaStreamOptionClientSessionID']['uuid']
                self.session_id = sid if isinstance(sid, uuid.UUID) else uuid.UUID(sid)
                receiver = VncStreamServer(rsd, bind='127.0.0.1', audio=False, decoder='av')
                def request_keyframe():
                    with contextlib.suppress(Exception):
                        self.loop.create_task(receiver._send_rtcp_pli())
                # PLI only. The library refresh path closes DirectPlayer on the
                # next IDR, which kills mpv and looks like a clean stop.
                receiver._fire_decoder_refresh = lambda now, reason=None, **kw: request_keyframe()
                def make_player(*args, **kwargs):
                    if self.window is not None:
                        self.window.on_keyframe_request = request_keyframe
                        player = self.window.configure(*args, **kwargs)
                        self.ready(player)
                        return player
                    return DirectPlayer(*args, **kwargs, ipc_path=self.runtime.root/'mpv.sock',
                                        on_stop=self.stop, on_ready=self.ready,
                                        on_keyframe_request=request_keyframe)
                receiver._transcoder_cls = make_player
                receiver._loop = self.loop
                cfg = answer['connection'].get('streamConfig', {})
                receiver._local_ssrc = int(cfg.get('RemoteSSRC', 0))
                receiver._remote_ssrc = int(cfg.get('LocalSSRC', 0))
                source_port = int(cfg.get('SourcePort', 0))
                receiver._rtcp_dest = (rsd.service.address[0], source_port) if source_port else None
                receiver._active_transport = transport
                tasks = [asyncio.create_task(receiver._udp_recv_and_pipe(transport)),
                         asyncio.create_task(receiver._rtcp_send_loop(transport))]
                self.stage = 'first-video-packet'
                # The video receiver is already running, so this overlaps the
                # first packet. The toolbar waits until the speaker state is known.
                audio = await start_system_audio(rsd, self.session_id)
                self._audio = audio
                self._audio_available = audio is not None and audio.output_alive()
                if audio is not None:
                    audio.on_stopped = self._audio_output_stopped
                await asyncio.wait_for(self.player_ready.wait(), 15)
                self.stage = 'input-setup'
                self.bridge = InputBridge(rsd, str(self.runtime.root/'mpv.sock'),
                                          player_pid=self.player.player.pid)
                self.bridge.on_audio_toggle = self._on_audio_toggle
                self.bridge.audio_available = self._audio_available
                self.bridge.audio_muted = True
                input_task = asyncio.create_task(self.bridge.run())
                await asyncio.wait_for(self.bridge.ready.wait(), 12)
                self.connected = True
                self.connection_ready.set()
                if self.window is not None:
                    await self.window.status('')
                self.stage = 'streaming'
                self.runtime.update('running', player_pid=self.player.player.pid,
                                    audio_muted=True, audio_available=self._audio_available,
                                    **self._player_status())
                while not self.stop_event.is_set():
                    if self._audio_available and self._audio is not None and not self._audio.output_alive():
                        self._mark_audio_unavailable()
                    if self._redraw_audio and getattr(self.bridge, 'writer', None) is not None:
                        self._redraw_audio = False
                        with contextlib.suppress(Exception):
                            await self.bridge.draw_toolbar()
                    player_status = self._player_status()
                    if (self.runtime.state.get('error') != self.bridge.error
                            or player_status.get('player') != self.runtime.state.get('player')):
                        self.runtime.update('running', error=self.bridge.error,
                                            player_pid=self.player.player.pid,
                                            audio_muted=True if not self.bridge.audio_available else self.bridge.audio_muted,
                                            audio_available=bool(self.bridge.audio_available),
                                            **player_status)
                    if input_task.done():
                        # Error type only: never exception messages, locals or keys.
                        if not input_task.cancelled() and input_task.exception():
                            log.error('Input service failed (%s)', type(input_task.exception()).__name__)
                            self.stop('input-service-failed')
                        else:
                            self.stop('input-service-ended' if self.player.player.poll() is None else None)
                        break
                    if tasks[0].done():
                        self.stop('usb-stream-ended')
                        break
                    if time.monotonic()-transport.last_packet > 15:
                        self.stop('usb-stream-timeout')
                        break
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.stop_event.wait(), .25)
            finally:
                self.cleaning_up = True
                self.runtime.update('stopping')
                errors = await close_session(
                    bridge=self.bridge, input_task=input_task,
                    service=service, session_id=self.session_id,
                    stream_tasks=tasks, player=None if self.window else self.player, transport=transport,
                    pli_tasks=receiver._pli_tasks if receiver else (),
                    audio=audio)
                if errors and self.error is None:
                    self.error = ', '.join(errors)
                # The tunnel remains alive until ALL cleanup above has finished.

    async def start_capture(self):
        if self.window is None:
            self.window = DirectPlayer(ipc_path=self.runtime.root/'mpv.sock',
                                       on_stop=self.stop, on_ready=self.ready)
        self.player = self.window
        self.runtime.update('starting', player_pid=self.window.player.pid)
        await self.window.status('Connecting to iPhone...')
        from connection import select_connection
        self.stage = 'device-discovery'
        mode, serial = await asyncio.wait_for(select_connection(self.connection, self.serial), CONNECT_TIMEOUT)
        if mode == 'usb':
            from image_preparation import ensure_usb_image
            self.stage = 'image-check'
            async def show_preparing():
                self.stage = 'image-mount'
                await self.window.status('Preparing iPhone...')
            preparation = asyncio.create_task(ensure_usb_image(serial, show_preparing))
            try:
                done, _ = await asyncio.wait((preparation,), timeout=IMAGE_PREP_TIMEOUT)
                if not done:
                    raise TimeoutError('image-preparation-timeout')
                await preparation
            finally:
                if not preparation.done():
                    preparation.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await preparation  # Release USB resources before stopping or Retry.
            await self.window.status('Connecting to iPhone...')
        capture = asyncio.create_task(self.capture((mode, serial)))
        ready = asyncio.create_task(self.connection_ready.wait())
        try:
            done, _ = await asyncio.wait(
                (capture, ready), timeout=CONNECT_TIMEOUT,
                return_when=asyncio.FIRST_COMPLETED)
            if not done:
                # A failed capture may already be cleaning up at this deadline.
                # Do not cancel that cleanup a second time.
                if self.cleaning_up:
                    await capture
                else:
                    capture.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await capture
                raise TimeoutError()
            if capture in done:
                await capture
            else:
                # Startup succeeded. Keep this attempt active until capture
                # ends because of a disconnect or shutdown request.
                await capture
        finally:
            ready.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ready
            if not capture.done():
                if not self.cleaning_up:
                    capture.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await capture

    async def run(self):
        server = await asyncio.start_unix_server(self.controls, path=str(self.runtime.root/'control.sock'), limit=4096)
        for sig in (signal.SIGTERM, signal.SIGINT):
            self.loop.add_signal_handler(sig, self.stop)
        try:
            while True:
                await self.run_attempt()
                if (self.shutdown_event.is_set() or not self.error or self.window is None
                        or self.window.player.poll() is not None):
                    break
                image_failure = self.stage in ('image-check', 'image-mount')
                message = (self.error if image_failure else
                           ('Disconnected from iPhone.' if self.connected else 'Cannot connect to iPhone.')
                           + '\nCheck the connection.\nUnlock your iPhone.')
                self.runtime.update('disconnected' if self.connected else 'error', error=self.error,
                                    audio_muted=True, audio_available=False)
                try:
                    self.stop_event.clear()
                    await self.window.status(message, ended=True)
                    if (self.shutdown_event.is_set()
                            or not await self.window.wait_retry(self.shutdown_event)
                            or self.shutdown_event.is_set()):
                        break
                except (OSError, RuntimeError, TimeoutError, ValueError) as error:
                    self.error = self.error or 'Retry controls are unavailable.'
                    log.error('Retry UI failed (%s)', type(error).__name__)
                    break
                # The previous tunnel and input session are fully closed before
                # resetting attempt state. The MPV window and pipe stay open.
                self.error = None
                self.connected = False
                self.cleaning_up = False
                self.bridge = None
                self.session_id = None
                self._audio = None
                self._audio_available = False
                self._redraw_audio = False
                self.player_ready.clear()
                self.connection_ready.clear()
                self.stop_event.clear()
        finally:
            if self.window is not None:
                self.window.close()
            server.close()
            await server.wait_closed()
            for sig in (signal.SIGTERM, signal.SIGINT):
                self.loop.remove_signal_handler(sig)
            final = 'disconnected' if self.error and self.connected else ('error' if self.error else 'stopped')
            self.runtime.update(final, error=self.error, player_pid=None,
                                audio_muted=True, audio_available=False,
                                **({'backlog': self.backlog} if self.backlog else {}))

    async def run_attempt(self):
        capture = asyncio.create_task(self.start_capture())
        stopping = asyncio.create_task(self.stop_event.wait())
        try:
            done, _ = await asyncio.wait((capture, stopping), return_when=asyncio.FIRST_COMPLETED)
            if capture in done:
                await capture
        except Exception as error:
            if self.stage in ('image-check', 'image-mount'):
                from image_preparation import ImagePreparationError
                if isinstance(error, ImagePreparationError):
                    message = IMAGE_FAILURE_MESSAGES.get(str(error), 'Developer image could not be prepared. Check the phone before retrying.')
                else:
                    message = 'Developer image could not be prepared. Unlock the phone and check USB before retrying.'
            else:
                message = 'Connection failed ('+type(error).__name__+'). Check the connection, pairing, Developer Mode and developer image.'
            self.error = self.error or message
            log.error('Capture failed during %s (%s)', self.stage, type(error).__name__)
        finally:
            # Do not interrupt cleanup once it has started.
            if not capture.done() and not self.cleaning_up:
                capture.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await capture
            stopping.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stopping

async def async_main(runtime, serial, connection='auto'):
    app = Mirror(runtime, serial, connection)
    await app.run()
    return 1 if app.error else 0

def main():
    parser = argparse.ArgumentParser(description='On-demand iPhone mirror')
    parser.add_argument('--serial', help='Select a paired iPhone')
    parser.add_argument('--connection',choices=('usb','wifi','auto'),default='auto')
    parser.add_argument('--from-launch-request',action='store_true',help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.from_launch_request:
        path = Path(os.environ['XDG_RUNTIME_DIR'])/'iphone-mirror/launch.json'
        try:
            request = json.loads(path.read_text())
        except FileNotFoundError:
            request = {}
        mode = request.get('connection','auto')
        if mode not in ('usb','wifi','auto'):
            parser.error('Invalid saved connection mode')
        args.connection = mode
        args.serial = request.get('serial')
        if args.serial is not None and not isinstance(args.serial,str):
            parser.error('Invalid saved device identifier')
    if shutil.which('systemctl'):
        legacy = subprocess.run(['systemctl', '--user', 'is-active', '--quiet', 'iphone-usb-mirror.service'],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
        if legacy.returncode == 0:
            print('Close the experimental iphone-usb-mirror viewer before starting this application.', file=sys.stderr)
            return 1
    os.umask(0o077)
    logging.basicConfig(level=logging.WARNING, format='%(name)s: %(message)s')
    runtime = Runtime()
    try:
        runtime.acquire()
    except AlreadyRunning:
        print('The mirror is already running. Use iphone-mirror start to focus it.')
        return 0
    try:
        return asyncio.run(async_main(runtime, args.serial, args.connection))
    finally:
        runtime.close()

if __name__ == '__main__':
    raise SystemExit(main())
