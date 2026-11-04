// The recorder — turns the call's per-person Opus packets into ONE mixed audio file,
// plus a timeline of who spoke when (what an AI transcriber needs to tell voices apart).
// -----------------------------------------------------------------------------
// Discord sends each speaker's voice as its own stream of 20 ms Opus packets, and
// sends NOTHING while they are silent. So every packet is placed on one wall-clock
// timeline: the first packet of a burst lands at "now", the rest follow it back to
// back (arrival jitter must not stretch or squash a sentence), and a gap of more
// than RESYNC_S re-anchors it to "now" (they paused, or packets were lost).
//
// The mix is streamed: frames older than LAG_MS are summed, written to ffmpeg's
// stdin as raw PCM and forgotten — memory stays at a couple of seconds of audio
// however long the meeting runs, and the MP3 is ready the moment it stops.
// ffmpeg cuts it into parts of SEG_S seconds so each one fits a Discord upload.
// -----------------------------------------------------------------------------
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const OpusScript = require('opusscript');

const RATE = 24000;                 // Opus decodes straight to 24 kHz mono — plenty for speech
const FRAME = RATE / 50;            // 20 ms
const LAG_MS = 1500;                // how late a packet may arrive and still be mixed in place
const FLUSH_MS = 100;
const RESYNC_S = Math.round(0.25 * RATE);
const AHEAD_MAX_S = 5 * RATE;       // a burst after a network stall may run ahead of now — not further
const SEG_S = 2100;                 // 35 min × 32 kbps ≈ 8.4 MB — under the 10 MB DM upload limit
const BITRATE = '32k';
const SEG_MERGE_S = 1.2 * RATE;     // one speaker's bursts closer than this read as one turn
const SILENCE = Buffer.from([0xf8, 0xff, 0xfe]);   // Discord's "I stopped" frame

function ffmpegPath() {
    if (process.env.FFMPEG_PATH) return process.env.FFMPEG_PATH;
    // ffmpeg-static downloads its binary at install; if that failed, try the system one.
    try { const p = require('ffmpeg-static'); if (p && fs.existsSync(p)) return p; } catch (_) {}
    return 'ffmpeg';
}

class Recorder {
    constructor(dir) {
        this.dir = dir;
        this.t0 = 0;
        this.cursor = 0;              // frames already sent to ffmpeg
        this.slots = new Map();       // frame index → Int32Array(FRAME), not yet sent
        this.tracks = new Map();      // userId → { dec, s, a, b }
        this.segments = [];           // { id, a, b } in samples
        this.proc = null;
        this.failed = null;
        this.timer = null;
        this.zero = Buffer.alloc(FRAME * 2);
        this._closed = null;
    }

    start(onFail) {
        fs.mkdirSync(this.dir, { recursive: true });
        const args = [
            '-hide_banner', '-loglevel', 'error',
            '-f', 's16le', '-ar', String(RATE), '-ac', '1', '-i', 'pipe:0',
            '-c:a', 'libmp3lame', '-b:a', BITRATE,
            '-f', 'segment', '-segment_time', String(SEG_S), '-reset_timestamps', '1',
            path.join(this.dir, 'part_%02d.mp3'),
        ];
        const proc = spawn(ffmpegPath(), args, { stdio: ['pipe', 'ignore', 'pipe'] });
        this.proc = proc;
        let errText = '';
        proc.stderr.on('data', d => { if (errText.length < 2000) errText += d; });
        const fail = err => {
            if (this.failed) return;
            this.failed = err || new Error('ffmpeg failed');
            clearInterval(this.timer);
            if (onFail) onFail(this.failed);
        };
        proc.on('error', fail);
        proc.stdin.on('error', () => {});   // EPIPE after a crash — 'close' reports it
        this._closed = new Promise(res => proc.on('close', code => {
            if (code !== 0 && !this.stopping) fail(new Error(`ffmpeg exited ${code}: ${errText.trim()}`));
            res(code);
        }));
        this.t0 = performance.now();
        this.timer = setInterval(() => this._flush(false), FLUSH_MS);
    }

    _nowS() { return Math.floor((performance.now() - this.t0) * RATE / 1000); }

    // One Opus packet from one speaker.
    packet(id, opus) {
        if (this.failed || this.stopping) return;
        let t = this.tracks.get(id);
        if (!t) {
            t = { dec: new OpusScript(RATE, 1, OpusScript.Application.VOIP), s: null, a: 0, b: 0 };
            this.tracks.set(id, t);
        }
        const now = this._nowS();
        const floor = this.cursor * FRAME;
        if (t.s === null || now - t.s > RESYNC_S) {
            if (t.s !== null && t.b > t.a) this.segments.push({ id, a: t.a, b: t.b });
            t.s = Math.max(now, floor);
            t.a = t.b = t.s;
        }
        if (t.s < floor) t.s = floor;                    // later than LAG_MS: nudge it forward
        if (t.s - now > AHEAD_MAX_S) return;
        let pcm;
        try { pcm = t.dec.decode(opus); } catch (_) { return; }
        if (pcm.byteOffset & 1) pcm = Buffer.from(pcm);
        const smp = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.length >> 1);
        let s = t.s, i = 0;
        while (i < smp.length) {
            const f = Math.floor(s / FRAME), off = s - f * FRAME;
            let slot = this.slots.get(f);
            if (!slot) { slot = new Int32Array(FRAME); this.slots.set(f, slot); }
            const n = Math.min(FRAME - off, smp.length - i);
            for (let k = 0; k < n; k++) slot[off + k] += smp[i + k];
            i += n; s += n;
        }
        t.s = s;
        if (!opus.equals(SILENCE)) t.b = s;              // the trailing silence frames are not speech
    }

    _flush(all) {
        if (!this.proc || this.failed) return;
        let target = Math.floor((this._nowS() - LAG_MS * RATE / 1000) / FRAME);
        if (all) for (const f of this.slots.keys()) if (f + 1 > target) target = f + 1;
        if (target <= this.cursor) return;
        const out = Buffer.allocUnsafe((target - this.cursor) * FRAME * 2);
        let o = 0;
        for (let f = this.cursor; f < target; f++) {
            const slot = this.slots.get(f);
            if (!slot) { this.zero.copy(out, o); o += FRAME * 2; continue; }
            this.slots.delete(f);
            for (let k = 0; k < FRAME; k++) {
                const v = slot[k];
                out.writeInt16LE(v > 32767 ? 32767 : v < -32768 ? -32768 : v, o);
                o += 2;
            }
        }
        this.cursor = target;
        try { this.proc.stdin.write(out); } catch (_) {}
    }

    // Ends the file. Resolves with the parts, the length and who spoke when.
    async stop() {
        if (this.stopping) return this.result;
        this.stopping = true;
        clearInterval(this.timer);
        this._flush(true);
        for (const [id, t] of this.tracks) {
            if (t.s !== null && t.b > t.a) this.segments.push({ id, a: t.a, b: t.b });
            try { t.dec.delete(); } catch (_) {}
        }
        this.tracks.clear();
        this.slots.clear();
        if (this.proc) {
            try { this.proc.stdin.end(); } catch (_) {}
            const killer = setTimeout(() => { try { this.proc.kill('SIGKILL'); } catch (_) {} }, 120000);
            await this._closed;
            clearTimeout(killer);
        }
        let files = [];
        try {
            files = fs.readdirSync(this.dir).filter(f => /^part_\d+\.mp3$/.test(f)).sort()
                .map(f => path.join(this.dir, f)).filter(f => fs.statSync(f).size > 0);
        } catch (_) {}
        this.result = {
            files,
            seconds: this.cursor * FRAME / RATE,
            segSeconds: SEG_S,
            turns: mergeTurns(this.segments),
            error: this.failed,
        };
        return this.result;
    }
}

// Bursts → turns: sorted, a speaker's bursts closer than SEG_MERGE_S joined, in seconds.
function mergeTurns(segs) {
    const byId = new Map();
    for (const g of segs) {
        if (!byId.has(g.id)) byId.set(g.id, []);
        byId.get(g.id).push(g);
    }
    const out = [];
    for (const [id, list] of byId) {
        list.sort((x, y) => x.a - y.a);
        let cur = null;
        for (const g of list) {
            if (cur && g.a - cur.b <= SEG_MERGE_S) { cur.b = Math.max(cur.b, g.b); continue; }
            if (cur) out.push(cur);
            cur = { id, a: g.a, b: g.b };
        }
        if (cur) out.push(cur);
    }
    out.sort((x, y) => x.a - y.a || x.b - y.b);
    // Shorter than a quarter second is a cough or a click, not a turn.
    return out.filter(g => g.b - g.a >= RATE / 4).map(g => ({ id: g.id, a: g.a / RATE, b: g.b / RATE }));
}

module.exports = { Recorder, SEG_S };
