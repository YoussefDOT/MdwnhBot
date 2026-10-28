// MdwnhBot — tells مقر المدونة who is talking in the Discord call.
// -----------------------------------------------------------------------------
// The bot sits (muted) in the voice channel people are meeting in, listens for
// Discord's "speaking" events, and relays them into the site's WebSocket relay
// (the same Cloudflare rooms the players' live positions travel through). The
// site draws the green outline around whoever is talking at the meeting table
// and fades out whoever is in the call but quiet.
//
// WHICH call it sits in — and it sits in none unless it's wanted:
//   1. Summoned: @mention it (or its role) anywhere in the server and it comes to
//      YOUR current call, whatever the site is doing. It stays while you're in it.
//   2. Following the meeting: members seated at the site's meeting table announce
//      it over the relay ({t:'meet',uid,on:1} every 20 s, on:0 on standing). The bot
//      joins the call most of them are in, and never hops mid-meeting.
//   0. The general meeting (القاعة): while the site's hall is live — its attendees
//      say {t:'hi'} in the relay's 'stage' room — and someone is in the Discord
//      STAGE channel, it sits there, above everything else, and also tells the site
//      who is a speaker: {t:'stage', ids:[in the channel], sp:[speakers]}.
//   3. Otherwise it leaves — LEAVE_GRACE_MS after the table empties, straight away if
//      the call does. Disconnected by hand (right-click → Disconnect) = it stays out
//      of that call for KICK_COOLDOWN_MS (a new mention overrides that).
//
// ZERO Firebase: speaking flips several times a second, which is exactly the kind
// of state that must never be written to the database. The old version of this
// bot mirrored voice-channel members into `users/{uid}` every 10 s — the site
// stopped reading that long ago (its own login writes the avatar/name), so it's
// gone: it cost download bandwidth for every client and nulled players' saved
// positions whenever they left the call.
//
// Messages it sends (the relay forwards raw bytes, so nothing server-side changed):
//   {t:'spk', uid:'mdwnhbot', s:'<discordId>', on:1|0}   the moment it changes
//   {t:'vc',  uid:'mdwnhbot', ids:[in the call], sp:[talking]}   every 15 s
//   {t:'meetq', uid:'mdwnhbot'}   on joining a room: "whoever is seated, say so now"
// A live speaker is re-asserted every 4 s so a lost 'off' can't stick for long.
//
// v5-mic — منصة الميكروفون (the site's night field, الساحة): a member standing on the
// stage's mic podium says {t:'mic', uid:<discord id>, on:1, rv:0|1} in the 'stage'
// room every 4 s. If they are in a Discord call (any voice channel, or the Stage
// channel), the bot goes there — above every other reason to be anywhere — and, ONLY
// while rv is 1, listens to that one member and plays back a reverb of their voice
// (wet only: the dry voice still reaches everyone straight from Discord). rv 0 = it
// stays, silent, and hears nothing. It answers in the 'stage' room:
//   {t:'rv', uid:'mdwnhbot', d:<their id>, st:'wait'|'live'|'nocall'|'nospeak'|'off', on:0|1}
// -----------------------------------------------------------------------------
require('dotenv').config();
const http = require('http');
const WebSocket = require('ws');
const { Client, GatewayIntentBits, ChannelType, Events } = require('discord.js');
const {
    joinVoiceChannel, getVoiceConnection, VoiceConnectionStatus, entersState,
    createAudioPlayer, createAudioResource, StreamType, EndBehaviorType, NoSubscriberBehavior,
} = require('@discordjs/voice');
const { Readable } = require('stream');

const VERSION = 'v5-mic';

// ── Config ───────────────────────────────────────────────────────────────────
const RELAY_BASE = process.env.RELAY_URL || 'wss://mdwnh-presence.yosefbore3y.workers.dev';
const RELAY_UID = 'mdwnhbot';
// Discord category → site lobby. A channel under neither talks to both lobbies.
const LOBBY_CATEGORIES = { male: 'صالة السادة', female: 'صالة السيدات' };
// Optional: comma-separated voice channel ids the bot may FOLLOW a meeting into.
// Empty = any. A mention ignores this — it goes wherever it was asked to.
const MEETING_CHANNEL_IDS = (process.env.MEETING_CHANNEL_IDS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
// القاعة — the general meeting: the Discord Stage channel it is held in, and the relay
// room both lobbies meet in.
const STAGE_CHANNEL_ID = process.env.STAGE_CHANNEL_ID || '1489339643215483092';
const STAGE_ROOM = 'stage';
const STAGE_LIVE_MS = 150000;       // the hall's "I am here" is every 20 s — ~1/min from a background tab
const HEARTBEAT_MS = 15000;
const SPEAK_REASSERT_MS = 4000;
const SPEAK_OFF_GRACE_MS = 250;     // Discord ends a "speaking" burst on every short pause
const RESCAN_DEBOUNCE_MS = 2000;
// The site re-announces a seat every 20 s — but a background tab's timers get throttled
// to ~1/min by Chrome, and people at the table flip to Discord all the time. A closed
// tab doesn't wait this out: the relay's {t:'bye'} for that member drops it at once.
const SEAT_STALE_MS = 150000;
const LEAVE_GRACE_MS = 60000;       // table empty → wait this long before leaving the call
const KICK_COOLDOWN_MS = 10 * 60 * 1000;
const DISCORD_ID_RE = /^\d{15,22}$/;

const state = {
    guild: null,
    channelId: null,
    channelName: null,
    lobbies: [],
    speaking: new Map(),            // discordId → { on, offTimer }
    seated: new Map(),              // discordId → last seat announcement (ms) at the site's table
    pin: null,                      // { channelId, by } — summoned by a mention
    kicked: null,                   // { channelId, until } — disconnected by hand
    leaveAt: 0,                     // when an empty table's grace period runs out
    stageLiveAt: 0,                 // last time the site's hall was heard from
    mic: null,                      // { id, at, rv } — someone on the field's mic podium
    selfMute: true,                 // how the current connection was joined
};
const MIC_STALE_MS = 13000;         // the podium is re-announced every 4 s

// ── Health check (Render needs a bound port) + keep-alive ─────────────────────
http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(`MdwnhBot ${VERSION} alive — voice: ${state.channelName || 'idle'}`);
}).listen(process.env.PORT || 3000);

const RENDER_URL = process.env.RENDER_EXTERNAL_URL;
if (RENDER_URL) {
    setInterval(() => { fetch(RENDER_URL).catch(() => {}); }, 10 * 60 * 1000);
}

// ── Relay sockets, one per lobby room, open while anyone is in a call at all ───
// (it has to hear the table's seat announcements BEFORE it's in a call).
const relays = {};   // lobby → { ws, retry, want }

function relayOpen(lobby) {
    const r = relays[lobby] || (relays[lobby] = { ws: null, retry: null, want: true });
    r.want = true;
    if (r.ws && (r.ws.readyState === WebSocket.OPEN || r.ws.readyState === WebSocket.CONNECTING)) return;
    const url = `${RELAY_BASE}/lobby/${encodeURIComponent(lobby)}?uid=${RELAY_UID}`;
    let ws;
    try { ws = new WebSocket(url); } catch (err) { scheduleRelayRetry(lobby); return; }
    r.ws = ws;
    ws.on('open', () => {
        console.log(`🔌 relay open: ${lobby}`);
        sendHeartbeat();
        // Anyone already at the table re-announces now rather than in up to 20 s.
        try { ws.send(JSON.stringify({ t: 'meetq', uid: RELAY_UID })); } catch (_) {}
    });
    ws.on('message', lobby === STAGE_ROOM ? onStageRelayMessage : onRelayMessage);
    ws.on('close', () => { if (r.want) scheduleRelayRetry(lobby); });
    ws.on('error', () => { try { ws.close(); } catch (_) {} });
}
function scheduleRelayRetry(lobby) {
    const r = relays[lobby];
    if (!r || r.retry || !r.want) return;
    r.retry = setTimeout(() => { r.retry = null; if (r.want) relayOpen(lobby); }, 3000);
}
function relayClose(lobby) {
    const r = relays[lobby];
    if (!r) return;
    r.want = false;
    if (r.retry) { clearTimeout(r.retry); r.retry = null; }
    if (r.ws) { try { r.ws.close(); } catch (_) {} r.ws = null; }
}
function relaySend(obj) {
    const data = JSON.stringify(obj);
    for (const lobby of state.lobbies) {
        const ws = relays[lobby] && relays[lobby].ws;
        if (ws && ws.readyState === WebSocket.OPEN) { try { ws.send(data); } catch (_) {} }
    }
}

// Every player's position flows through the room too — only the seat announcements
// matter here, so everything else is dropped before it's parsed. The uid is
// client-claimed like everything on the relay; the worst a spoof can do is make the
// bot sit in a call someone is already in.
function onRelayMessage(data) {
    const s = data.toString();
    if (s.indexOf('"meet"') === -1 && s.indexOf('"bye"') === -1) return;
    let m;
    try { m = JSON.parse(s); } catch (_) { return; }
    if (!m || typeof m.uid !== 'string' || !DISCORD_ID_RE.test(m.uid)) return;
    const had = state.seated.has(m.uid);
    // Their socket closed (tab shut, network gone). A mere blip re-announces within
    // seconds of reconnecting, and the grace period covers the gap.
    if (m.t === 'bye') {
        if (had) { state.seated.delete(m.uid); scheduleRescan(); }
        return;
    }
    if (m.t !== 'meet') return;
    if (m.on === 1) {
        state.seated.set(m.uid, Date.now());
        if (!had) scheduleRescan();
    } else if (had) {
        state.seated.delete(m.uid);
        scheduleRescan();
    }
}

// The hall's own room: anything from an attendee means the meeting is live. Only
// "is it live" matters here, so nothing is parsed beyond the type.
function stageLive() { return Date.now() - state.stageLiveAt < STAGE_LIVE_MS; }
function onStageRelayMessage(data) {
    const s = data.toString();
    if (s.indexOf('"t":"mic"') !== -1) { onMicMessage(s); return; }
    if (s.indexOf('"t":"bye"') !== -1 && state.mic && s.indexOf(state.mic.id) !== -1) { micOff(); return; }
    if (s.indexOf('"t":"hi"') === -1) return;
    const was = stageLive();
    state.stageLiveAt = Date.now();
    if (!was) { console.log('🎤 the hall is live'); scheduleRescan(0); }
}

// ── منصة الميكروفون: the podium on the field's stage ──────────────────────────
function micLive() { return !!(state.mic && Date.now() - state.mic.at < MIC_STALE_MS); }
function micChannel() {
    if (!micLive() || !state.guild) return null;
    const vs = state.guild.voiceStates.cache.get(state.mic.id);
    const ch = vs && vs.channelId ? state.guild.channels.cache.get(vs.channelId) : null;
    return ch && ch.isVoiceBased() && ch.id !== ch.guild.afkChannelId ? ch : null;
}
function onMicMessage(s) {
    let m;
    try { m = JSON.parse(s); } catch (_) { return; }
    if (!m || m.t !== 'mic') return;
    // The site's own login id; `cu` is the member's primary account (an alt logs in
    // with another). Whichever of the two is in a voice channel is the one.
    const ids = [m.uid, m.cu].filter(x => typeof x === 'string' && DISCORD_ID_RE.test(x));
    if (!ids.length) return;
    if (m.on !== 1) {
        if (state.mic && ids.includes(state.mic.id)) micOff();
        return;
    }
    // One podium: whoever is already on it keeps it until they step off (or go quiet).
    if (micLive() && !ids.includes(state.mic.id)) return;
    let id = ids[0];
    if (state.guild) for (const x of ids) { const vs = state.guild.voiceStates.cache.get(x); if (vs && vs.channelId) { id = x; break; } }
    const fresh = !micLive();
    const rv = m.rv === 1;
    const was = state.mic && state.mic.rv;
    state.mic = { id, at: Date.now(), rv };
    if (!micChannel()) { micStatus('nocall'); reverbStop(); return; }
    if (fresh || !state.channelId || state.channelId !== micChannel().id) { micStatus('wait'); scheduleRescan(0); return; }
    if (rv !== was) applyReverb();
    micStatus(state.selfMute ? 'wait' : (stageBlocked() ? 'nospeak' : 'live'));
}
function micOff() {
    if (!state.mic) return;
    const id = state.mic.id;
    state.mic = null;
    reverbStop();
    micStatus('off', id);
    scheduleRescan(0);
}
// The bot itself is suppressed in a Stage channel unless it is a speaker.
function stageBlocked() {
    const me = state.guild && state.guild.members.me;
    return !!(me && me.voice && me.voice.channelId === STAGE_CHANNEL_ID && me.voice.suppress);
}
function micStatus(st, id) {
    const ws = relays[STAGE_ROOM] && relays[STAGE_ROOM].ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const d = id || (state.mic && state.mic.id) || '';
    const on = st === 'live' && state.mic && state.mic.rv ? 1 : 0;
    try { ws.send(JSON.stringify({ t: 'rv', uid: RELAY_UID, d, st, on })); } catch (_) {}
}
setInterval(() => {
    if (!state.mic) return;
    if (!micLive()) { micOff(); return; }
    micStatus(!micChannel() ? 'nocall' : (state.channelId !== micChannel().id || state.selfMute) ? 'wait' : (stageBlocked() ? 'nospeak' : 'live'));
}, 4000);

// ── The reverb: Freeverb (Jezar's) — 8 combs, 4 all-passes per side ─────────
// Wet only, mono in, stereo out. 48 kHz, 20 ms frames of 960 samples.
class Comb {
    constructor(n) { this.b = new Float32Array(n); this.i = 0; this.s = 0; }
    run(x, fb, damp) {
        const y = this.b[this.i];
        this.s = y * (1 - damp) + this.s * damp;
        this.b[this.i] = x + this.s * fb;
        if (++this.i >= this.b.length) this.i = 0;
        return y;
    }
}
class AllPass {
    constructor(n) { this.b = new Float32Array(n); this.i = 0; }
    run(x) {
        const bo = this.b[this.i];
        this.b[this.i] = x + bo * 0.5;
        if (++this.i >= this.b.length) this.i = 0;
        return bo - x;
    }
}
const FV_K = 48000 / 44100;
const FV_COMBS = [1116, 1188, 1277, 1356, 1422, 1491, 1557, 1617];
const FV_APS = [556, 441, 341, 225];
const FV_SPREAD = 23;
const RV_ROOM = 0.88, RV_DAMP = 0.28, RV_WET = 2.6, RV_PREDELAY = Math.round(0.008 * 48000);   // short: the trip through Discord already delays it
class Freeverb {
    constructor() {
        const n = (x) => Math.round(x * FV_K);
        this.cl = FV_COMBS.map(c => new Comb(n(c))); this.cr = FV_COMBS.map(c => new Comb(n(c + FV_SPREAD)));
        this.al = FV_APS.map(a => new AllPass(n(a))); this.ar = FV_APS.map(a => new AllPass(n(a + FV_SPREAD)));
        this.pre = new Float32Array(RV_PREDELAY + 1); this.pi = 0;
        this.fb = RV_ROOM * 0.28 + 0.7; this.damp = RV_DAMP * 0.4;
        this.tail = 0;
    }
    // Int16 interleaved stereo in → Int16 interleaved stereo out (same length).
    run(inp) {
        const out = Buffer.alloc(inp.length);
        const N = inp.length >> 2;
        let peak = 0;
        for (let k = 0; k < N; k++) {
            const x = (inp.readInt16LE(k * 4) + inp.readInt16LE(k * 4 + 2)) / 65536;   // mono, ±1
            const d = this.pre[this.pi]; this.pre[this.pi] = x * 0.015; if (++this.pi >= this.pre.length) this.pi = 0;
            let l = 0, r = 0;
            for (let c = 0; c < 8; c++) { l += this.cl[c].run(d, this.fb, this.damp); r += this.cr[c].run(d, this.fb, this.damp); }
            for (let a = 0; a < 4; a++) { l = this.al[a].run(l); r = this.ar[a].run(r); }
            l *= RV_WET; r *= RV_WET;
            const al = Math.abs(l), ar = Math.abs(r); if (al > peak) peak = al; if (ar > peak) peak = ar;
            out.writeInt16LE(Math.max(-32767, Math.min(32767, Math.round(l * 32767))), k * 4);
            out.writeInt16LE(Math.max(-32767, Math.min(32767, Math.round(r * 32767))), k * 4 + 2);
        }
        this.tail = peak;
        return out;
    }
}
const FRAME_BYTES = 960 * 2 * 2;
const OpusScript = require('opusscript');
// LOW LATENCY, every step of it (the first version queued ~0.4 s):
//  • the member's packets are decoded the moment they arrive (opusscript, no stream);
//  • at most RV_IN_MAX frames wait to be used — older audio is dropped, never queued;
//  • the output is PULLED: the player asks for its next 20 ms packet and only then is
//    it reverbed and encoded (Opus RESTRICTED_LOWDELAY) — no Node stream buffering
//    (a piped encoder alone held 16 packets, 320 ms) and no timer of our own;
//  • when the tail has rung out nothing is produced and the bot falls silent.
const RV_IN_MAX = 3;
let rvRun = null;      // { id, sub, dec, enc, fifo, out, player, verb, quiet, want }
function reverbStart(conn, id) {
    reverbStop();
    let sub, dec, enc;
    try {
        sub = conn.receiver.subscribe(id, { end: { behavior: EndBehaviorType.Manual } });
        dec = new OpusScript(48000, 2, OpusScript.Application.AUDIO);
        enc = new OpusScript(48000, 2, OpusScript.Application.RESTRICTED_LOWDELAY);
        try { enc.setBitrate(64000); } catch (_) {}
    } catch (err) { console.error('❌ reverb: cannot listen —', err && err.message); return; }
    const r = { id, sub, dec, enc, fifo: [], verb: new Freeverb(), quiet: 0, out: null, player: null, want: false };
    const silence = Buffer.alloc(FRAME_BYTES);
    // one 20 ms packet, made now: the newest voice we have (or silence while the tail rings)
    const make = () => {
        const f = r.fifo.shift() || null;
        if (!f && r.quiet > 40) return null;              // the tail has died away: stay quiet
        const w = r.verb.run(f || silence);
        if (f) r.quiet = 0; else if (r.verb.tail < 0.002) r.quiet++;
        try { return enc.encode(w, 960); } catch (_) { return null; }
    };
    const feed = () => {
        while (r.want) {
            const pkt = make();
            if (!pkt) return;
            r.want = r.out.push(pkt);
        }
    };
    sub.on('data', (pkt) => {
        let pcm;
        try { pcm = dec.decode(pkt); } catch (_) { return; }
        if (!pcm || pcm.length !== FRAME_BYTES) return;
        r.fifo.push(pcm);
        while (r.fifo.length > RV_IN_MAX) r.fifo.shift();   // late audio is worse than lost audio
        if (r.want) feed();
    });
    sub.on('error', () => {});
    r.out = new Readable({ objectMode: true, highWaterMark: 1, read() { r.want = true; feed(); } });
    r.player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Play, maxMissedFrames: Infinity } });
    r.player.on('error', (e) => console.error('❌ reverb player:', e && e.message));
    r.player.play(createAudioResource(r.out, { inputType: StreamType.Opus, silencePaddingFrames: 0 }));
    conn.subscribe(r.player);
    rvRun = r;
    console.log(`🎛️ reverb on for ${id}`);
}
function reverbStop() {
    const r = rvRun;
    if (!r) return;
    rvRun = null;
    try { r.sub.removeAllListeners('data'); r.sub.destroy(); } catch (_) {}
    try { r.player.stop(true); } catch (_) {}
    try { r.out.push(null); } catch (_) {}
    try { r.dec.delete(); } catch (_) {}
    try { r.enc.delete(); } catch (_) {}
    console.log('🎛️ reverb off');
}
// Listening only while the member has the reverb on — off, nothing is subscribed.
function applyReverb() {
    const conn = state.guild && getVoiceConnection(state.guild.id);
    const want = !!(state.mic && state.mic.rv && conn && micChannel() && state.channelId === micChannel().id && !state.selfMute);
    if (want && (!rvRun || rvRun.id !== state.mic.id)) reverbStart(conn, state.mic.id);
    else if (!want && rvRun) reverbStop();
}
function inStage() { return state.channelId === STAGE_CHANNEL_ID; }
// Who is in the Stage channel, and who of them is a speaker: not suppressed AND
// not merely invited. A moderator's «invite to speak» already sets suppress to
// false, with requestToSpeakTimestamp set until the member accepts — counting
// that as a speaker put people on the hall's stage who never said yes.
let stageSendTimer = null;
function sendStage() {
    if (!inStage() || !state.guild) return;
    const ch = state.guild.channels.cache.get(STAGE_CHANNEL_ID);
    if (!ch) return;
    const humans = humansIn(ch);
    const sp = [];
    for (const [id, m] of humans) if (m.voice && m.voice.suppress === false && !m.voice.requestToSpeakTimestamp) sp.push(id);
    relaySend({ t: 'stage', uid: RELAY_UID, ids: [...humans.keys()], sp });
}
function scheduleStageSend() {
    if (stageSendTimer) return;
    stageSendTimer = setTimeout(() => { stageSendTimer = null; sendStage(); }, 300);
}

// ── Who's in the call / who's talking ─────────────────────────────────────────
function humansIn(channel) {
    return channel ? channel.members.filter(m => !m.user.bot) : new Map();
}
function speakingIds() {
    return [...state.speaking].filter(([, s]) => s.on).map(([id]) => id);
}
function sendHeartbeat() {
    if (!state.channelId || !state.guild) return;
    const ch = state.guild.channels.cache.get(state.channelId);
    relaySend({ t: 'vc', uid: RELAY_UID, ids: [...humansIn(ch).keys()], sp: speakingIds() });
    sendStage();
}
function speakOn(id) {
    const s = state.speaking.get(id) || { on: false, offTimer: null };
    if (s.offTimer) { clearTimeout(s.offTimer); s.offTimer = null; }
    if (!s.on) {
        s.on = true;
        relaySend({ t: 'spk', uid: RELAY_UID, s: id, on: 1 });
    }
    state.speaking.set(id, s);
}
function speakOff(id) {
    const s = state.speaking.get(id);
    if (!s || !s.on || s.offTimer) return;
    s.offTimer = setTimeout(() => {
        s.offTimer = null;
        s.on = false;
        relaySend({ t: 'spk', uid: RELAY_UID, s: id, on: 0 });
    }, SPEAK_OFF_GRACE_MS);
}
function clearSpeaking() {
    for (const [id, s] of state.speaking) {
        if (s.offTimer) clearTimeout(s.offTimer);
        if (s.on) relaySend({ t: 'spk', uid: RELAY_UID, s: id, on: 0 });
    }
    state.speaking.clear();
}

setInterval(() => {
    for (const id of speakingIds()) relaySend({ t: 'spk', uid: RELAY_UID, s: id, on: 1 });
}, SPEAK_REASSERT_MS);
setInterval(sendHeartbeat, HEARTBEAT_MS);
// A seat nobody re-announced (tab closed, socket gone) expires without an 'on:0'.
setInterval(() => {
    const now = Date.now();
    for (const at of state.seated.values()) if (now - at > SEAT_STALE_MS) { scheduleRescan(0); return; }
    // The hall went quiet (the meeting ended): the Stage channel is no longer wanted.
    if (inStage() && !stageLive()) scheduleRescan(0);
}, 10000);

// ── Which channel to sit in ───────────────────────────────────────────────────
function voiceOk(c) {
    return !!c && c.isVoiceBased() && c.type !== ChannelType.GuildStageVoice && c.id !== c.guild.afkChannelId;
}

function pickChannel(guild) {
    // -1) The field's mic podium: wherever its member is in a call, above everything.
    const mc = micChannel();
    if (mc) return mc;
    // 0) The general meeting: the Stage channel, while the hall is live and someone is in it.
    if (stageLive() && !(state.kicked && state.kicked.until > Date.now() && state.kicked.channelId === STAGE_CHANNEL_ID)) {
        const st = guild.channels.cache.get(STAGE_CHANNEL_ID);
        if (st && st.isVoiceBased() && humansIn(st).size > 0) return st;
    }
    // 1) Summoned: that call, for as long as whoever asked is still in it.
    if (state.pin) {
        const vs = guild.voiceStates.cache.get(state.pin.by);
        const ch = guild.channels.cache.get(state.pin.channelId);
        if (vs && vs.channelId === state.pin.channelId && voiceOk(ch)) return ch;
        console.log('📌 summon over — whoever asked left that call');
        state.pin = null;
    }
    // 2) Following the meeting: the call the members at the site's table are in.
    const kicked = state.kicked && state.kicked.until > Date.now() ? state.kicked.channelId : null;
    const count = new Map();
    for (const id of state.seated.keys()) {
        const vs = guild.voiceStates.cache.get(id);
        const chId = vs && vs.channelId;
        if (!chId || chId === kicked) continue;
        if (MEETING_CHANNEL_IDS.length && !MEETING_CHANNEL_IDS.includes(chId)) continue;
        count.set(chId, (count.get(chId) || 0) + 1);
    }
    // Stay put while anyone at the table is still here — never hop mid-meeting.
    if (state.channelId && count.has(state.channelId)) {
        const cur = guild.channels.cache.get(state.channelId);
        if (voiceOk(cur)) return cur;
    }
    let best = null, most = 0;
    for (const [id, n] of count) {
        const ch = guild.channels.cache.get(id);
        if (voiceOk(ch) && n > most) { best = ch; most = n; }
    }
    return best;
}

function lobbiesFor(channel) {
    if (channel.id === STAGE_CHANNEL_ID) return [STAGE_ROOM];
    const cat = channel.parent ? channel.parent.name : null;
    const hit = Object.entries(LOBBY_CATEGORIES).find(([, name]) => name === cat);
    return hit ? [hit[0]] : Object.keys(LOBBY_CATEGORIES);
}

function leaveVoice() {
    if (!state.guild) return;
    // Tell the site the call is over. The relay sockets stay — rescan() owns them.
    clearSpeaking();
    reverbStop();
    relaySend({ t: 'vc', uid: RELAY_UID, ids: [], sp: [] });
    const conn = getVoiceConnection(state.guild.id);
    if (conn) { try { conn.destroy(); } catch (_) {} }
    if (state.channelId) console.log(`👋 left voice: ${state.channelName}`);
    state.channelId = null;
    state.channelName = null;
    state.lobbies = [];
    state.leaveAt = 0;
}

function joinChannel(channel) {
    const guild = channel.guild;
    const mute = !(micChannel() && micChannel().id === channel.id);
    if (state.channelId === channel.id && getVoiceConnection(guild.id)) {
        if (mute !== state.selfMute) {
            // Same call, the podium came or went: rejoin with the new mute (no hop).
            state.selfMute = mute;
            joinVoiceChannel({ channelId: channel.id, guildId: guild.id, adapterCreator: guild.voiceAdapterCreator, selfDeaf: false, selfMute: mute });
            if (!mute) stageSpeak(channel);
            applyReverb();
        }
        return;
    }
    reverbStop();
    leaveVoice();
    state.selfMute = mute;
    state.channelId = channel.id;
    state.channelName = channel.name;
    state.lobbies = lobbiesFor(channel);
    for (const l of state.lobbies) relayOpen(l);
    console.log(`🎙️ joining voice: ${channel.name} → lobbies ${state.lobbies.join(', ')}`);

    // Deafened bots receive no audio packets — and the speaking events come from
    // those packets — so: muted, NOT deafened. It never subscribes to or decodes
    // anyone's audio; it only sees that packets are arriving.
    const conn = joinVoiceChannel({
        channelId: channel.id,
        guildId: guild.id,
        adapterCreator: guild.voiceAdapterCreator,
        selfDeaf: false,
        selfMute: mute,
    });
    conn.receiver.speaking.on('start', id => speakOn(id));
    conn.receiver.speaking.on('end', id => speakOff(id));
    conn.on(VoiceConnectionStatus.Ready, () => {
        console.log('✅ voice ready'); sendHeartbeat();
        if (!state.selfMute) { stageSpeak(channel); setTimeout(applyReverb, 600); }
    });
    conn.on(VoiceConnectionStatus.Disconnected, async () => {
        // A move between channels or a network blip: give it a moment to reconnect.
        try {
            await Promise.race([
                entersState(conn, VoiceConnectionStatus.Signalling, 5000),
                entersState(conn, VoiceConnectionStatus.Connecting, 5000),
            ]);
        } catch (_) {
            if (state.channelId === channel.id) { leaveVoice(); scheduleRescan(); }
        }
    });
    conn.on('error', err => console.error('❌ voice error:', err && err.message));
}

// In a Stage channel the bot is an audience member until it is made a speaker: it
// asks to be one (needs «Mute Members» there to do it itself), else it requests to speak.
function stageSpeak(channel) {
    if (!channel || channel.type !== ChannelType.GuildStageVoice) return;
    const me = channel.guild.members.me;
    setTimeout(() => {
        if (!me || !me.voice || me.voice.channelId !== channel.id) return;
        me.voice.setSuppressed(false)
            .then(() => { console.log('🎤 speaking in the stage'); micStatus('live'); })
            .catch(() => me.voice.setRequestToSpeak(true).then(() => micStatus('nospeak')).catch(() => micStatus('nospeak')));
    }, 800);
}

function rescan() {
    const g = state.guild;
    if (!g) return;
    const now = Date.now();
    for (const [id, at] of state.seated) if (now - at > SEAT_STALE_MS) state.seated.delete(id);
    // The relay rooms are only worth listening to while someone is in a call at all.
    const anyone = g.voiceStates.cache.some(vs =>
        vs.channelId && vs.id !== client.user.id && !(vs.member && vs.member.user.bot));
    for (const l of [...Object.keys(LOBBY_CATEGORIES), STAGE_ROOM]) { if (anyone) relayOpen(l); else relayClose(l); }
    if (state.mic && !micLive()) micOff();

    const ch = pickChannel(g);
    if (ch) { state.leaveAt = 0; joinChannel(ch); sendHeartbeat(); return; }
    if (!state.channelId) return;
    const cur = g.channels.cache.get(state.channelId);
    // Nobody left in the call → go now. The table emptied → go after the grace period
    // (someone standing up for a moment, or reloading, mustn't make it leave and rejoin).
    if (!cur || humansIn(cur).size === 0) { leaveVoice(); return; }
    if (!state.leaveAt) state.leaveAt = now + LEAVE_GRACE_MS;
    if (now >= state.leaveAt) { leaveVoice(); return; }
    scheduleRescan(state.leaveAt - now + 50);
}

let rescanTimer = null, rescanDue = 0;
function scheduleRescan(ms = RESCAN_DEBOUNCE_MS) {
    const due = Date.now() + ms;
    if (rescanTimer && rescanDue <= due) return;   // an earlier one is already coming
    clearTimeout(rescanTimer);
    rescanDue = due;
    rescanTimer = setTimeout(() => { rescanTimer = null; rescan(); }, ms);
}

// ── Discord ───────────────────────────────────────────────────────────────────
// GuildMessages (not privileged) is for the mention summon — a message that mentions
// the bot arrives with its mentions even without the Message Content intent.
const client = new Client({ intents: [
    GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates, GatewayIntentBits.GuildMessages,
] });

client.once(Events.ClientReady, () => {
    console.log(`🤖 ${VERSION} logged in as ${client.user.tag}`);
    state.guild = client.guilds.cache.first() || null;
    if (!state.guild) { console.error('❌ No guild found — is the bot in the server?'); return; }
    console.log(`📡 Watching voice in: ${state.guild.name}`);
    scheduleRescan();
});

client.on(Events.VoiceStateUpdate, (oldState, newState) => {
    if (!state.guild) return;
    if (oldState.id === client.user.id || newState.id === client.user.id) {
        // Disconnected by hand (right-click → Disconnect): take the hint and stay out of
        // that call for a while instead of hopping straight back in. Our own leaves
        // never match — leaveVoice() clears state.channelId before this event lands.
        if (oldState.channelId && !newState.channelId && oldState.channelId === state.channelId) {
            state.kicked = { channelId: oldState.channelId, until: Date.now() + KICK_COOLDOWN_MS };
            if (state.pin && state.pin.channelId === oldState.channelId) state.pin = null;
            console.log(`🚪 disconnected by hand from ${state.channelName} — staying out for ${KICK_COOLDOWN_MS / 60000} min`);
            leaveVoice();
            scheduleRescan();
        }
        // Dragged into another call by hand: go along with it (the voice connection
        // already has), and don't follow the table back into the old one for a while.
        else if (oldState.channelId && newState.channelId && oldState.channelId !== newState.channelId
                 && oldState.channelId === state.channelId) {
            const ch = state.guild.channels.cache.get(newState.channelId);
            state.kicked = { channelId: oldState.channelId, until: Date.now() + KICK_COOLDOWN_MS };
            clearSpeaking();
            relaySend({ t: 'vc', uid: RELAY_UID, ids: [], sp: [] });
            state.channelId = newState.channelId;
            state.channelName = ch ? ch.name : null;
            state.lobbies = ch ? lobbiesFor(ch) : [];
            for (const l of state.lobbies) relayOpen(l);
            console.log(`↪️ moved by hand to ${state.channelName}`);
            sendHeartbeat();
            scheduleRescan();
        }
        return;
    }
    // The Stage channel: someone came, went, was brought up or sent back down.
    if (inStage() && (oldState.channelId === STAGE_CHANNEL_ID || newState.channelId === STAGE_CHANNEL_ID)) scheduleStageSend();
    // The podium's member changed call (or left Discord): follow, or say so.
    if (state.mic && newState.id === state.mic.id) {
        if (!newState.channelId) { reverbStop(); micStatus('nocall'); }
        scheduleRescan(0);
    }
    // Someone left mid-sentence: make sure their green ring goes out.
    if (oldState.channelId === state.channelId && newState.channelId !== state.channelId) speakOff(oldState.id);
    scheduleRescan();
});

// @MdwnhBot (the user or its role — Discord's autocomplete offers both) → come to the
// caller's call, whatever the site is doing. No reply text: a 🎧 reaction means it's
// coming, 🤷 means the caller isn't in a call.
client.on(Events.MessageCreate, msg => {
    if (!state.guild || !msg.guild || msg.guild.id !== state.guild.id || msg.author.bot) return;
    const botRole = msg.guild.members.me && msg.guild.members.me.roles.botRole;
    const pinged = msg.mentions.users.has(client.user.id)
        || (botRole && msg.mentions.roles.has(botRole.id));
    if (!pinged) return;
    const ch = msg.member && msg.member.voice ? msg.member.voice.channel : null;
    if (!voiceOk(ch)) { msg.react('🤷').catch(() => {}); return; }
    state.pin = { channelId: ch.id, by: msg.author.id };
    if (state.kicked && state.kicked.channelId === ch.id) state.kicked = null;
    state.leaveAt = 0;
    console.log(`📣 summoned to ${ch.name} by ${msg.author.tag}`);
    msg.react('🎧').catch(() => {});
    scheduleRescan(0);
});

client.on('error', err => console.error('❌ Discord client error:', err));
process.on('unhandledRejection', err => console.error('❌ Unhandled rejection:', err));

const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;
if (!DISCORD_TOKEN) {
    console.error('❌ DISCORD_BOT_TOKEN not found in environment variables!');
    process.exit(1);
}
client.login(DISCORD_TOKEN);
