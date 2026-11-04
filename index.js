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
// RECORDING (v6-rec) — «@ليمو سجل» from someone in a call: the bot comes to that call
// (a Stage channel too) and records it until «@ليمو انهاء التسجيل», until everyone has
// left, or REC_MAX_MS. It says so: a reply, a line in the call's own chat, «🔴» on its
// nickname + status, and {t:'brec', on} into the hall's relay room (the site shows it).
// The result goes by DM to RECORD_TO: the call as ONE mixed MP3 (record.js; parts of
// 35 min so each fits a 10 MB upload) + a context file written for an AI to transcribe
// and summarise — the meeting, who attended, who spoke when, and the chat: the channel
// it was asked in, the call's own chat, and the مقر hall's comments if a hall meeting
// is live at the same time ({t:'hc'} / {t:'hsg'} in the 'stage' room, names off {t:'hi'}).
// Whoever asked is mentioned when it is done. Reading chat needs the privileged
// Message Content intent — off in the Developer Portal = audio only, and it says so.
// While recording it DOES subscribe to and decode everyone's audio; otherwise never.
// -----------------------------------------------------------------------------
require('dotenv').config();
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const WebSocket = require('ws');
const { Client, GatewayIntentBits, ChannelType, Events, ActivityType } = require('discord.js');
const {
    joinVoiceChannel, getVoiceConnection, VoiceConnectionStatus, entersState, EndBehaviorType,
} = require('@discordjs/voice');
const { Recorder } = require('./record');

const VERSION = 'v6-rec';

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
// Recording.
const RECORD_TO = process.env.RECORD_TO || '567266235163738112';   // who gets the files
const REC_MAX_MS = (Number(process.env.REC_MAX_HOURS) || 4) * 3600000;
const REC_EMPTY_GRACE_MS = 10000;   // the last one out reconnecting must not end it
const REC_JOIN_MAX_MS = 20000;
const REC_CHAT_MAX = 4000;          // chat lines kept per recording
const REC_MARK = '🔴 ';
const TZ = 'Asia/Riyadh';
const ROSTER_URL = 'https://raw.githubusercontent.com/mdwnstudio/MdwnhMembers/main/members.json';
const PROFILES_URL = 'https://mdwnstudio.github.io/Members/profiles.json';

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
    rec: null,                      // the recording (see «Recording» below)
    chatOn: process.env.CHAT_LOG !== '0',   // logged in with the Message Content intent
    finishing: new Set(),           // recordings being encoded / sent
};

// ── Health check (Render needs a bound port) + keep-alive ─────────────────────
http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    const rec = state.rec && state.rec.live ? ` — recording ${clock(Date.now() - state.rec.t0)}` : '';
    res.end(`MdwnhBot ${VERSION} alive — voice: ${state.channelName || 'idle'}${rec}`);
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
function relaySendRoom(room, obj) {
    const ws = relays[room] && relays[room].ws;
    if (ws && ws.readyState === WebSocket.OPEN) { try { ws.send(JSON.stringify(obj)); } catch (_) {} }
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
// "is it live" matters here — unless a recording is running, which also keeps the
// hall's comments (and who is who, off the attendees' "I am here").
function stageLive() { return Date.now() - state.stageLiveAt < STAGE_LIVE_MS; }
function onStageRelayMessage(data) {
    const s = data.toString();
    const hi = s.indexOf('"t":"hi"') !== -1;
    if (hi) {
        const was = stageLive();
        state.stageLiveAt = Date.now();
        if (!was) { console.log('🎤 the hall is live'); scheduleRescan(0); }
    }
    if (!state.rec || !state.rec.live) return;
    if (!hi && s.indexOf('"t":"hc"') === -1 && s.indexOf('"t":"hsg"') === -1) return;
    let m;
    try { m = JSON.parse(s); } catch (_) { return; }
    recHallMsg(m);
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
    // R) Recording: that call, whatever else is going on, until the recording ends.
    if (state.rec) {
        const ch = guild.channels.cache.get(state.rec.channelId);
        if (ch && ch.isVoiceBased()) return ch;
        recStop('lost');
    }
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
    // A recording of this call ends with it — and is still sent, as far as it got.
    if (state.rec && state.rec.live && state.rec.channelId === state.channelId) recStop('lost');
    // Tell the site the call is over. The relay sockets stay — rescan() owns them.
    clearSpeaking();
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
    if (state.channelId === channel.id && getVoiceConnection(guild.id)) return;
    leaveVoice();
    state.channelId = channel.id;
    state.channelName = channel.name;
    state.lobbies = lobbiesFor(channel);
    for (const l of state.lobbies) relayOpen(l);
    console.log(`🎙️ joining voice: ${channel.name} → lobbies ${state.lobbies.join(', ')}`);

    // Deafened bots receive no audio packets — and the speaking events come from
    // those packets — so: muted, NOT deafened. Outside a recording it never subscribes
    // to or decodes anyone's audio; it only sees that packets are arriving.
    const conn = joinVoiceChannel({
        channelId: channel.id,
        guildId: guild.id,
        adapterCreator: guild.voiceAdapterCreator,
        selfDeaf: false,
        selfMute: true,
    });
    // 'start' fires synchronously on a speaker's first packet, before the receiver
    // looks for a subscription — so subscribing here catches that packet too.
    conn.receiver.speaking.on('start', id => { speakOn(id); recSubscribe(id, conn); });
    conn.receiver.speaking.on('end', id => speakOff(id));
    conn.on(VoiceConnectionStatus.Ready, () => { console.log('✅ voice ready'); sendHeartbeat(); recMaybeBegin(); });
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

function rescan() {
    const g = state.guild;
    if (!g) return;
    const now = Date.now();
    for (const [id, at] of state.seated) if (now - at > SEAT_STALE_MS) state.seated.delete(id);
    // The relay rooms are only worth listening to while someone is in a call at all.
    const anyone = g.voiceStates.cache.some(vs =>
        vs.channelId && vs.id !== client.user.id && !(vs.member && vs.member.user.bot));
    for (const l of [...Object.keys(LOBBY_CATEGORIES), STAGE_ROOM]) { if (anyone) relayOpen(l); else relayClose(l); }

    const ch = pickChannel(g);
    if (ch) { state.leaveAt = 0; joinChannel(ch); sendHeartbeat(); recMaybeBegin(); return; }
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

// ── Recording ─────────────────────────────────────────────────────────────────
// state.rec, while asked for / running:
//   { channelId, channelName, stage, by, byName, cmdMsg, cmdChannelId, chatOn,
//     live, t0, conn, dir, audio (record.js), subs: userId → receive stream,
//     people: userId → { name, ms, since }, chat: [...], hall: { names, title, seen },
//     info: { event, about, topic }, emptyAt, timers }
// Nothing of it touches Firebase or the site's data: the audio is written to a temp
// folder, sent by DM, and deleted.
const sleep = ms => new Promise(res => setTimeout(res, ms));
const pad2 = n => String(n).padStart(2, '0');
function clock(ms) {
    const s = Math.max(0, Math.round(ms / 1000));
    return `${pad2(Math.floor(s / 3600))}:${pad2(Math.floor(s / 60) % 60)}:${pad2(s % 60)}`;
}
function clip(s, n) {
    const a = [...String(s == null ? '' : s).replace(/\s+/g, ' ').trim()];
    return a.length > n ? a.slice(0, n - 1).join('') + '…' : a.join('');
}
// A chat line on one line: its line breaks become «⏎».
const chatText = (s, n) => clip(String(s == null ? '' : s).replace(/\s*\n\s*/g, ' ⏎ '), n);
const mdCell = s => (String(s || '').replace(/\|/g, '¦').replace(/\s+/g, ' ').trim() || '—');
const fmtDay = t => new Intl.DateTimeFormat('ar-SA-u-ca-gregory-nu-latn', { timeZone: TZ, weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }).format(t);
const fmtTime = t => new Intl.DateTimeFormat('ar-SA-u-ca-gregory-nu-latn', { timeZone: TZ, hour: 'numeric', minute: '2-digit' }).format(t);
const fmtHijri = t => new Intl.DateTimeFormat('ar-SA-u-ca-islamic-umalqura-nu-latn', { timeZone: TZ, year: 'numeric', month: 'long', day: 'numeric' }).format(t);
function fileStamp(t) {
    const p = Object.fromEntries(new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' })
        .formatToParts(t).map(x => [x.type, x.value]));
    return `meeting-${p.year}-${p.month}-${p.day}-${p.hour}${p.minute}`;
}
async function withRetry(fn, tries = 3) {
    let last;
    for (let i = 0; i < tries; i++) {
        try { return await fn(); } catch (err) { last = err; if (i < tries - 1) await sleep(5000 * (i + 1)); }
    }
    throw last;
}

// The commands. Folded like the site folds names (hamza, harakat, ta marbuta), so
// «سجّل» / «إنهاء التسجيل» / «انهاء التسجيل» all read the same.
function foldAr(s) {
    return String(s || '').normalize('NFC')
        .replace(/[ً-ٰٟـ]/g, '')
        .replace(/[أإآٱ]/g, 'ا').replace(/ى/g, 'ي').replace(/ة/g, 'ه').replace(/ؤ/g, 'و').replace(/ئ/g, 'ي')
        .toLowerCase();
}
function cmdText(msg) {
    return foldAr(String(msg.content || '').replace(/<@[!&]?\d+>/g, ' '))
        .replace(/[.!?؟،,:]+/g, ' ').replace(/\s+/g, ' ').trim();
}
const REC_STOP_RE = /(^| )(انهاء|انهي|انه|اوقف|وقف|ايقاف|اختم|stop|end) ?(ال)?(تسجيل|recording|record)( |$)/;
const REC_START_RE = /(^| )(سجل|سجلي|سجلوا|ابدا ?(ال)?تسجيل|شغل ?(ال)?تسجيل|record|rec)( |$)/;
// While recording, «انهاء التسجيل» alone (no mention) in the recorded chats ends it too.
const REC_STOP_BARE_RE = /^(انهاء|انهي|اوقف|وقف|ايقاف) ?(ال)?تسجيل$/;

// Who is who — the team roster (names as the team writes them) + their roles.
let _team = null, _teamAt = 0;
async function loadTeam() {
    if (_team && Date.now() - _teamAt < 30 * 60000) return _team;
    const get = u => fetch(u, { signal: AbortSignal.timeout(6000) }).then(r => (r.ok ? r.json() : null)).catch(() => null);
    const [roster, profiles] = await Promise.all([get(ROSTER_URL), get(PROFILES_URL)]);
    const prof = (profiles && profiles.profiles) || {};
    const byId = new Map(), list = [];
    for (const m of ((roster && roster.members) || [])) {
        if (!m || !m.slug || m.dummy) continue;
        const p = prof[m.slug] || {};
        const rec = { name: m.name, role: p.role || '', depts: Array.isArray(p.depts) ? p.depts : [] };
        if (m.active !== false) list.push(rec);
        for (const id of [m.discordId, ...(Array.isArray(m.altDiscordIds) ? m.altDiscordIds : [])]) {
            if (id) byId.set(String(id), rec);
        }
    }
    const team = { byId, list };
    if (list.length) { _team = team; _teamAt = Date.now(); }
    return _team || team;
}
function memberName(id, fallback) {
    const m = state.guild && state.guild.members.cache.get(id);
    return (m && m.displayName) || fallback || String(id);
}
function personName(team, id, fallback) {
    const t = team.byId.get(String(id));
    if (t) return t.name;
    if (/^siraj_/.test(id)) return 'سراج (حساب تجربة)';
    return fallback || memberName(id);
}

function recChannelOk(c) {
    return !!c && c.isVoiceBased() && c.id !== c.guild.afkChannelId;
}
function reply(msg, content) {
    const o = { content, allowedMentions: { parse: [], repliedUser: false } };
    return msg.reply(o).catch(() => msg.channel.send(o)).catch(() => {});
}

// The meeting's name: the Discord event running in that channel (a Stage's event
// first), its description (often the agenda), and the Stage's own topic.
async function meetingInfo(ch) {
    const out = { event: null, about: null, topic: null };
    if (!ch) return out;
    try {
        const g = ch.guild;
        const si = g.stageInstances.cache.find(s => s.channelId === ch.id);
        if (si && si.topic) out.topic = si.topic;
        let ev = null;
        if (si && si.guildScheduledEventId) ev = await g.scheduledEvents.fetch(si.guildScheduledEventId).catch(() => null);
        if (!ev) {
            const all = await g.scheduledEvents.fetch().catch(() => null);
            ev = (all && all.find(e => e.channelId === ch.id && e.isActive())) || null;
        }
        if (ev) { out.event = ev.name || null; out.about = ev.description || null; }
    } catch (_) {}
    return out;
}
function recTitle(r) {
    const i = r.info || {};
    return i.event || (r.stage && r.hall.title) || i.topic || r.channelName;
}

// «🔴» on its nickname (what everyone in the call sees in the member list) and status.
// A restart mid-recording leaves the nickname behind — onReady takes it back off.
function recLook(on) {
    if (!client || !client.user) return;
    try {
        client.user.setPresence({ activities: on ? [{ type: ActivityType.Custom, name: 'Custom Status', state: '🔴 يسجّل الاجتماع' }] : [], status: 'online' });
    } catch (_) {}
    const me = state.guild && state.guild.members.me;
    if (!me) return;
    const cur = me.nickname || '';
    if (on && !cur.startsWith(REC_MARK)) {
        me.setNickname([...(REC_MARK + (cur || me.user.username))].slice(0, 32).join('')).catch(() => {});
    } else if (!on && cur.startsWith(REC_MARK)) {
        const base = cur.slice(REC_MARK.length);
        me.setNickname(base && base !== me.user.username ? base : null).catch(() => {});
    }
}
// The site's hall shows «🔴 يُسجَّل» while this keeps arriving (it forgets it after 45 s).
function recAnnounceHall() {
    const r = state.rec;
    if (r && r.live) relaySendRoom(STAGE_ROOM, { t: 'brec', uid: RELAY_UID, on: 1, a: r.stage ? 1 : 0 });
}
setInterval(recAnnounceHall, HEARTBEAT_MS);

function recStartCommand(msg) {
    const ch = msg.member && msg.member.voice ? msg.member.voice.channel : null;
    if (!recChannelOk(ch)) { reply(msg, 'ادخل مكالمة صوتية أولًا، ثم اطلب التسجيل.'); return; }
    const cur = state.rec;
    if (cur) {
        if (cur.channelId !== ch.id) reply(msg, `أسجّل الآن مكالمة أخرى («${cur.channelName}»)، أنهِ ذلك التسجيل أولًا.`);
        else reply(msg, cur.live ? `أسجّل هذه المكالمة منذ ${clock(Date.now() - cur.t0)}.` : 'أستعدّ للتسجيل…');
        return;
    }
    const r = state.rec = {
        channelId: ch.id, channelName: ch.name, stage: ch.type === ChannelType.GuildStageVoice,
        by: msg.author.id, byName: memberName(msg.author.id, msg.author.username),
        cmdMsg: msg, cmdChannelId: msg.channelId, chatOn: state.chatOn,
        live: false, t0: 0, conn: null, dir: null, audio: null,
        subs: new Map(), people: new Map(), chat: [],
        hall: { names: new Map(), title: null, seen: false },
        info: null, emptyAt: 0, timers: {},
    };
    r.timers.join = setTimeout(() => {
        if (state.rec !== r || r.live) return;
        state.rec = null;
        reply(msg, 'تعذّر دخول المكالمة لبدء التسجيل، حاول مرة أخرى.');
        scheduleRescan(0);
    }, REC_JOIN_MAX_MS);
    // Asked for by name: an earlier «disconnected by hand» no longer keeps it out.
    if (state.kicked && state.kicked.channelId === ch.id) state.kicked = null;
    state.leaveAt = 0;
    console.log(`⏺️ recording asked for in ${ch.name} by ${msg.author.tag}`);
    msg.react('🎙️').catch(() => {});
    scheduleRescan(0);
}

// Starts once the bot is really in that call (rescan → joinChannel → Ready).
function recMaybeBegin() {
    const r = state.rec;
    if (!r || r.live || !state.guild || state.channelId !== r.channelId) return;
    const conn = getVoiceConnection(state.guild.id);
    if (!conn || conn.joinConfig.channelId !== r.channelId || conn.state.status !== VoiceConnectionStatus.Ready) return;
    recBegin(r, conn);
}
function recBegin(r, conn) {
    clearTimeout(r.timers.join);
    const ch = state.guild.channels.cache.get(r.channelId);
    r.live = true;
    r.t0 = Date.now();
    r.conn = conn;
    try {
        r.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mdwnh-rec-'));
        r.audio = new Recorder(r.dir);
        r.audio.start(err => { console.error('❌ recorder:', err.message); if (state.rec === r) recStop('fail'); });
    } catch (err) {
        console.error('❌ recorder:', err && err.message);
        r.audio = r.audio || new Recorder(path.join(os.tmpdir(), 'mdwnh-rec-failed'));
        recStop('fail');
        return;
    }
    for (const [id, m] of humansIn(ch)) { recJoin(r, id, m); recSubscribe(id, conn); }
    r.timers.max = setTimeout(() => { if (state.rec === r) recStop('max'); }, REC_MAX_MS);
    recLook(true);
    recAnnounceHall();
    meetingInfo(ch).then(i => { r.info = i; });
    console.log(`🔴 recording ${r.channelName}${r.chatOn ? ' + chat' : ''}`);

    // Say so — where it was asked, and in the call's own chat.
    const hall = stageLive() ? '، وتعليقات القاعة في المقر' : '';
    const what = !r.chatOn ? `صوت المكالمة «${r.channelName}»`
        : r.cmdChannelId === r.channelId ? `المكالمة «${r.channelName}» ومحادثتها النصية`
        : `المكالمة «${r.channelName}» ومحادثتها النصية، وما يُكتب في هذه القناة`;
    reply(r.cmdMsg, `🔴 بدأتُ تسجيل ${what}${hall}.\nلإيقافه: <@${client.user.id}> انهاء التسجيل — ويتوقف وحده إذا غادر الجميع.`);
    if (r.cmdChannelId !== r.channelId && ch && typeof ch.send === 'function') {
        ch.send({
            content: `🔴 هذه المكالمة تُسجَّل الآن${r.chatOn ? '، وكذلك ما يُكتب هنا' : ''} — بطلب من ${r.byName}.`,
            allowedMentions: { parse: [] },
        }).catch(() => {});
    }
    recCheckEmpty();
}

// One receive stream per person for the whole recording. Packets only flow while they
// talk; a stream the receiver destroyed (a decryption hiccup) is replaced on their
// next 'start'.
function recSubscribe(id, conn) {
    const r = state.rec;
    if (!r || !r.live || !r.conn || (conn && conn !== r.conn) || r.subs.has(id)) return;
    if (client.user && id === client.user.id) return;
    const m = state.guild && state.guild.members.cache.get(id);
    if (m && m.user.bot) return;
    let st;
    try { st = r.conn.receiver.subscribe(id, { end: { behavior: EndBehaviorType.Manual } }); } catch (_) { return; }
    r.subs.set(id, st);
    st.on('data', pkt => { if (r.live) r.audio.packet(id, pkt); });
    st.on('error', () => {});
    st.once('close', () => { if (r.subs.get(id) === st) r.subs.delete(id); });
}

function recJoin(r, id, member) {
    if (member && member.user && member.user.bot) return;
    let p = r.people.get(id);
    if (!p) { p = { name: member ? member.displayName : memberName(id), ms: 0, since: null }; r.people.set(id, p); }
    else if (member) p.name = member.displayName;
    if (p.since === null) p.since = Date.now();
}
function recLeave(r, id) {
    const p = r.people.get(id);
    if (p && p.since !== null) { p.ms += Date.now() - p.since; p.since = null; }
}
function recVoiceState(oldState, newState) {
    const r = state.rec;
    if (!r || !r.live) return;
    if (newState.channelId === r.channelId && oldState.channelId !== r.channelId) {
        if (!(newState.member && newState.member.user.bot)) { recJoin(r, newState.id, newState.member); recSubscribe(newState.id, r.conn); }
    } else if (oldState.channelId === r.channelId && newState.channelId !== r.channelId) {
        recLeave(r, oldState.id);
    }
    recCheckEmpty();
}
// Everyone gone → it ends, after REC_EMPTY_GRACE_MS (a dropped connection rejoining).
function recCheckEmpty() {
    const r = state.rec;
    if (!r || !r.live || !state.guild) return;
    const ch = state.guild.channels.cache.get(r.channelId);
    clearTimeout(r.timers.empty);
    if (ch && humansIn(ch).size > 0) { r.emptyAt = 0; return; }
    const now = Date.now();
    if (!r.emptyAt) r.emptyAt = now;
    if (now - r.emptyAt >= REC_EMPTY_GRACE_MS) { recStop('empty'); return; }
    r.timers.empty = setTimeout(recCheckEmpty, REC_EMPTY_GRACE_MS - (now - r.emptyAt) + 50);
}

// The chats. Discord: the channel it was asked in + the call's own text chat (needs
// the Message Content intent). The hall: its comments, off the relay.
function recPushChat(r, e) { if (r.chat.length < REC_CHAT_MAX) r.chat.push(e); }
function recLogDiscord(msg) {
    const r = state.rec;
    if (!r || !r.live || !r.chatOn) return;
    if (msg.channelId !== r.channelId && msg.channelId !== r.cmdChannelId) return;
    let text = chatText(msg.cleanContent || msg.content, 1500);
    if (msg.stickers && msg.stickers.size) {
        text += (text ? ' ' : '') + [...msg.stickers.values()].map(s => `[ملصق: ${clip(s.name, 40)}]`).join(' ');
    }
    const files = [...msg.attachments.values()].slice(0, 10).map(a => `${a.name || 'ملف'} ${a.url}`);
    if (!text && !files.length) return;
    recPushChat(r, {
        src: msg.channelId, uid: msg.author.id, name: memberName(msg.author.id, msg.author.username),
        t: Date.now() - r.t0, text, files, reply: !!(msg.reference && msg.reference.messageId),
    });
}
function recHallMsg(m) {
    const r = state.rec;
    if (!r || !r.live || !m || typeof m.uid !== 'string' || m.uid.length > 40 || m.uid === RELAY_UID) return;
    const h = r.hall;
    if (m.t === 'hi') {
        h.seen = true;
        if (typeof m.n === 'string' && m.n.trim()) h.names.set(m.uid, clip(m.n, 40));
        if (typeof m.ti === 'string' && m.ti.trim()) h.title = clip(m.ti, 80);
        return;
    }
    let text = '';
    if (m.t === 'hc') {
        text = (typeof m.k === 'string' && m.k && !m.m) ? `[ملصق: ${clip(m.k, 40)}]` : chatText(m.m, 400);
    } else if (m.t === 'hsg') {
        text = chatText(m.m, 800);
        if (text) text = '[اقتراح] ' + text;
        const i = Math.max(0, Math.min(9, Number(m.i) || 0));
        if (text && i) text += i === 1 ? ' (مع صورة)' : i === 2 ? ' (مع صورتين)' : ` (مع ${i} صور)`;
    }
    if (!text) return;
    h.seen = true;
    recPushChat(r, { src: 'hall', uid: m.uid, t: Date.now() - r.t0, text, files: [] });
}

function recStopCommand(msg) {
    const r = state.rec;
    if (!r) { reply(msg, 'لا يوجد تسجيل جارٍ الآن.'); return; }
    const inCall = msg.member && msg.member.voice && msg.member.voice.channelId === r.channelId;
    if (!inCall && msg.author.id !== r.by && msg.author.id !== RECORD_TO) {
        reply(msg, 'يُنهي التسجيلَ من بدأه أو أحدُ من في المكالمة.');
        return;
    }
    msg.react('⏹️').catch(() => {});
    if (!r.live) {
        clearTimeout(r.timers.join);
        state.rec = null;
        reply(msg, 'أُلغي التسجيل قبل أن يبدأ.');
        scheduleRescan(0);
        return;
    }
    recStop('cmd', { id: msg.author.id, name: memberName(msg.author.id, msg.author.username) });
}

// Ends it at once (no more packets, the bot is free to leave) and finishes it in the
// background: the file is closed, the context written, both sent.
function recStop(reason, by) {
    const r = state.rec;
    if (!r) return null;
    state.rec = null;
    for (const k of Object.keys(r.timers)) clearTimeout(r.timers[k]);
    if (!r.live) return null;
    r.live = false;
    r.endedAt = Date.now();
    for (const st of r.subs.values()) { try { st.destroy(); } catch (_) {} }
    r.subs.clear();
    for (const id of r.people.keys()) recLeave(r, id);
    recLook(false);
    relaySendRoom(STAGE_ROOM, { t: 'brec', uid: RELAY_UID, on: 0 });
    console.log(`⏹️ recording stopped (${reason}) after ${clock(r.endedAt - r.t0)}`);
    scheduleRescan(0);
    const job = recFinish(r, reason, by)
        .catch(err => console.error('❌ finishing the recording:', err))
        .finally(() => state.finishing.delete(job));
    state.finishing.add(job);
    return job;
}

const REC_REASONS = {
    empty: 'غادر الجميع المكالمة',
    max: 'بلغ التسجيل أقصى مدته',
    kicked: 'أُخرجتُ من المكالمة',
    moved: 'نُقلتُ إلى مكالمة أخرى',
    lost: 'انقطع اتصالي بالمكالمة',
    fail: 'تعطّل التسجيل',
    restart: 'أُعيد تشغيلي',
};
function reasonText(reason, by) {
    if (reason === 'cmd') return `بطلب من ${by && by.name ? by.name : 'أحد الحضور'}`;
    return REC_REASONS[reason] || 'انتهى';
}

async function recFinish(r, reason, by) {
    const out = await r.audio.stop();
    const ch = state.guild && state.guild.channels.cache.get(r.channelId);
    // An event started after the recording did: look again, keep what was found first.
    const later = await meetingInfo(ch).catch(() => null);
    r.info = { ...(later || {}), ...Object.fromEntries(Object.entries(r.info || {}).filter(([, v]) => v)) };
    const team = await loadTeam();
    const stamp = fileStamp(r.t0);
    const why = reasonText(reason, by);
    const parts = out.files.map((f, i) => ({ path: f, name: out.files.length === 1 ? `${stamp}.mp3` : `${stamp}-part${i + 1}.mp3` }));
    const ctxName = `${stamp}-context.md`;
    const ctxPath = path.join(r.dir, ctxName);
    let ctxOk = false;
    try { fs.writeFileSync(ctxPath, buildContext(r, out, team, why, parts), 'utf8'); ctxOk = true; } catch (_) {}

    let sent = false;
    if (parts.length || (ctxOk && r.chat.length)) {
        try {
            const user = await withRetry(() => client.users.fetch(RECORD_TO));
            const first = [];
            if (ctxOk) first.push({ attachment: ctxPath, name: ctxName });
            if (parts[0]) first.push({ attachment: parts[0].path, name: parts[0].name });
            await withRetry(() => user.send({ content: recSummary(r, out, team, why, parts, ctxOk ? ctxName : null), files: first }));
            for (let i = 1; i < parts.length; i++) {
                await withRetry(() => user.send({ content: `الجزء ${i + 1} من ${parts.length}`, files: [{ attachment: parts[i].path, name: parts[i].name }] }));
            }
            sent = true;
        } catch (err) {
            console.error('❌ sending the recording:', err && err.message);
        }
    }

    const dur = clock(out.seconds * 1000);
    const title = recTitle(r);
    const who = `<@${r.by}>`;
    const text = sent ? `${who} انتهى تسجيل «${title}» (${dur}) — ${why}. أُرسل التسجيل.`
        : !parts.length ? `${who} انتهى التسجيل — ${why}، لكن لم يُحفظ صوت.`
        : `${who} انتهى تسجيل «${title}» (${dur}) — ${why}، لكن تعذّر إرساله.`;
    const o = { content: text, allowedMentions: { users: [r.by], repliedUser: false } };
    try { await r.cmdMsg.reply(o); }
    catch (_) {
        try { const c = await client.channels.fetch(r.cmdChannelId); if (c) await c.send(o); } catch (_) {}
    }
    if (sent) fs.rm(r.dir, { recursive: true, force: true }, () => {});
    else console.log(`📁 the recording is kept in ${r.dir} until the next restart`);
}

// Who attended, longest first: { id, name, discord, team, ms, talk }.
function recAttendees(r, out, team) {
    const talk = new Map();
    for (const g of out.turns) talk.set(g.id, (talk.get(g.id) || 0) + (g.b - g.a));
    const ids = new Set([...r.people.keys(), ...talk.keys()]);
    return [...ids].map(id => {
        const p = r.people.get(id);
        const disc = p ? p.name : memberName(id);
        return { id, name: personName(team, id, disc), discord: disc, team: team.byId.get(String(id)) || null, ms: p ? p.ms : 0, talk: talk.get(id) || 0 };
    }).sort((a, b) => b.ms - a.ms || b.talk - a.talk);
}

function recSummary(r, out, team, why, parts, ctxName) {
    const people = recAttendees(r, out, team);
    const lines = [
        `🎙️ تسجيل اجتماع: **${recTitle(r)}**`,
        `🔊 ${r.channelName} · ${fmtDay(r.t0)} · ${fmtTime(r.t0)}`,
        `⏱️ ${clock(out.seconds * 1000)} · بدأه ${personName(team, r.by, r.byName)} · ${why}`,
        `👥 الحضور (${people.length}): ${clip(people.map(p => p.name).join('، '), 900)}`,
    ];
    if (r.chat.length) lines.push(`💬 الرسائل النصية المحفوظة: ${r.chat.length}`);
    if (parts.length > 1) lines.push(`📼 الصوت مقسوم إلى ${parts.length === 2 ? 'جزأين' : `${parts.length} أجزاء`}`);
    if (ctxName) lines.push(`📄 \`${ctxName}\` — أعطِه مع الصوت لنموذج ذكاء اصطناعي ليفرّغ الكلام ويكتب المحضر.`);
    const s = lines.join('\n');
    return s.length > 1900 ? s.slice(0, 1899) + '…' : s;
}

// The file for the AI: everything it needs to transcribe the audio, tell the voices
// apart and write the minutes — and nothing the AI should not see (no emails, no ids).
function buildContext(r, out, team, why, parts) {
    const L = [];
    const title = recTitle(r);
    const people = recAttendees(r, out, team);
    const nameOf = id => (people.find(p => p.id === id) || {}).name || personName(team, id, r.hall.names.get(id));
    const info = r.info || {};

    L.push(`# تسجيل اجتماع: ${title}`, '');
    L.push('هذا الملف يرافق تسجيل الصوت. أعطِ نموذج الذكاء الاصطناعي ملف (ملفات) الصوت مع هذا الملف، ليفرّغ الكلام وينسب كل كلام إلى قائله ويكتب محضر الاجتماع. التعليمات في آخره.', '');

    L.push('## الاجتماع');
    L.push(`- العنوان: ${title}`);
    if (info.event && info.event !== title) L.push(`- فعالية ديسكورد: ${info.event}`);
    if (info.about) L.push(`- وصف الفعالية: ${chatText(info.about, 1200)}`);
    if (info.topic && info.topic !== title) L.push(`- موضوع المنصة: ${info.topic}`);
    if (r.hall.title) L.push(`- اجتماع القاعة في مقر المدونة: «${r.hall.title}»${r.stage ? '' : ' (كان قائمًا في الوقت نفسه)'}`);
    L.push(`- القناة: ${r.channelName}${r.stage ? ' (منصة Stage في ديسكورد)' : ' (قناة صوتية في ديسكورد)'}`);
    L.push(`- التاريخ: ${fmtDay(r.t0)} — ${fmtHijri(r.t0)}`);
    L.push(`- من ${fmtTime(r.t0)} إلى ${fmtTime(r.endedAt)} بتوقيت الرياض (المدة ${clock(out.seconds * 1000)})`);
    L.push(`- بدأ التسجيل: ${personName(team, r.by, r.byName)} — وانتهى: ${why}`);
    L.push('- الفريق: فريق المدونة', '');

    L.push('## ملفات الصوت');
    if (!parts.length) L.push('- لم يُحفظ صوت.');
    parts.forEach((p, i) => {
        const a = i * out.segSeconds, b = Math.min(out.seconds, (i + 1) * out.segSeconds);
        L.push(`- \`${p.name}\`: من ${clock(a * 1000)} إلى ${clock(b * 1000)}`);
    });
    L.push('- الصوت مزيج واحد لكل المتحدثين (أحادي، 24 kHz، MP3). كل الأوقات في هذا الملف من بداية التسجيل؛ لجزء بعد الأول اطرح وقت بدايته.', '');

    L.push('## الحضور');
    if (!people.length) L.push('لا أحد.');
    else {
        L.push('| الاسم | الدور في الفريق | الأقسام | مدة الحضور | مدة الكلام |', '|---|---|---|---|---|');
        for (const p of people) {
            const n = p.name + (p.discord && p.discord !== p.name ? ` (في ديسكورد: ${p.discord})` : '');
            L.push(`| ${mdCell(n)} | ${mdCell(p.team && p.team.role)} | ${mdCell(p.team && p.team.depts.join('، '))} | ${clock(p.ms)} | ${clock(p.talk * 1000)} |`);
        }
    }
    const hallOnly = [...r.hall.names.keys()].filter(id => !r.people.has(id)).map(id => personName(team, id, r.hall.names.get(id)));
    if (hallOnly.length) L.push('', `حضروا القاعة في مقر المدونة دون المكالمة: ${hallOnly.join('، ')}.`);
    L.push('');

    L.push('## من تكلّم متى');
    L.push('كل سطر فترة كلام متصلة لشخص واحد (ساعة:دقيقة:ثانية من بداية التسجيل). سطران متداخلان = تكلّما في الوقت نفسه. مأخوذ من بثّ كل شخص على حدة، فهو موثوق لمعرفة القائل.');
    if (!out.turns.length) L.push('', 'لم يتكلم أحد.');
    else {
        L.push('', '```text');
        for (const g of out.turns.slice(0, 6000)) L.push(`${clock(g.a * 1000)} – ${clock(g.b * 1000)}  ${nameOf(g.id)}`);
        L.push('```');
    }
    L.push('');

    L.push('## المحادثة النصية أثناء الاجتماع');
    if (!r.chatOn) L.push('- لم تُحفظ رسائل ديسكورد (البوت لا يملك إذن قراءة الرسائل).');
    const groups = new Map();
    for (const e of r.chat) { if (!groups.has(e.src)) groups.set(e.src, []); groups.get(e.src).push(e); }
    const srcName = src => {
        if (src === 'hall') return `تعليقات القاعة في مقر المدونة${r.hall.title ? ` («${r.hall.title}»)` : ''}`;
        if (src === r.channelId) return `محادثة المكالمة نفسها (${r.channelName})`;
        const c = state.guild && state.guild.channels.cache.get(src);
        return `#${c ? c.name : src} (القناة التي طُلب فيها التسجيل)`;
    };
    if (!r.chat.length) L.push('- لم يُكتب شيء أثناء التسجيل.');
    for (const [src, list] of groups) {
        L.push('', `### ${srcName(src)}`);
        for (const e of list) {
            const who = src === 'hall' ? personName(team, e.uid, r.hall.names.get(e.uid)) : personName(team, e.uid, e.name);
            let line = `- [${clock(e.t)}] ${who}: ${e.text || ''}`.trimEnd();
            if (e.reply) line += ' (ردًّا على رسالة سابقة)';
            for (const f of e.files) line += ` [مرفق: ${f}]`;
            L.push(line);
        }
    }
    if (r.chat.length >= REC_CHAT_MAX) L.push('', `(حُفظت أول ${REC_CHAT_MAX} رسالة فقط.)`);
    L.push('');

    if (team.list.length) {
        L.push('## أسماء الفريق (لكتابتها كما ينبغي)');
        L.push(team.list.map(t => t.name + (t.role ? ` (${t.role})` : '')).join('، ') + '.', '');
    }

    L.push('## المطلوب من النموذج');
    L.push('1. فرّغ الكلام كاملًا كما قيل. الكلام عربي وقد يكون بلهجة عامية ويتخلله إنجليزي: اكتب العامية كما نُطقت ولا تحوّلها إلى الفصحى.');
    L.push('2. انسب كل مقطع إلى قائله بجدول «من تكلّم متى»، واكتب الأسماء كما في قائمة الفريق. إن لم يتّضح القائل أو تداخل اثنان فاكتب [غير واضح].');
    L.push('3. ثم اكتب المحضر: ملخص في بضعة أسطر، القرارات، المهام (من؟ ماذا؟ متى إن ذُكر موعد)، الأسئلة المعلّقة، والمواعيد القادمة.');
    L.push('4. استعن بالمحادثة النصية (الروابط والملاحظات والاقتراحات) في موضعها من الوقت.');
    L.push('5. لا تضف شيئًا لم يُقل، ولا تخمّن الأرقام والمواعيد.', '');

    const names = [...new Set(people.map(p => p.name))].slice(0, 25);
    L.push('## نص تمهيدي قصير لنموذج التفريغ');
    L.push('(نماذج مثل Whisper تقبل نصًا تمهيديًا قصيرًا يحسّن كتابة الأسماء.)', '');
    L.push(`> اجتماع فريق المدونة: «${title}». المتحدثون: ${names.join('، ') || '—'}. مقر المدونة، ليمو.`);
    return L.join('\n') + '\n';
}

// ── Discord ───────────────────────────────────────────────────────────────────
// GuildMessages (not privileged) is for the mention summon — a message that mentions
// the bot arrives with its text even without the Message Content intent. That intent
// (privileged — Developer Portal → Bot → Privileged Gateway Intents) is asked for so
// a recording can keep the chat; if the portal refuses it, the bot logs in again
// without it (contentFallback) and records audio only. CHAT_LOG=0 skips asking.
let client = null;

function onReady() {
    console.log(`🤖 ${VERSION} logged in as ${client.user.tag}${state.chatOn ? '' : ' (no chat logs)'}`);
    state.guild = client.guilds.cache.first() || null;
    if (!state.guild) { console.error('❌ No guild found — is the bot in the server?'); return; }
    console.log(`📡 Watching voice in: ${state.guild.name}`);
    recLook(false);   // a restart mid-recording must not leave «🔴» on its name
    scheduleRescan();
}

function onVoiceState(oldState, newState) {
    if (!state.guild) return;
    if (oldState.id === client.user.id || newState.id === client.user.id) {
        // Disconnected by hand (right-click → Disconnect): take the hint and stay out of
        // that call for a while instead of hopping straight back in. Our own leaves
        // never match — leaveVoice() clears state.channelId before this event lands.
        if (oldState.channelId && !newState.channelId && oldState.channelId === state.channelId) {
            state.kicked = { channelId: oldState.channelId, until: Date.now() + KICK_COOLDOWN_MS };
            if (state.pin && state.pin.channelId === oldState.channelId) state.pin = null;
            console.log(`🚪 disconnected by hand from ${state.channelName} — staying out for ${KICK_COOLDOWN_MS / 60000} min`);
            if (state.rec && state.rec.live && state.rec.channelId === oldState.channelId) recStop('kicked');
            leaveVoice();
            scheduleRescan();
        }
        // Dragged into another call by hand: go along with it (the voice connection
        // already has), and don't follow the table back into the old one for a while.
        else if (oldState.channelId && newState.channelId && oldState.channelId !== newState.channelId
                 && oldState.channelId === state.channelId) {
            if (state.rec && state.rec.live && state.rec.channelId === oldState.channelId) recStop('moved');
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
    // Someone left mid-sentence: make sure their green ring goes out.
    if (oldState.channelId === state.channelId && newState.channelId !== state.channelId) speakOff(oldState.id);
    recVoiceState(oldState, newState);
    scheduleRescan();
}

// @MdwnhBot (the user or its role — Discord's autocomplete offers both) → come to the
// caller's call, whatever the site is doing. No reply text: a 🎧 reaction means it's
// coming, 🤷 means the caller isn't in a call. «@… سجل» / «@… انهاء التسجيل» are the
// recording's commands; while recording, other messages in its chats are logged.
function onMessage(msg) {
    if (!state.guild || !msg.guild || msg.guild.id !== state.guild.id || msg.author.bot) return;
    const botRole = msg.guild.members.me && msg.guild.members.me.roles.botRole;
    const pinged = msg.mentions.users.has(client.user.id)
        || (botRole && msg.mentions.roles.has(botRole.id));
    const text = cmdText(msg);
    const r = state.rec;
    const inRecChat = r && (msg.channelId === r.channelId || msg.channelId === r.cmdChannelId);
    if (text && (pinged ? REC_STOP_RE.test(text) : (inRecChat && REC_STOP_BARE_RE.test(text)))) { recStopCommand(msg); return; }
    if (pinged && text && REC_START_RE.test(text)) { recStartCommand(msg); return; }
    recLogDiscord(msg);
    if (!pinged || state.rec) return;   // while recording it stays in that call
    const ch = msg.member && msg.member.voice ? msg.member.voice.channel : null;
    if (!voiceOk(ch)) { msg.react('🤷').catch(() => {}); return; }
    state.pin = { channelId: ch.id, by: msg.author.id };
    if (state.kicked && state.kicked.channelId === ch.id) state.kicked = null;
    state.leaveAt = 0;
    console.log(`📣 summoned to ${ch.name} by ${msg.author.tag}`);
    msg.react('🎧').catch(() => {});
    scheduleRescan(0);
}

const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;
if (!DISCORD_TOKEN) {
    console.error('❌ DISCORD_BOT_TOKEN not found in environment variables!');
    process.exit(1);
}
const isDisallowed = err => /disallowed intents|DisallowedIntents|4014/i
    .test(`${err && err.message} ${err && err.code}`);
function makeClient(withContent) {
    const intents = [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates, GatewayIntentBits.GuildMessages];
    if (withContent) intents.push(GatewayIntentBits.MessageContent);
    const c = new Client({ intents });
    c.once(Events.ClientReady, onReady);
    c.on(Events.VoiceStateUpdate, onVoiceState);
    c.on(Events.MessageCreate, onMessage);
    c.on(Events.ShardDisconnect, ev => { if (withContent && ev && ev.code === 4014) contentFallback(); });
    c.on('error', err => {
        if (withContent && isDisallowed(err)) contentFallback();
        else console.error('❌ Discord client error:', err);
    });
    return c;
}
let fellBack = false;
function contentFallback() {
    if (fellBack) return;
    fellBack = true;
    console.warn('⚠️ Message Content intent is not enabled (Developer Portal → Bot → Privileged Gateway Intents) — recordings will be audio only.');
    state.chatOn = false;
    const old = client;
    client = makeClient(false);
    Promise.resolve().then(() => old && old.destroy()).catch(() => {})
        .then(() => client.login(DISCORD_TOKEN))
        .catch(err => console.error('❌ Discord login failed:', err));
}
process.on('unhandledRejection', err => console.error('❌ Unhandled rejection:', err));

// A redeploy / restart mid-recording: send what was recorded before going (Render
// allows ~30 s between SIGTERM and the kill).
async function shutdown(sig) {
    if (state.rec && state.rec.live) { console.log(`⏹️ ${sig} — finishing the recording first`); recStop('restart'); }
    else state.rec = null;
    if (state.finishing.size) await Promise.race([Promise.allSettled([...state.finishing]), sleep(25000)]);
    process.exit(0);
}
process.once('SIGTERM', () => shutdown('SIGTERM'));
process.once('SIGINT', () => shutdown('SIGINT'));

client = makeClient(state.chatOn);
client.login(DISCORD_TOKEN).catch(err => {
    if (state.chatOn && isDisallowed(err)) contentFallback();
    else console.error('❌ Discord login failed:', err);
});
