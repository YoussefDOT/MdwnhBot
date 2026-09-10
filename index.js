// MdwnhBot — tells مقر المدونة who is talking in the Discord call.
// -----------------------------------------------------------------------------
// The bot sits (muted) in the voice channel people are meeting in, listens for
// Discord's "speaking" events, and relays them into the site's WebSocket relay
// (the same Cloudflare rooms the players' live positions travel through). The
// site draws the green outline around whoever is talking at the meeting table
// and fades out whoever is in the call but quiet.
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
// A live speaker is re-asserted every 4 s so a lost 'off' can't stick for long.
// -----------------------------------------------------------------------------
require('dotenv').config();
const http = require('http');
const WebSocket = require('ws');
const { Client, GatewayIntentBits, ChannelType, Events } = require('discord.js');
const {
    joinVoiceChannel, getVoiceConnection, VoiceConnectionStatus, entersState,
} = require('@discordjs/voice');

const VERSION = 'v2-voice';

// ── Config ───────────────────────────────────────────────────────────────────
const RELAY_BASE = process.env.RELAY_URL || 'wss://mdwnh-presence.yosefbore3y.workers.dev';
const RELAY_UID = 'mdwnhbot';
// Discord category → site lobby. A channel under neither talks to both lobbies.
const LOBBY_CATEGORIES = { male: 'صالة السادة', female: 'صالة السيدات' };
// Optional: comma-separated voice channel ids the bot may sit in. Empty = it follows
// whichever voice channel has the most people in it.
const MEETING_CHANNEL_IDS = (process.env.MEETING_CHANNEL_IDS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
const HEARTBEAT_MS = 15000;
const SPEAK_REASSERT_MS = 4000;
const SPEAK_OFF_GRACE_MS = 250;     // Discord ends a "speaking" burst on every short pause
const RESCAN_DEBOUNCE_MS = 2000;

const state = {
    guild: null,
    channelId: null,
    channelName: null,
    lobbies: [],
    speaking: new Map(),            // discordId → { on, offTimer }
};

// ── Health check (Render needs a bound port) + keep-alive ─────────────────────
http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(`MdwnhBot ${VERSION} alive — voice: ${state.channelName || 'idle'}`);
}).listen(process.env.PORT || 3000);

const RENDER_URL = process.env.RENDER_EXTERNAL_URL;
if (RENDER_URL) {
    setInterval(() => { fetch(RENDER_URL).catch(() => {}); }, 10 * 60 * 1000);
}

// ── Relay sockets, one per lobby room, open only while the bot is in a call ────
const relays = {};   // lobby → { ws, retry, want }

function relayOpen(lobby) {
    const r = relays[lobby] || (relays[lobby] = { ws: null, retry: null, want: true });
    r.want = true;
    if (r.ws && (r.ws.readyState === WebSocket.OPEN || r.ws.readyState === WebSocket.CONNECTING)) return;
    const url = `${RELAY_BASE}/lobby/${encodeURIComponent(lobby)}?uid=${RELAY_UID}`;
    let ws;
    try { ws = new WebSocket(url); } catch (err) { scheduleRelayRetry(lobby); return; }
    r.ws = ws;
    ws.on('open', () => { console.log(`🔌 relay open: ${lobby}`); sendHeartbeat(); });
    // Every player's position flows through the room too — the bot has no use for it.
    ws.on('message', () => {});
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

// ── Which channel to sit in ───────────────────────────────────────────────────
function pickChannel(guild) {
    const voice = guild.channels.cache.filter(c =>
        c.isVoiceBased() && c.type !== ChannelType.GuildStageVoice && c.id !== guild.afkChannelId
        && (!MEETING_CHANNEL_IDS.length || MEETING_CHANNEL_IDS.includes(c.id)));
    // Stay put while anyone is still here — never hop mid-meeting.
    if (state.channelId) {
        const cur = voice.get(state.channelId);
        if (cur && humansIn(cur).size > 0) return cur;
    }
    let best = null, most = 0;
    for (const c of voice.values()) {
        const n = humansIn(c).size;
        if (n > most) { best = c; most = n; }
    }
    return best;
}

function lobbiesFor(channel) {
    const cat = channel.parent ? channel.parent.name : null;
    const hit = Object.entries(LOBBY_CATEGORIES).find(([, name]) => name === cat);
    return hit ? [hit[0]] : Object.keys(LOBBY_CATEGORIES);
}

function leaveVoice() {
    if (!state.guild) return;
    // Tell the site the call is over before the sockets go.
    clearSpeaking();
    relaySend({ t: 'vc', uid: RELAY_UID, ids: [], sp: [] });
    const conn = getVoiceConnection(state.guild.id);
    if (conn) { try { conn.destroy(); } catch (_) {} }
    if (state.channelId) console.log(`👋 left voice: ${state.channelName}`);
    state.channelId = null;
    state.channelName = null;
    const was = state.lobbies;
    state.lobbies = [];
    for (const l of was) relayClose(l);
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
    // those packets — so: muted, NOT deafened. It never subscribes to or decodes
    // anyone's audio; it only sees that packets are arriving.
    const conn = joinVoiceChannel({
        channelId: channel.id,
        guildId: guild.id,
        adapterCreator: guild.voiceAdapterCreator,
        selfDeaf: false,
        selfMute: true,
    });
    conn.receiver.speaking.on('start', id => speakOn(id));
    conn.receiver.speaking.on('end', id => speakOff(id));
    conn.on(VoiceConnectionStatus.Ready, () => { console.log('✅ voice ready'); sendHeartbeat(); });
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

let rescanTimer = null;
function scheduleRescan() {
    if (rescanTimer) return;
    rescanTimer = setTimeout(() => {
        rescanTimer = null;
        if (!state.guild) return;
        const ch = pickChannel(state.guild);
        if (ch) joinChannel(ch); else leaveVoice();
        sendHeartbeat();
    }, RESCAN_DEBOUNCE_MS);
}

// ── Discord ───────────────────────────────────────────────────────────────────
const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates] });

client.once(Events.ClientReady, () => {
    console.log(`🤖 ${VERSION} logged in as ${client.user.tag}`);
    state.guild = client.guilds.cache.first() || null;
    if (!state.guild) { console.error('❌ No guild found — is the bot in the server?'); return; }
    console.log(`📡 Watching voice in: ${state.guild.name}`);
    scheduleRescan();
});

client.on(Events.VoiceStateUpdate, (oldState, newState) => {
    if (!state.guild) return;
    if (newState.id === client.user.id || oldState.id === client.user.id) return;
    // Someone left mid-sentence: make sure their green ring goes out.
    if (oldState.channelId === state.channelId && newState.channelId !== state.channelId) speakOff(oldState.id);
    scheduleRescan();
});

client.on('error', err => console.error('❌ Discord client error:', err));
process.on('unhandledRejection', err => console.error('❌ Unhandled rejection:', err));

const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;
if (!DISCORD_TOKEN) {
    console.error('❌ DISCORD_BOT_TOKEN not found in environment variables!');
    process.exit(1);
}
client.login(DISCORD_TOKEN);
