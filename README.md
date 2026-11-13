# MdwnhBot

Relays who is talking in the Discord call to مقر المدونة (see the header of `index.js`),
and records meetings on request.

`@ليمو` from someone in a call brings it there — **a Stage channel too, any one, not just
`STAGE_CHANNEL_ID`**. In a Stage it feeds the مقر hall (who is in it, who is a speaker,
who is talking), and a summoned Stage outranks the default one.

**الساحة's podium mic:** whoever speaks from the podium in the site is **server-muted in
Discord** while they hold the mic (only in the call the bot sits in), and unmuted when
they let go, leave the hall, or go silent for 2.5 min. It only ever unmutes members it
muted itself, and gives every mute back on shutdown. Needs the **Mute Members**
permission.

**ليمو on الساحة's stage:** pressing «استدعاء ليمو» in the site brings the bot up to
**speak in the Stage** it sits in (still self-muted); sending ليمو away puts it back in the
audience. It only ever steps down if it stepped up itself. Needs **Mute Members** on the
Stage (Stage moderator) — without it, it raises its hand instead and logs a warning.

## Recording a meeting

| In any channel, while you are in a call | |
|---|---|
| `@ليمو سجل` | comes to your call (a Stage channel too) and starts recording |
| `@ليمو انهاء التسجيل` | stops it (also «انهاء التسجيل» alone, in the chats being recorded) |

It also stops by itself when everyone has left the call (after 10 s), or after
`REC_MAX_HOURS`. While recording it says so: a reply, a line in the call's own chat,
«🔴» on its nickname and status, and «🔴 يُسجَّل» in the مقر hall when a hall meeting
is running.

When it ends, `RECORD_TO` gets a DM with:
- the call as one mixed MP3 (mono, 32 kbps; cut into 35-minute parts so each one fits
  Discord's 10 MB upload limit);
- `…-context.txt` — written for an AI to transcribe the audio and write the minutes (plain text, every line forced right-to-left so it reads correctly in any viewer):
  the meeting (Discord event name / Stage topic / the hall's title), who attended (with
  their team role), **who spoke when** (taken from each person's own stream, so it is
  reliable for telling voices apart), the chat (the channel it was asked in, the call's
  own chat, and the hall's comments if a hall meeting was live), and instructions.

Whoever asked for the recording is mentioned when it is done.

### Chat logs need one switch

Reading ordinary messages needs Discord's **Message Content Intent**:
Developer Portal → the bot's application → **Bot** → *Privileged Gateway Intents* →
turn on **Message Content Intent**. Without it the bot still runs and records audio
only (it logs a warning and says «صوت المكالمة» instead of «ومحادثتها النصية»).

### Settings (Render environment)

| | default |
|---|---|
| `RECORD_TO` | `567266235163738112` — who receives the recordings |
| `REC_MAX_HOURS` | `4` |
| `CHAT_LOG` | set to `0` to never ask for the Message Content intent |
| `FFMPEG_PATH` | the bundled `ffmpeg-static` binary, else `ffmpeg` on the PATH |

`record.js` holds the audio side: Opus decoding (`opusscript`, no native build), the
streaming mix and the MP3 encode (`ffmpeg-static`).
