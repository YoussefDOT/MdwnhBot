# MdwnhBot

Relays who is talking in the Discord call to مقر المدونة (see the header of `index.js`),
and records meetings on request.

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
- `…-context.md` — written for an AI to transcribe the audio and write the minutes:
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
