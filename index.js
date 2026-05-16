require('dotenv').config();
const { Client, GatewayIntentBits } = require('discord.js');
// Firebase Configuration (Public Database)
const DATABASE_URL = "https://mdwnh-digital-s-default-rtdb.europe-west1.firebasedatabase.app/";


// Discord client setup
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildVoiceStates,
    GatewayIntentBits.GuildMembers
  ]
});

// Store current voice channel users
const currentUsers = new Map();

// Function to scan all voice channels and update Firebase
async function scanVoiceChannels(guild) {
  const usersInVoice = new Map();
  
  // Iterate through all voice channels
  guild.channels.cache.forEach(channel => {
    if (channel.isVoiceBased()) {
      channel.members.forEach(member => {
        // Use Discord username (not display name or server nickname)
        const username = member.user.username;
        
        usersInVoice.set(member.id, {
          username: username,
          channelName: channel.name,
          channelId: channel.id,
          userId: member.id,
          avatar: member.user.displayAvatarURL(),
          joinedAt: Date.now()
        });
      });
    }
  });

  // Update Firebase with current state
  const updates = {};
  
  // Add/update users currently in voice
  usersInVoice.forEach((userData, userId) => {
    updates[`users/${userId}`] = {
      username: userData.username,
      channelName: userData.channelName,
      channelId: userData.channelId,
      avatar: userData.avatar,
      status: 'in-voice',
      lastSeen: { ".sv": "timestamp" }
    };
  });

  // Mark users who left as offline
  currentUsers.forEach((userData, userId) => {
    if (!usersInVoice.has(userId)) {
      updates[`users/${userId}`] = {
        username: userData.username,
        avatar: userData.avatar,
        status: 'offline',
        lastSeen: { ".sv": "timestamp" }
      };
    }
  });

  // Perform atomic update via REST API
  try {
    const response = await fetch(`${DATABASE_URL}.json`, {
      method: 'PATCH',
      body: JSON.stringify(updates),
      headers: { 'Content-Type': 'application/json' }
    });

    if (!response.ok) {
      throw new Error(`Firebase REST error: ${response.status} ${response.statusText}`);
    }

    console.log(`✅ Updated ${usersInVoice.size} users in voice channels`);
  } catch (error) {
    console.error('❌ Firebase update error:', error);
  }

  // Update local cache
  currentUsers.clear();
  usersInVoice.forEach((userData, userId) => {
    currentUsers.set(userId, userData);
  });
}

// Bot ready event
client.once('ready', async () => {
  console.log(`🤖 Bot logged in as ${client.user.tag}`);
  
  // Get the first guild (server) - modify if you have multiple servers
  const guild = client.guilds.cache.first();
  
  if (!guild) {
    console.error('❌ No guild found! Make sure the bot is added to a server.');
    return;
  }

  console.log(`📡 Monitoring voice channels in: ${guild.name}`);
  
  // Initial scan
  await scanVoiceChannels(guild);
  
  // Scan every 10 seconds for changes
  setInterval(() => scanVoiceChannels(guild), 10000);
});

// Listen for voice state updates (instant detection)
client.on('voiceStateUpdate', async (oldState, newState) => {
  const guild = newState.guild || oldState.guild;
  
  // Immediate scan when someone joins/leaves/moves
  await scanVoiceChannels(guild);
});

// Error handling
client.on('error', error => {
  console.error('❌ Discord client error:', error);
});

process.on('unhandledRejection', error => {
  console.error('❌ Unhandled promise rejection:', error);
});

// Login to Discord
const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;

if (!DISCORD_TOKEN) {
  console.error('❌ DISCORD_BOT_TOKEN not found in environment variables!');
  process.exit(1);
}

client.login(DISCORD_TOKEN);
