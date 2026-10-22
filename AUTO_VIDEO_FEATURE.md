# Auto Video Poster — Source & Timer

## Sources
1. Multiple ZIP files: upload one or more ZIPs. Supported video files are extracted into the campaign queue.
2. Telegram Saved Messages: select a Telegram account and the app scans that account's Saved Messages for video messages. It stores only Telegram message IDs and sends the media from Telegram; it does not copy all Saved Message videos into Railway storage.

## Channels
- Up to 10 channels per campaign.
- Each channel has an independent shuffled queue.
- When possible, a posting round uses different videos for different channels.
- When a channel finishes its queue, it automatically creates a new shuffled cycle and continues forever.

## Caption
The configured caption is attached to each video. Supported placeholders:
- `{filename}`
- `{name}`
- `{index}`
- `{total}`

## Timer
- Minimum and maximum delay are configurable from 1 to 1440 minutes.
- Example: 5–15 means a random delay between 5 and 15 minutes after each posting round.
- 10–10 means exactly 10 minutes.

## Important
The Telegram account used by the campaign must have permission to post in the selected channels. The Saved Messages source uses the same connected Telegram account.
