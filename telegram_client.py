from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, PhoneCodeInvalidError, SessionPasswordNeededError
import asyncio
import logging
import time
import os

logger = logging.getLogger(__name__)

# Session storage directory
SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

async def add_telegram_account(phone, api_id, api_hash, user_id, code_callback=None, password_callback=None):
    """
    Add a new Telegram account and get session string
    """
    try:
        session_file = f"{SESSIONS_DIR}/user_{user_id}_{phone.replace('+', '')}"
        client = TelegramClient(session_file, api_id, api_hash)
        await client.connect()

        if not await client.is_user_authorized():
            # Send code request
            await client.send_code_request(phone)
            logger.info(f"Verification code sent to {phone}")

            # Wait for code input (in production, this would be via web)
            if code_callback:
                code = await code_callback()
            else:
                # For demo, return None
                logger.warning(f"Need verification code for {phone}")
                await client.disconnect()
                return None

            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                # 2FA enabled
                if password_callback:
                    password = await password_callback()
                    await client.sign_in(password=password)
                else:
                    logger.warning(f"2FA password needed for {phone}")
                    await client.disconnect()
                    return None

        # Get session string
        session_string = client.session.save()
        await client.disconnect()
        return session_string

    except PhoneCodeInvalidError:
        logger.error(f"Invalid code for {phone}")
        return None
    except Exception as e:
        logger.error(f"Error adding account {phone}: {e}")
        return None

async def get_user_groups(session_string, api_id, api_hash):
    """
    Get all groups a user is member of
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        groups = []
        async for dialog in client.iter_dialogs():
            if dialog.is_group:
                groups.append({
                    'id': dialog.id,
                    'name': dialog.name,
                    'title': dialog.title,
                    'username': getattr(dialog.entity, 'username', None),
                    'participants_count': getattr(dialog.entity, 'participants_count', 0),
                    'megagroup': getattr(dialog.entity, 'megagroup', False)
                })

        await client.disconnect()
        return groups
    except Exception as e:
        logger.error(f"Error getting groups: {e}")
        return []

async def broadcast_message(session_string, api_id, api_hash, message_type, content, group_ids, delay):
    """
    Broadcast message to selected groups
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        sent_count = 0
        errors = []

        for group_id in group_ids:
            try:
                if message_type == 'text':
                    await client.send_message(int(group_id), content)
                elif message_type == 'photo':
                    # Check if content is URL or file path
                    if content.startswith('http'):
                        await client.send_file(int(group_id), content)
                    else:
                        await client.send_file(int(group_id), content)
                elif message_type == 'video':
                    if content.startswith('http'):
                        await client.send_file(int(group_id), content)
                    else:
                        await client.send_file(int(group_id), content)

                sent_count += 1
                await asyncio.sleep(delay)

            except FloodWaitError as e:
                logger.warning(f"Flood wait: {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                continue
            except Exception as e:
                errors.append(str(e))
                logger.error(f"Error sending to group {group_id}: {e}")

        await client.disconnect()
        return {'success': True, 'sent_count': sent_count, 'errors': errors}

    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        return {'success': False, 'error': str(e)}

async def report_content(session_string, api_id, api_hash, target_url, report_type, custom_text):
    """
    Report channel or post
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        # Get the entity
        entity = await client.get_entity(target_url)

        # Different report types
        report_reasons = {
            'spam': 'spam',
            'violence': 'violence',
            'scam': 'scam',
            'copyright': 'copyright_infringement'
        }

        reason = report_reasons.get(report_type, 'spam')

        # Report the entity
        # Note: Actual reporting requires specific API methods
        # This is a simplified version
        result = await client(functions.messages.ReportRequest(
            peer=entity,
            id=[entity.id],
            reason=reason,
            message=custom_text
        ))

        await client.disconnect()
        return {'success': True, 'message': f'Reported {target_url} for {report_type}'}

    except Exception as e:
        logger.error(f"Error reporting: {e}")
        return {'success': False, 'error': str(e)}

async def join_groups(session_string, api_id, api_hash, invite_links):
    """
    Join groups via invite links
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        joined = 0
        failed = []

        for link in invite_links:
            try:
                # Extract username or join link
                if '/joinchat/' in link or '/+' in link:
                    await client.join_channel(link)
                else:
                    username = link.split('/')[-1]
                    entity = await client.get_entity(username)
                    await client.join_channel(entity)

                joined += 1
                await asyncio.sleep(2)

            except FloodWaitError as e:
                logger.warning(f"Flood wait: {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                failed.append({'link': link, 'error': str(e)})
                logger.error(f"Error joining {link}: {e}")

        await client.disconnect()
        return {'success': True, 'joined': joined, 'failed': failed}

    except Exception as e:
        logger.error(f"Error joining groups: {e}")
        return {'success': False, 'error': str(e)}

async def scrape_group_members(session_string, api_id, api_hash, group_link, limit=100):
    """
    Scrape members from a group
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        # Get the group entity
        entity = await client.get_entity(group_link)

        members = []
        async for user in client.iter_participants(entity, limit=limit):
            member_data = {
                'id': user.id,
                'username': user.username,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'phone': user.phone,
                'online': getattr(user.status, 'was_online', None) is not None if hasattr(user, 'status') else False
            }
            members.append(member_data)

        await client.disconnect()
        return {'success': True, 'members': members, 'count': len(members)}

    except Exception as e:
        logger.error(f"Error scraping members: {e}")
        return {'success': False, 'error': str(e)}

async def add_members_to_group(session_string, api_id, api_hash, target_group, members, delay=5, daily_limit=50):
    """
    Add scraped members to own group
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        # Get the target group entity
        entity = await client.get_entity(target_group)

        added = 0
        failed = []

        for i, member in enumerate(members[:daily_limit]):
            try:
                # Try to add by username first
                if member.get('username'):
                    await client(functions.channels.InviteToChannelRequest(
                        channel=entity,
                        users=[member['username']]
                    ))
                    added += 1

                elif member.get('id'):
                    # Add by user ID
                    user = await client.get_entity(member['id'])
                    await client(functions.channels.InviteToChannelRequest(
                        channel=entity,
                        users=[user]
                    ))
                    added += 1

                await asyncio.sleep(delay)

            except FloodWaitError as e:
                logger.warning(f"Flood wait: {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                failed.append({'member': member, 'error': str(e)})
                logger.error(f"Error adding member: {e}")

        await client.disconnect()
        return {'success': True, 'added': added, 'failed': failed}

    except Exception as e:
        logger.error(f"Error adding members: {e}")
        return {'success': False, 'error': str(e)}

async def send_direct_messages(session_string, api_id, api_hash, usernames, message, delay=3):
    """
    Send bulk DMs to scraped usernames
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        sent = 0
        failed = []

        for username in usernames:
            try:
                if username:
                    await client.send_message(username, message)
                    sent += 1
                    await asyncio.sleep(delay)

            except FloodWaitError as e:
                logger.warning(f"Flood wait: {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                failed.append({'username': username, 'error': str(e)})
                logger.error(f"Error sending to {username}: {e}")

        await client.disconnect()
        return {'success': True, 'sent': sent, 'failed': failed}

    except Exception as e:
        logger.error(f"Error sending DMs: {e}")
        return {'success': False, 'error': str(e)}

async def search_groups_by_keyword(session_string, api_id, api_hash, keyword, limit=50):
    """
    Search for public groups by keyword
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        groups = []
        async for dialog in client.iter_dialogs():
            if dialog.is_group and keyword.lower() in dialog.name.lower():
                groups.append({
                    'id': dialog.id,
                    'name': dialog.name,
                    'title': dialog.title,
                    'username': getattr(dialog.entity, 'username', None),
                    'participants_count': getattr(dialog.entity, 'participants_count', 0)
                })
                if len(groups) >= limit:
                    break

        await client.disconnect()
        return {'success': True, 'groups': groups}

    except Exception as e:
        logger.error(f"Error searching groups: {e}")
        return {'success': False, 'error': str(e)}

async def get_account_info(session_string, api_id, api_hash):
    """
    Get account information
    """
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()

        me = await client.get_me()

        info = {
            'id': me.id,
            'username': me.username,
            'first_name': me.first_name,
            'last_name': me.last_name,
            'phone': me.phone,
            'is_bot': me.bot
        }

        await client.disconnect()
        return {'success': True, 'info': info}

    except Exception as e:
        logger.error(f"Error getting account info: {e}")
        return {'success': False, 'error': str(e)}