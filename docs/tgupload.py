"""
Handles uploading files (zipped and unzipped) to Telegram via Pyrogram,
including progress monitoring, rate limiting, and dump channel logic.
"""

import os
import asyncio
import time
import math
import json
from typing import Optional, Dict, List
from pyrogram import Client, enums
from pyrogram.errors import FloodWait, PeerIdInvalid, UserAlreadyParticipant
from pyrogram.types import InputMediaAudio

# Import helpers from the utils module
from .utils import (
    DownloadCancelledError, 
    get_metadata, 
    get_audio_metadata, 
    format_bytes, 
    format_speed,
    sanitize_cover_art
)

async def get_metadata_robust(file_path: str):
    """
    Extracts metadata using FFprobe. Robust for OPUS/OGG files.
    Returns: (year, album, artist, title)
    """
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", file_path
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        
        data = json.loads(stdout)
        tags = data.get('format', {}).get('tags', {})
        
        if not tags:
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'audio':
                    tags = stream.get('tags', {})
                    break

        def get_tag(keys, default):
            for k in keys:
                for tag_k, tag_v in tags.items():
                    if tag_k.lower() == k.lower():
                        return tag_v
            return default

        title = get_tag(['title', 'name', 'song'], "Unknown Title")
        artist = get_tag(['artist', 'performer', 'album_artist', 'author'], "Unknown Artist")
        album = get_tag(['album', 'album_title'], "Unknown Album")
        year = get_tag(['date', 'year', 'originaldate', 'creation_time', 'tyer'], "")

        return year, album, artist, title
    except Exception as e:
        print(f"Robust metadata extraction failed in upload: {e}")
        return "", "Unknown Album", "Unknown Artist", "Unknown Title"

async def get_duration_robust(file_path: str) -> int:
    """
    Extracts duration using FFprobe. Essential for OPUS files.
    """
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", file_path
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        data = json.loads(stdout)
        
        duration = data.get('format', {}).get('duration')
        if not duration:
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'audio':
                    duration = stream.get('duration')
                    break
        
        if duration:
            return int(float(duration))
        return 0
    except Exception as e:
        print(f"Robust duration extraction failed: {e}")
        return 0


async def _check_and_refresh_chat_access(pyrogram_app: Client, ptb_bot, chat_id: int):
    """
    Internal helper to fix PeerIdInvalid.
    Checks if the Pyrogram user can see the chat. If not, it refreshes
    the cache. If it still can't, it tries to join the chat.
    """
    try:
        await pyrogram_app.get_chat(chat_id)
    except PeerIdInvalid:
        print(f"Pyrogram user cannot see chat {chat_id} in cache. Forcing full dialog refresh...")
        try:
            await pyrogram_app.get_dialogs()
            print("Dialog refresh complete. Retrying get_chat...")
            await pyrogram_app.get_chat(chat_id)
        except PeerIdInvalid:
            print(f"Cache refresh failed. Pyrogram user is not in chat {chat_id}. Attempting to join...")
            try:
                link = await ptb_bot.create_chat_invite_link(chat_id, member_limit=1)
                await pyrogram_app.join_chat(link.invite_link)
                print(f"Pyrogram user successfully joined chat {chat_id}")
                await pyrogram_app.get_chat(chat_id)
            except UserAlreadyParticipant:
                print(f"Pyrogram user is already in chat {chat_id}.")
                try:
                    await pyrogram_app.get_chat(chat_id)
                except PeerIdInvalid:
                     print("Cache is still stale even after UserAlreadyParticipant. Forcing dialogs again.")
                     await pyrogram_app.get_dialogs() 
            except Exception as join_error:
                print(f"CRITICAL: Pyrogram user failed to join chat {chat_id}. {join_error}")
                raise Exception(f"Bot user (Pyrogram) failed to join this group. Please add the user manually.")
        except Exception as e:
            print(f"CRITICAL: Failed to refresh chat {chat_id}. {e}")
            raise e


async def upload_to_telegram(
    ptb_bot, 
    pyrogram_app: Client, 
    file_path: str, 
    caption: str, 
    pyro_caption: str, 
    chat_id: int, 
    thread_id: int, 
    reply_to_message_id: int, 
    dl_id: str, 
    cancel_event: asyncio.Event, 
    thumbnail_path: Optional[str],
    download_tasks_lock,
    download_registry: Dict,
    TELEGRAM_UPLOAD_SPEED_LIMIT_MBPS: float, 
    DUMP_CHANNEL_ID: str 
) -> bool:
    """
    Uploads a single (zipped) file DIRECTLY to the user chat.
    """
    last_check_time, last_bytes_sent = time.time(), 0
    
    async def progress_callback(current, total):
        nonlocal last_check_time, last_bytes_sent
        if cancel_event.is_set():
            await pyrogram_app.stop_transmission()
            raise DownloadCancelledError(f"Upload `{dl_id}` cancelled.")

        current_time = time.time()
        elapsed_time = current_time - last_check_time
        if elapsed_time < 2.5 and current != total:
             return 
        speed = (current - last_bytes_sent) / elapsed_time if elapsed_time > 0 else 0
        progress_speed_text = format_speed(speed)
        last_bytes_sent, last_check_time = current, current_time
        
        percentage = (current / total) * 100
        progress_stats_text = f"{format_bytes(current)} / {format_bytes(total)} ({percentage:.1f}%)"
        
        async with download_tasks_lock:
            if dl_id in download_registry:
                download_registry[dl_id]['progress_stats'] = progress_stats_text
                download_registry[dl_id]['progress_speed'] = progress_speed_text

    try:
        await _check_and_refresh_chat_access(pyrogram_app, ptb_bot, chat_id)
    except Exception as e:
        print(f"Error during pre-upload chat check for {dl_id}: {e}")
        return False

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # ✅ IMPORTANT: We do NOT set 'status' = 'uploading' here anymore
            # This allows the custom messages from core_worker.py
            # (Zipping Part 1/3, Uploading Part 1/3, Zipping Part 2/3 (in background), etc.)
            # to stay visible in the Status entry.

            # --- UPLOAD TO USER ---
            sent_msg = await pyrogram_app.send_document(
                chat_id=chat_id, 
                document=file_path,
                caption=pyro_caption, 
                parse_mode=enums.ParseMode.MARKDOWN,
                progress=progress_callback,
                thumb=thumbnail_path if thumbnail_path and os.path.exists(thumbnail_path) else None,
                reply_to_message_id=reply_to_message_id 
            )

            # --- DUMP LOGIC ADDED HERE ---
            if DUMP_CHANNEL_ID:
                try:
                    await sent_msg.copy(chat_id=DUMP_CHANNEL_ID, caption=pyro_caption)
                except Exception as e:
                    print(f"Failed to send zip to dump channel: {e}")
            # -----------------------------

            return True 

        except DownloadCancelledError:
            raise 
        except FloodWait as e:
            if attempt < max_retries - 1:
                print(f"Flood wait of {e.value} seconds. Waiting and retrying...")
                await asyncio.sleep(e.value)
                continue
            else:
                return False
        except Exception as e:
            print(f"Error during Pyrogram upload for {dl_id} on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
            else:
                return False 
    return False


async def upload_unzipped_to_telegram(
    ptb_bot, 
    pyrogram_app: Client, 
    chat_id: int, 
    thread_id: int, 
    reply_to_message_id: int, 
    caption: str, 
    pyro_caption: str, 
    cover_path: Optional[str], 
    audio_files: List[str], 
    dl_id: str, 
    num_audio_files: int, 
    cancel_event: asyncio.Event,
    download_tasks_lock,
    download_registry: Dict,
    TELEGRAM_UPLOAD_SPEED_LIMIT_MBPS: float, 
    DUMP_CHANNEL_ID: str 
):
    """
    Uploads unzipped tracks and cover art DIRECTLY to the user.
    """
    sanitized_path = None
    try:
        await _check_and_refresh_chat_access(pyrogram_app, ptb_bot, chat_id)

        async with download_tasks_lock:
            if dl_id in download_registry:
                download_registry[dl_id]['status'] = 'uploading'
        
        last_check_time, last_bytes_sent = time.time(), 0
        
        async def progress_callback(current, total):
            nonlocal last_check_time, last_bytes_sent
            if cancel_event.is_set():
                await pyrogram_app.stop_transmission()
                raise DownloadCancelledError(f"Upload `{dl_id}` cancelled.")

            current_time = time.time()
            elapsed_time = current_time - last_check_time
            if elapsed_time < 2.5 and current != total:
                 return 
            speed = (current - last_bytes_sent) / elapsed_time if elapsed_time > 0 else 0
            progress_speed_text = format_speed(speed)
            last_bytes_sent, last_check_time = current, current_time

            async with download_tasks_lock:
                if dl_id in download_registry:
                    download_registry[dl_id]['progress_speed'] = progress_speed_text

        # Logic for single track upload
        if num_audio_files == 1:
            async with download_tasks_lock:
                if dl_id in download_registry:
                    download_registry[dl_id]['progress_stats'] = "Uploading single track..."
            file_path = audio_files[0]
            
            _, _, album_artist_from_metadata, title = get_metadata(file_path)
            
            if "Unknown" in (title or "") or "Unknown" in (album_artist_from_metadata or "") or file_path.lower().endswith(('.opus', '.ogg')):
                 r_year, r_album, r_artist, r_title = await get_metadata_robust(file_path)
                 if r_title and r_title != "Unknown Title": title = r_title
                 if r_artist and r_artist != "Unknown Artist": album_artist_from_metadata = r_artist

            _, _, _, duration = get_audio_metadata(file_path)
            if not duration or duration == 0:
                duration = await get_duration_robust(file_path)
            
            last_check_time, last_bytes_sent = time.time(), 0 
            
            # --- UPLOAD TO USER ---
            sent_msg = await pyrogram_app.send_audio(
                chat_id=chat_id,
                reply_to_message_id=reply_to_message_id, 
                audio=file_path,
                caption=pyro_caption,
                parse_mode=enums.ParseMode.MARKDOWN,
                title=title or os.path.basename(file_path),
                performer=album_artist_from_metadata,
                duration=int(duration),
                progress=progress_callback
            )

            # --- DUMP LOGIC ADDED HERE ---
            if DUMP_CHANNEL_ID:
                try:
                    await sent_msg.copy(chat_id=DUMP_CHANNEL_ID, caption=pyro_caption)
                except Exception as e:
                    print(f"Failed to send track to dump: {e}")
            # -----------------------------
            
        # Logic for Albums/Playlists
        else:
            caption_head = pyro_caption
            caption_tail = None
            if len(pyro_caption) > 1024:
                split_pos = pyro_caption[:1024].rfind('\n')
                if split_pos == -1: split_pos = 1020
                caption_head = pyro_caption[:split_pos]
                caption_tail = pyro_caption[split_pos:]

            if cover_path and os.path.exists(cover_path):
                sanitized_path = await asyncio.to_thread(sanitize_cover_art, cover_path)

                # Send Cover
                sent_cover = await pyrogram_app.send_photo(
                    chat_id=chat_id, 
                    photo=sanitized_path, 
                    caption=caption_head,
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_to_message_id=reply_to_message_id
                )
                # Dump Cover
                if DUMP_CHANNEL_ID:
                    try: await sent_cover.copy(DUMP_CHANNEL_ID, caption=caption_head)
                    except: pass
            else:
                sent_text = await pyrogram_app.send_message(
                    chat_id=chat_id, 
                    text=caption_head, 
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_to_message_id=reply_to_message_id
                )
                # Dump Text
                if DUMP_CHANNEL_ID:
                    try: await sent_text.copy(DUMP_CHANNEL_ID)
                    except: pass
            
            if caption_tail:
                await pyrogram_app.send_message(
                    chat_id=chat_id, 
                    text=caption_tail, 
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_to_message_id=reply_to_message_id
                )

            group_size = 10
            total_groups = math.ceil(num_audio_files / group_size)
            
            for i in range(0, num_audio_files, group_size):
                if cancel_event.is_set():
                     raise DownloadCancelledError(f"Upload `{dl_id}` cancelled.")

                chunk = audio_files[i:i + group_size]
                current_group_num = (i // group_size) + 1
                
                async with download_tasks_lock:
                    if dl_id in download_registry:
                        download_registry[dl_id]['progress_stats'] = f"Preparing group {current_group_num}/{total_groups}"
                        download_registry[dl_id]['progress_speed'] = "-"

                media_group = []
                for file_path in chunk:
                    _, _, track_artist, title = get_metadata(file_path)
                    
                    if "Unknown" in (title or "") or "Unknown" in (track_artist or "") or file_path.lower().endswith(('.opus', '.ogg')):
                        r_year, r_album, r_artist, r_title = await get_metadata_robust(file_path)
                        if r_title and r_title != "Unknown Title": title = r_title
                        if r_artist and r_artist != "Unknown Artist": track_artist = r_artist

                    _, _, _, duration = get_audio_metadata(file_path)
                    if not duration or duration == 0:
                        duration = await get_duration_robust(file_path)
                    
                    media = InputMediaAudio(
                        media=file_path,
                        title=title or os.path.basename(file_path),
                        performer=track_artist,
                        duration=int(duration)
                    )
                    media_group.append(media)

                if not media_group:
                    continue

                while True:
                    try:
                        async with download_tasks_lock:
                           if dl_id in download_registry:
                               download_registry[dl_id]['progress_stats'] = f"Uploading group {current_group_num}/{total_groups}"

                        # --- SEND GROUP TO USER ---
                        sent_msgs = await pyrogram_app.send_media_group(
                            chat_id=chat_id, 
                            media=media_group,
                            reply_to_message_id=reply_to_message_id
                        )

                        # --- DUMP GROUP LOGIC ---
                        if DUMP_CHANNEL_ID:
                            try:
                                msg_ids = [m.id for m in sent_msgs]
                                await pyrogram_app.forward_messages(
                                    chat_id=DUMP_CHANNEL_ID,
                                    from_chat_id=chat_id,
                                    message_ids=msg_ids
                                )
                            except Exception as e:
                                print(f"Failed to dump media group: {e}")
                        # ------------------------

                        break 
                    
                    except FloodWait as e:
                        print(f"Flood wait of {e.value} seconds for group {current_group_num}. Sleeping...")
                        async with download_tasks_lock:
                           if dl_id in download_registry:
                               download_registry[dl_id]['progress_stats'] = f"Waiting ({e.value}s) | Group {current_group_num}"
                        await asyncio.sleep(e.value)
                    
                    except Exception as e:
                        print(f"Failed to upload group {current_group_num}: {e}. Skipping.")
                        break

            
        return True
    
    except Exception as e:
        print(f"Error during unzipped upload for {dl_id}: {e}")
        try:
            await ptb_bot.send_message(
                chat_id=chat_id, 
                text=f"❌ An error occurred during file upload.",
                reply_to_message_id=reply_to_message_id
            )
        except: pass
        return False
    
    finally:
        if sanitized_path and sanitized_path != cover_path and os.path.exists(sanitized_path):
            try:
                os.remove(sanitized_path)
            except Exception as e:
                pass