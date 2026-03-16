# Authored By Certified Coders © 2025
import asyncio
import contextlib
import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple, Union

import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
import aiohttp
import urllib.parse


from AnnieXMedia.utils.cookie_handler import COOKIE_PATH
from AnnieXMedia.utils.database import is_on_off
from AnnieXMedia.utils.downloader import yt_dlp_download
from AnnieXMedia.utils.errors import capture_internal_err
from AnnieXMedia.utils.formatters import time_to_seconds
from AnnieXMedia.utils.tuning import YTDLP_TIMEOUT, YOUTUBE_META_MAX, YOUTUBE_META_TTL


# === Caches ===
_cache: Dict[str, Tuple[float, List[Dict]]] = {}
_cache_lock = asyncio.Lock()
_formats_cache: Dict[str, Tuple[float, List[Dict], str]] = {}
_formats_lock = asyncio.Lock()


# === Constants ===
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")


# === Helpers ===
def _cookies_args() -> List[str]:
    return [] # Cookies completely disabled!



async def _exec_proc(*args: str) -> Tuple[bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return b"", b"timeout"


@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    key = f"q:{query}"
    now = time.time()

    async with _cache_lock:
        if key in _cache:
            ts, val = _cache[key]
            if now - ts < YOUTUBE_META_TTL:
                return val
            _cache.pop(key, None)
        if len(_cache) > YOUTUBE_META_MAX:
            _cache.clear()

    # --- THE INVIDIOUS API ROTATOR ---
    INVIDIOUS_APIS = [
        "https://vid.puffyan.us",
        "https://invidious.nerdvpn.de",
        "https://inv.tux.pizza",
        "https://invidious.jing.rocks",
        "https://invidious.fdn.fr"
    ]

    try:
        safe_query = urllib.parse.quote(query)
        result = []
        async with aiohttp.ClientSession() as session:
            for base_api in INVIDIOUS_APIS:
                api_url = f"{base_api}/api/v1/search?q={safe_query}"
                try:
                    async with session.get(api_url, timeout=7) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            first_video = next((item for item in data if item.get("type") == "video"), None)
                            if first_video:
                                duration_seconds = first_video.get("lengthSeconds", 0)
                                mins, secs = divmod(duration_seconds, 60)
                                
                                # Fix proxied thumbnails
                                thumb = first_video.get("videoThumbnails", [{}])[0].get("url", "")
                                if thumb and thumb.startswith("/"):
                                    thumb = base_api + thumb
                                    
                                result = [{
                                    "id": first_video.get("videoId", ""),
                                    "title": first_video.get("title", ""),
                                    "duration": f"{mins}:{secs:02d}",
                                    "thumbnails": [{"url": thumb}]
                                }]
                                break # Success! Stop searching
                except Exception:
                    continue # Server blocked us, instantly try the next one
    except Exception:
        result = []



# === Main Class ===
class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")

    def _prepare_link(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        if isinstance(videoid, str) and videoid.strip():
            link = self.base_url + videoid.strip()

        link = link.strip()

        if "youtu.be" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]

        return link.split("&")[0]

    # === URL Handling ===
    @capture_internal_err
    async def exists(self, link: str, videoid: Union[str, bool, None] = None) -> bool:
        return bool(self._url_pattern.search(self._prepare_link(link, videoid)))

    @capture_internal_err
    async def url(self, message: Message) -> Optional[str]:
        msgs = [message] + ([message.reply_to_message] if message.reply_to_message else [])
        for msg in msgs:
            text = msg.text or msg.caption or ""
            entities = (msg.entities or []) + (msg.caption_entities or [])
            for ent in entities:
                if ent.type == MessageEntityType.URL:
                    return text[ent.offset: ent.offset + ent.length].split("&si")[0]
                if ent.type == MessageEntityType.TEXT_LINK:
                    return ent.url.split("&si")[0]
        return None

    async def _ensure_watch_url(self, maybe_query_or_url: str) -> Optional[str]:
        prepared = self._prepare_link(maybe_query_or_url)
        if prepared.startswith("http"):
            return prepared
        data = await cached_youtube_search(prepared)
        if not data:
            return None
        vid = data[0].get("id")
        return self.base_url + vid if vid else None

    # === Metadata Fetching ===
    @capture_internal_err
    async def _fetch_video_info(self, query: str, *, use_cache: bool = True) -> Optional[Dict]:
        q = self._prepare_link(query)
        res = await cached_youtube_search(q)
        return res[0] if res else None


    @capture_internal_err
    async def is_live(self, link: str) -> bool:
        prepared = self._prepare_link(link)
        stdout, _ = await _exec_proc("yt-dlp", *(_cookies_args()), "--dump-json", prepared)
        if not stdout:
            return False
        try:
            info = json.loads(stdout.decode())
            return bool(info.get("is_live"))
        except json.JSONDecodeError:
            return False

    @capture_internal_err
    async def details(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], int, str, str]:
        prepared_link = self._prepare_link(link, videoid)

        info = await self._fetch_video_info(prepared_link)
        if not info:
            raise ValueError("Video not found via Piped API")

        dt = info.get("duration")
        ds = int(time_to_seconds(dt)) if dt else 0
        thumb = info.get("thumbnails", [{}])[0].get("url", "")

        return info.get("title", ""), dt, ds, thumb.split("?")[0] if thumb else "", info.get("id", "")


    @capture_internal_err
    async def title(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return info.get("title", "") if info else ""

    @capture_internal_err
    async def duration(self, link: str, videoid: Union[str, bool, None] = None) -> Optional[str]:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return info.get("duration") if info else None

    @capture_internal_err
    async def thumbnail(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[-1].get("url", "")
        ).split("?")[0] if info else ""

    @capture_internal_err
    async def track(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[Dict, str]:
        prepared_link = self._prepare_link(link, videoid)
        
        info = await self._fetch_video_info(prepared_link)
        if not info:
            raise ValueError(f"No results from Piped API for query/URL: '{prepared_link}'")

        thumb = info.get("thumbnails", [{}])[0].get("url", "")

        details = {
            "title": info.get("title", "Unknown Title"),
            "link": f"https://www.youtube.com/watch?v={info.get('id', '')}",
            "vidid": info.get("id", ""),
            "duration_min": info.get("duration"),
            "thumb": thumb.split("?")[0] if thumb else "",
        }
        return details, info.get("id", "")


    # === Media & Formats ===
    @capture_internal_err
    async def video(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[int, str]:
        link = self._prepare_link(link, videoid)
        stdout, stderr = await _exec_proc(
            "yt-dlp",
            *(_cookies_args()),
            "-g",
            "-f",
            "best[height<=?720][width<=?1280]",
            link,
        )
        return (1, stdout.decode().split("\n")[0]) if stdout else (0, stderr.decode())

    @capture_internal_err
    async def playlist(
        self, link: str, limit: int, user_id, videoid: Union[str, bool, None] = None
    ) -> List[str]:
        if videoid:
            link = self.playlist_url + str(videoid)
        link = self._prepare_link(link).split("&")[0]

        try:
            plist = await Playlist.get(link)
            items = [video.get("id") for video in plist.get("videos", [])[:limit] if video.get("id")]
            if items:
                return items
        except Exception:
            pass

        stdout, _ = await _exec_proc(
            "yt-dlp",
            *(_cookies_args()),
            "-i",
            "--get-id",
            "--flat-playlist",
            "--playlist-end",
            str(limit),
            "--skip-download",
            link,
        )
        items = stdout.decode().strip().split("\n") if stdout else []
        return [i for i in items if i]

    @capture_internal_err
    async def formats(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[List[Dict], str]:
        link = self._prepare_link(link, videoid)
        key = f"f:{link}"
        now = time.time()

        async with _formats_lock:
            cached = _formats_cache.get(key)
            if cached and now - cached[0] < YOUTUBE_META_TTL:
                return cached[1], cached[2]

        opts = {"quiet": True}
        if cf := _cookiefile_path():
            opts["cookiefile"] = cf

        out: List[Dict] = []
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(link, download=False)
                for fmt in info.get("formats", []):
                    if "dash" in str(fmt.get("format", "")).lower():
                        continue
                    if not any(k in fmt for k in ("filesize", "filesize_approx")):
                        continue
                    if not all(k in fmt for k in ("format", "format_id", "ext", "format_note")):
                        continue
                    size = fmt.get("filesize") or fmt.get("filesize_approx")
                    if not size:
                        continue
                    out.append(
                        {
                            "format": fmt["format"],
                            "filesize": size,
                            "format_id": fmt["format_id"],
                            "ext": fmt["ext"],
                            "format_note": fmt["format_note"],
                            "yturl": link,
                        }
                    )
        except Exception:
            pass

        async with _formats_lock:
            if len(_formats_cache) > YOUTUBE_META_MAX:
                _formats_cache.clear()
            _formats_cache[key] = (now, out, link)

        return out, link

    @capture_internal_err
    async def slider(
        self, link: str, query_type: int, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], str, str]:
        q = self._prepare_link(link, videoid)
        res = await cached_youtube_search(q)
        if not res:
            raise IndexError("No results found via Piped API")
        r = res[0]
        return (
            r.get("title", ""),
            r.get("duration"),
            r.get("thumbnails", [{}])[-1].get("url", "").split("?")[0],
            r.get("id", ""),
        )


    @capture_internal_err
    async def download(
        self,
        link: str,
        mystic,
        *,
        video: Union[bool, str, None] = None,
        videoid: Union[str, bool, None] = None,
    ) -> Union[Tuple[str, Optional[bool]], Tuple[None, None]]:
        link = self._prepare_link(link, videoid)

        if video:
            if await self.is_live(link):
                status, stream_url = await self.video(link)
                if status == 1:
                    return stream_url, None
                return None, None

            if await is_on_off(1):
                p = await yt_dlp_download(link, type="video", title=await self.title(link))
                return (p, True) if p else (None, None)

            stdout, _ = await _exec_proc(
                "yt-dlp",
                *(_cookies_args()),
                "-g",
                "-f",
                "best[height<=?720][width<=?1280]",
                link,
            )
            if stdout:
                return stdout.decode().split("\n")[0], None
            return None, None

        p = await yt_dlp_download(link, type="audio", title=await self.title(link))
        return (p, True) if p else (None, None)
