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

    # --- THE JIOSAAVN API PIVOT ---
    try:
        import urllib.parse
        import aiohttp
        safe_query = urllib.parse.quote(query)
        api_url = f"https://saavn.dev/api/search/songs?query={safe_query}"
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("data", {}).get("results", [])
                    if results:
                        first = results[0]
                        duration_seconds = int(first.get("duration", 0))
                        mins, secs = divmod(duration_seconds, 60)
                        
                        images = first.get("image", [])
                        thumb = images[-1].get("url") if images else ""
                        
                        result = [{
                            "id": first.get("id"),  # The real JioSaavn short ID!
                            "title": first.get("name", "Unknown Title"),
                            "duration": f"{mins}:{secs:02d}",
                            "thumbnails": [{"url": thumb}],
                            "raw_duration": duration_seconds
                        }]
                    else:
                        result = []
                else:
                    result = []
    except Exception:
        result = []

    if result:
        async with _cache_lock:
            _cache[key] = (now, result)
    return result




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
    @capture_internal_err
    async def details(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], int, str, str]:
        target_id = videoid if videoid else link
        
        # --- THE JIOSAAVN ROTATOR ---
        SAAVN_APIS = [
            "https://saavn.dev",
            "https://jiosaavn-api-privatecvc2.vercel.app",
            "https://jiosaavn-api-v3.vercel.app",
            "https://saavn.me"
        ]
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for base_api in SAAVN_APIS:
                try:
                    # Depending on the API version, the URL structure changes slightly
                    api_url = f"{base_api}/api/songs/{target_id}" if "dev" in base_api else f"{base_api}/songs?id={target_id}"
                    async with session.get(api_url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            songs = data.get("data", [])
                            if songs:
                                first = songs[0] if isinstance(songs, list) else songs
                                duration_seconds = int(first.get("duration", 0))
                                mins, secs = divmod(duration_seconds, 60)
                                images = first.get("image", [])
                                thumb = images[-1].get("url") if images else ""
                                return first.get("name", "Unknown Title"), f"{mins}:{secs:02d}", duration_seconds, thumb, target_id
                except Exception:
                    continue # Server disconnected or failed, instantly try the next one!
                    
        raise ValueError("Song not found on any JioSaavn backup server")

    @capture_internal_err
    async def track(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[Dict, str]:
        target_id = videoid if videoid else link
        
        SAAVN_APIS = [
            "https://saavn.dev",
            "https://jiosaavn-api-privatecvc2.vercel.app",
            "https://jiosaavn-api-v3.vercel.app",
            "https://saavn.me"
        ]
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for base_api in SAAVN_APIS:
                try:
                    api_url = f"{base_api}/api/songs/{target_id}" if "dev" in base_api else f"{base_api}/songs?id={target_id}"
                    async with session.get(api_url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            songs = data.get("data", [])
                            if songs:
                                first = songs[0] if isinstance(songs, list) else songs
                                duration_seconds = int(first.get("duration", 0))
                                mins, secs = divmod(duration_seconds, 60)
                                images = first.get("image", [])
                                thumb = images[-1].get("url") if images else ""
                                
                                details = {
                                    "title": first.get("name", "Unknown Title"),
                                    "link": link, 
                                    "vidid": target_id,
                                    "duration_min": f"{mins}:{secs:02d}",
                                    "thumb": thumb,
                                }
                                return details, target_id
                except Exception:
                    continue
                    
        raise ValueError("Song not found on any mikasa backup server")



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
        # stream.py passes the short JioSaavn ID through 'videoid'
        target_id = videoid if videoid else link

        api_url = f"https://saavn.dev/api/songs/{target_id}"
        dl_link = None
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        songs = data.get("data", [])
                        
                        # Extract the highest quality download link
                        if isinstance(songs, list) and songs:
                            dls = songs[0].get("downloadUrl", [])
                        elif isinstance(songs, dict):
                            dls = songs.get("downloadUrl", [])
                        else:
                            dls = []
                            
                        dl_link = dls[-1].get("url") if dls else None
        except Exception:
            pass

        if not dl_link:
            return None, None

        # Pass the raw audio stream URL straight to our lightweight downloader!
        p = await yt_dlp_download(dl_link, type="video" if video else "audio", title=await self.title(link))
        return (p, True) if p else (None, None)
