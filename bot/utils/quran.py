import json
import aiofiles
import os
import logging
from typing import List, Dict, Optional
from config import settings

logger = logging.getLogger(__name__)

class QuranManager:
    def __init__(self, json_path: str = None):
        """Initialize QuranManager with the path to the JSON file."""
        self.json_path = "data/quran.json"
        self.verses = None
        self.verse_by_id = {}
        self.verse_by_surah_ayah = {}

    async def initialize(self):
        """Async initialization of QuranManager."""
        try:
            # Ensure the data directory exists
            os.makedirs(os.path.dirname(self.json_path), exist_ok=True)
            
            self.verses = await self._load_quran()
            self.verse_by_id = {v['id']: v for v in self.verses}
            self.verse_by_surah_ayah = {(v['surah_number'], v['ayah_number']): v for v in self.verses}
            logger.info("QuranManager initialized with %d verses", len(self.verses))
        except Exception as e:
            logger.error("Failed to initialize QuranManager: %s", e)
            raise

    async def _load_quran(self) -> List[Dict]:
        """Load Quran data from JSON file asynchronously."""
        try:
            async with aiofiles.open(self.json_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                verses = json.loads(content)
            logger.info("Loaded Quran data from %s", self.json_path)
            return verses
        except FileNotFoundError:
            logger.error("Quran JSON file not found at %s", self.json_path)
            raise
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in Quran file: %s", self.json_path)
            raise
        except Exception as e:
            logger.error("Error loading Quran data: %s", e)
            raise

    def get_verse(self, surah_number: int, ayah_number: int) -> Optional[Dict]:
        """Get a specific verse by surah and ayah number."""
        verse = self.verse_by_surah_ayah.get((surah_number, ayah_number))
        if not verse:
            logger.debug("Verse not found: surah=%d, ayah=%d", surah_number, ayah_number)
        return verse

    def get_verse_by_id(self, verse_id: int) -> Optional[Dict]:
        """Get a verse by its unique ID."""
        verse = self.verse_by_id.get(verse_id)
        if not verse:
            logger.debug("Verse not found: verse_id=%d", verse_id)
        return verse

    def get_verses_in_range(self, start_id: int, end_id: int) -> List[Dict]:
        """Get a range of verses by their IDs."""
        verses = [verse for verse in self.verses if start_id <= verse['id'] <= end_id]
        logger.debug("Retrieved %d verses from id %d to %d", len(verses), start_id, end_id)
        return verses

    def get_surah_verses(self, surah_number: int) -> List[Dict]:
        """Get all verses of a surah, including Bismillah."""
        verses = [verse for verse in self.verses if verse['surah_number'] == surah_number]
        logger.debug("Retrieved %d verses for surah %d", len(verses), surah_number)
        return verses

    def get_surah_verse_count(self, surah_number: int) -> int:
        """Get the number of verses in a surah, including Bismillah."""
        count = len(self.get_surah_verses(surah_number))
        logger.debug("Verse count for surah %d: %d", surah_number, count)
        return count

    def get_surah_name(self, surah_number: int) -> Optional[str]:
        """Get the name of a surah."""
        verse = self.get_verse(surah_number, 1)
        name = verse['surah_name'] if verse else None
        if not name:
            logger.debug("Surah name not found for surah %d", surah_number)
        return name