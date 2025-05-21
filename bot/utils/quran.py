import json
import aiofiles
import os
import logging
from typing import List, Dict, Optional
from config import settings

logger = logging.getLogger(__name__)

class QuranError(Exception):
    """Custom exception class for Quran-related errors."""
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error

class QuranManager:
    _instance = None

    @classmethod
    async def get_instance(cls):
        """Get the singleton instance of QuranManager, initializing it if necessary."""
        try:
            if not cls._instance:
                cls._instance = cls()
                await cls._instance.initialize()
            return cls._instance
        except Exception as e:
            raise QuranError(f"Error initializing QuranManager: {str(e)}", e)

    def __init__(self, json_path: str = None):
        """Initialize QuranManager with the path to the JSON file."""
        if self._instance is not None:
            raise RuntimeError("Use get_instance() to access QuranManager")
        self.json_path = json_path or "data/quran.json"
        self.verses = None
        self.verse_by_id = {}
        self.verse_by_surah_ayah = {}
        logger.debug("QuranManager initialized with json_path=%s", self.json_path)

    async def initialize(self):
        """Async initialization of QuranManager."""
        logger.info("Starting QuranManager.initialize")
        try:
            os.makedirs(os.path.dirname(self.json_path), exist_ok=True)
            logger.debug("Data directory ensured: %s", os.path.dirname(self.json_path))
            
            self.verses = await self._load_quran()
            self.verse_by_id = {v['id']: v for v in self.verses}
            self.verse_by_surah_ayah = {(v['surah_number'], v['ayah_number']): v for v in self.verses}
            logger.info("QuranManager initialized with %d verses", len(self.verses))
        except Exception as e:
            logger.error("Failed to initialize QuranManager: %s", e, exc_info=True)
            raise

    async def _load_quran(self) -> List[Dict]:
        """Load Quran data from JSON file asynchronously."""
        logger.info("Loading Quran data from %s", self.json_path)
        try:
            async with aiofiles.open(self.json_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                verses = json.loads(content)
            logger.info("Loaded %d verses from %s", len(verses), self.json_path)
            if not verses:
                logger.error("Quran JSON file is empty")
                raise ValueError("Quran JSON file is empty")
            return verses
        except FileNotFoundError:
            logger.error("Quran JSON file not found at %s", self.json_path)
            raise
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in Quran file: %s, error: %s", self.json_path, e)
            raise
        except Exception as e:
            logger.error("Error loading Quran data: %s", e, exc_info=True)
            raise

    def get_verse(self, surah_number: int, ayah_number: int) -> Optional[Dict]:
        logger.debug("Fetching verse: surah=%d, ayah=%d", surah_number, ayah_number)
        verse = self.verse_by_surah_ayah.get((surah_number, ayah_number))
        if not verse:
            logger.debug("Verse not found: surah=%d, ayah=%d", surah_number, ayah_number)
        return verse

    def get_verse_by_id(self, verse_id: int) -> Optional[Dict]:
        logger.debug("Fetching verse by id: verse_id=%d", verse_id)
        try:
            return self.verse_by_id.get(verse_id)
        except Exception as e:
            raise QuranError(f"Error fetching verse {verse_id}: {str(e)}", e)

    def get_verses_in_range(self, start_id: int, end_id: int) -> List[Dict]:
        logger.debug("Fetching verses from id %d to %d", start_id, end_id)
        verses = [verse for verse in self.verses if start_id <= verse['id'] <= end_id]
        logger.debug("Retrieved %d verses from id %d to %d", len(verses), start_id, end_id)
        return verses

    def get_surah_verses(self, surah_number: int) -> List[Dict]:
        logger.debug("Fetching verses for surah %d", surah_number)
        verses = [verse for verse in self.verses if verse['surah_number'] == surah_number]
        logger.debug("Retrieved %d verses for surah %d", len(verses), surah_number)
        return verses

    def get_surah_verse_count(self, surah_number: int) -> int:
        logger.debug("Counting verses for surah %d", surah_number)
        count = len(self.get_surah_verses(surah_number))
        logger.debug("Verse count for surah %d: %d", surah_number, count)
        return count

    def get_surah_name(self, surah_number: int) -> Optional[str]:
        logger.debug("Fetching surah name for surah %d", surah_number)
        verse = self.get_verse(surah_number, 1)
        name = verse['surah_name'] if verse else None
        if not name:
            logger.debug("Surah name not found for surah %d", surah_number)
        return name