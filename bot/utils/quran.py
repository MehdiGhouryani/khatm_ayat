import json
import os
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class QuranManager:
    def __init__(self, json_path: str = "data/quran.json"):
        """Initialize QuranManager with the path to the JSON file."""
        self.json_path = json_path
        try:
            self.verses = self._load_quran()
            self.verse_by_id = {v['id']: v for v in self.verses}
            self.verse_by_surah_ayah = {(v['surah_number'], v['ayah_number']): v for v in self.verses}
            logger.info(f"QuranManager initialized with {len(self.verses)} verses")
        except Exception as e:
            logger.error(f"Failed to initialize QuranManager: {e}")
            raise

    def _load_quran(self) -> List[Dict]:
        """Load Quran data from JSON file."""
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                verses = json.load(f)
            logger.info(f"Loaded Quran data from {self.json_path}")
            return verses
        except FileNotFoundError:
            logger.error(f"Quran JSON file not found at {self.json_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format in Quran file: {self.json_path}")
            raise

    def get_verse(self, surah_number: int, ayah_number: int) -> Optional[Dict]:
        """Get a specific verse by surah and ayah number."""
        verse = self.verse_by_surah_ayah.get((surah_number, ayah_number))
        if not verse:
            logger.warning(f"Verse not found: surah={surah_number}, ayah={ayah_number}")
        return verse

    def get_verse_by_id(self, verse_id: int) -> Optional[Dict]:
        """Get a verse by its unique ID."""
        verse = self.verse_by_id.get(verse_id)
        if not verse:
            logger.warning(f"Verse not found: verse_id={verse_id}")
        return verse

    def get_verses_in_range(self, start_id: int, end_id: int) -> List[Dict]:
        """Get a range of verses by their IDs."""
        verses = [verse for verse in self.verses if start_id <= verse['id'] <= end_id]
        logger.debug(f"Retrieved {len(verses)} verses from id {start_id} to {end_id}")
        return verses

    def get_surah_verses(self, surah_number: int) -> List[Dict]:
        """Get all verses of a surah, including Bismillah."""
        verses = [verse for verse in self.verses if verse['surah_number'] == surah_number]
        logger.debug(f"Retrieved {len(verses)} verses for surah {surah_number}")
        return verses

    def get_surah_verse_count(self, surah_number: int) -> int:
        """Get the number of verses in a surah, including Bismillah."""
        count = len(self.get_surah_verses(surah_number))
        logger.debug(f"Verse count for surah {surah_number}: {count}")
        return count

    def get_surah_name(self, surah_number: int) -> Optional[str]:
        """Get the name of a surah."""
        verse = self.get_verse(surah_number, 1)
        name = verse['surah_name'] if verse else None
        if not name:
            logger.warning(f"Surah name not found for surah {surah_number}")
        return name