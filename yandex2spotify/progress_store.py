import json
import os

from . import config
from .logger import logger


class ProgressStore:
    def __init__(self, path=None, schema_version=None, flush_every=None):
        self.path = config.PROGRESS_FILE if path is None else path
        self.schema_version = config.PROGRESS_SCHEMA_VERSION if schema_version is None else schema_version
        self.flush_every = config.PROGRESS_FLUSH_EVERY if flush_every is None else flush_every
        self._dirty = 0

        self.data, progress_changed = self._load()
        if progress_changed:
            self.save(force=True)

    @staticmethod
    def to_non_negative_int(value, default=0):
        try:
            result = int(value)
        except (TypeError, ValueError):
            return default
        return max(result, 0)

    @staticmethod
    def normalize_buffer(value):
        if not isinstance(value, list):
            return []
        return [str(item) for item in value]

    def _default_progress(self):
        return {
            'schema_version': self.schema_version,
            'likes': {'index': 0, 'buffer': []},
            'albums': {'index': 0, 'buffer': []},
            'artists': {'index': 0, 'buffer': []},
            'playlists': {},
        }

    def _load(self):
        default_progress = self._default_progress()
        if not os.path.exists(self.path):
            return default_progress, False

        try:
            with open(self.path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except Exception:
            logger.warning('Could not parse progress file. Starting with clean progress.')
            return default_progress, True

        if not isinstance(data, dict):
            logger.warning('Progress file format is invalid. Starting with clean progress.')
            return default_progress, True

        changed = False
        schema_version = self.to_non_negative_int(data.get('schema_version', 1), default=1)

        playlists_data = data.get('playlists')
        if not isinstance(playlists_data, dict):
            playlists_data = {}
            changed = True

        normalized = self._default_progress()

        for section in ['likes', 'albums', 'artists']:
            raw_node = data.get(section)
            if not isinstance(raw_node, dict):
                raw_node = {}
                changed = True

            index = self.to_non_negative_int(raw_node.get('index', 0))
            buffer_items = self.normalize_buffer(raw_node.get('buffer'))

            if schema_version < self.schema_version and index > 0:
                logger.warning(
                    f'Progress migration: resetting {section} (index {index} -> 0) due to schema update.'
                )
                index = 0
                buffer_items = []
                changed = True

            normalized[section] = {'index': index, 'buffer': buffer_items}

        for raw_key, raw_value in playlists_data.items():
            key = str(raw_key)
            if not isinstance(raw_value, dict):
                raw_value = {}
                changed = True

            index = self.to_non_negative_int(raw_value.get('index', 0))
            buffer_items = self.normalize_buffer(raw_value.get('buffer'))

            spotify_playlist_id = raw_value.get('spotify_playlist_id')
            if spotify_playlist_id is not None and not isinstance(spotify_playlist_id, str):
                spotify_playlist_id = str(spotify_playlist_id)
                changed = True

            if schema_version < self.schema_version and index > 0 and not spotify_playlist_id:
                logger.warning(
                    f'Progress migration: resetting playlist {key} (index {index} -> 0) due to missing spotify playlist id.'
                )
                index = 0
                buffer_items = []
                changed = True

            normalized['playlists'][key] = {
                'index': index,
                'buffer': buffer_items,
                'spotify_playlist_id': spotify_playlist_id,
            }

        if schema_version != self.schema_version:
            changed = True

        return normalized, changed

    def save(self, force=False):
        if not force and self._dirty < self.flush_every:
            return

        self.data['schema_version'] = self.schema_version
        with open(self.path, 'w', encoding='utf-8') as file:
            json.dump(self.data, file, ensure_ascii=False, indent=2)

        self._dirty = 0

    def bump_dirty(self):
        self._dirty += 1
        self.save(force=False)
