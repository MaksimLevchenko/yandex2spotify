import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import importer
from yandex2spotify import config as app_config
from importer import Importer, NotFoundException, SearchException


class DummySpotifyClient:
    def me(self):
        return {'id': 'test-user'}

    def search(self, query, type=None):
        item_id = f'{type}_{query}'.replace(' ', '_')
        return {f'{type}s': {'items': [{'id': item_id}]}}

    def _put(self, endpoint, args=None):
        return {}

    def _post(self, endpoint, payload=None):
        if endpoint == 'me/playlists':
            return {'id': 'new-playlist-id'}
        return {}

    def playlist(self, playlist_id, fields=None):
        return {'id': playlist_id}

    def playlist_upload_cover_image(self, playlist_id, image_data):
        return {}


class FakeLikeTrack:
    def __init__(self, track_id, album_id='album'):
        self.id = track_id
        self.album_id = album_id


class FakePlaylistTrack:
    def __init__(self, track):
        self.track = track


class FakePlaylist:
    def __init__(self, kind='42', title='My Playlist'):
        self.kind = kind
        self.title = title
        self.collective = False
        self.cover = SimpleNamespace(type='none', download=lambda *args, **kwargs: None)

    def fetch_tracks(self):
        return [FakePlaylistTrack('track1'), FakePlaylistTrack('track2')]


class FakeLikesYandexClient:
    def users_likes_tracks(self):
        return SimpleNamespace(
            tracks=[
                FakeLikeTrack('0'),
                FakeLikeTrack('1'),
                FakeLikeTrack('2'),
                FakeLikeTrack('3'),
            ]
        )

    def tracks(self, track_ids):
        return ['t0', 't1', 't2', 't3']


class ImporterTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.progress_path = os.path.join(self.temp_dir.name, 'progress.json')
        self.not_found_json_path = os.path.join(self.temp_dir.name, 'not_found_tracks.json')
        self.not_found_txt_path = os.path.join(self.temp_dir.name, 'not_found_tracks.txt')

        self.progress_patcher = patch.object(app_config, 'PROGRESS_FILE', self.progress_path)
        self.not_found_json_patcher = patch.object(app_config, 'NOT_FOUND_JSON_FILE', self.not_found_json_path)
        self.not_found_txt_patcher = patch.object(app_config, 'NOT_FOUND_TXT_FILE', self.not_found_txt_path)

        self.progress_patcher.start()
        self.not_found_json_patcher.start()
        self.not_found_txt_patcher.start()

        self.addCleanup(self.progress_patcher.stop)
        self.addCleanup(self.not_found_json_patcher.stop)
        self.addCleanup(self.not_found_txt_patcher.stop)

    def make_importer(self, yandex_client=None):
        if yandex_client is None:
            yandex_client = SimpleNamespace()
        spotify_client = DummySpotifyClient()
        return Importer(spotify_client, yandex_client, ignore_list=[], strict_search=False)

    def test_resume_algorithm_does_not_skip_tracks(self):
        importer_instance = self.make_importer(FakeLikesYandexClient())

        calls = {'count': 0}

        def flaky_import_item(item):
            calls['count'] += 1
            if calls['count'] == 3:
                raise RuntimeError('stop import')
            return f'id_{item}', item, item

        with patch.object(importer_instance, '_import_item', side_effect=flaky_import_item):
            with self.assertRaises(RuntimeError):
                importer_instance.import_likes()

        self.assertEqual(importer_instance._progress['likes']['index'], 2)
        self.assertEqual(importer_instance._progress['likes']['buffer'], ['id_t3', 'id_t2'])

        saved_ids = []

        def capture_save_to_library(uris):
            for uri in uris:
                saved_ids.append(uri.split(':')[-1])

        with patch.object(importer_instance, '_import_item', side_effect=lambda item: (f'id_{item}', item, item)):
            with patch.object(importer_instance, '_save_uris_to_library', side_effect=capture_save_to_library):
                importer_instance.import_likes()

        self.assertEqual(importer_instance._progress['likes']['index'], 4)
        self.assertEqual(importer_instance._progress['likes']['buffer'], [])
        self.assertEqual(saved_ids, ['id_t3', 'id_t2', 'id_t1', 'id_t0'])

    def test_playlist_resume_reuses_existing_spotify_playlist(self):
        playlist = FakePlaylist(kind='42', title='Resume Playlist')
        yandex_client = SimpleNamespace(users_playlists_list=lambda: [playlist])
        importer_instance = self.make_importer(yandex_client)

        importer_instance._progress['playlists']['42'] = {
            'index': 0,
            'buffer': [],
            'spotify_playlist_id': 'existing-playlist-id'
        }

        with patch.object(importer_instance, '_playlist_exists', return_value=True):
            with patch.object(importer_instance, '_create_playlist') as create_playlist_mock:
                with patch.object(importer_instance, '_import_item', side_effect=lambda item: (f'id_{item}', item, item)):
                    with patch.object(importer_instance, '_add_uris_to_playlist') as add_to_playlist_mock:
                        importer_instance.import_playlists()

        create_playlist_mock.assert_not_called()
        self.assertTrue(add_to_playlist_mock.called)
        for args, _ in add_to_playlist_mock.call_args_list:
            self.assertEqual(args[0], 'existing-playlist-id')

    def test_flush_buffer_preserves_unsent_tail_on_failure(self):
        importer_instance = self.make_importer()

        progress_node = {'index': 0, 'buffer': []}
        buffer_ids = ['id1', 'id2', 'id3']

        def save_items_callback(_, chunk):
            if chunk == ['id3']:
                raise RuntimeError('network failure')

        with patch.object(app_config, 'FLUSH_SPOTIFY_BATCH', 2):
            with self.assertRaises(RuntimeError):
                importer_instance._flush_buffer(buffer_ids, save_items_callback, progress_node)

        self.assertEqual(progress_node['buffer'], ['id3'])
        self.assertEqual(buffer_ids, ['id1', 'id2', 'id3'])

    def test_not_found_files_include_reasons(self):
        importer_instance = self.make_importer()

        progress_node = {'index': 0, 'buffer': []}

        side_effect = [
            NotFoundException('Song A', 'query a'),
            SearchException('Song B', 'query b'),
            ('id_song_c', 'Song C', 'query c')
        ]

        with patch.object(importer_instance, '_import_item', side_effect=side_effect):
            importer_instance._add_items_to_spotify(
                ['a', 'b', 'c'],
                section_name='Likes',
                save_items_callback=lambda *_: None,
                progress_node=progress_node
            )

        importer_instance._write_not_found_files()

        with open(self.not_found_json_path, 'r', encoding='utf-8') as file:
            payload = json.load(file)

        likes_section = payload['sections']['Likes']
        self.assertEqual(len(likes_section['not_found']), 1)
        self.assertEqual(len(likes_section['search_error']), 1)
        self.assertEqual(likes_section['not_found'][0]['item'], 'Song A')
        self.assertEqual(likes_section['search_error'][0]['item'], 'Song B')

        with open(self.not_found_txt_path, 'r', encoding='utf-8') as file:
            txt_payload = file.read()

        self.assertIn('not_found: 1', txt_payload)
        self.assertIn('search_error: 1', txt_payload)
        self.assertIn('Song A', txt_payload)
        self.assertIn('Song B', txt_payload)

    def test_progress_migration_resets_legacy_indexes_and_sets_schema(self):
        legacy_progress = {
            'likes': {'index': 5, 'buffer': ['l1']},
            'albums': {'index': 2, 'buffer': ['a1']},
            'artists': {'index': 1, 'buffer': ['ar1']},
            'playlists': {
                '100': {'index': 4, 'buffer': ['p1']},
                '101': {'index': 3, 'buffer': ['p2'], 'spotify_playlist_id': 'keep-id'}
            }
        }

        with open(self.progress_path, 'w', encoding='utf-8') as file:
            json.dump(legacy_progress, file)

        importer_instance = self.make_importer()

        self.assertEqual(importer_instance._progress['schema_version'], 2)
        self.assertEqual(importer_instance._progress['likes']['index'], 0)
        self.assertEqual(importer_instance._progress['likes']['buffer'], [])
        self.assertEqual(importer_instance._progress['albums']['index'], 0)
        self.assertEqual(importer_instance._progress['artists']['index'], 0)

        migrated_playlist_without_id = importer_instance._progress['playlists']['100']
        self.assertEqual(migrated_playlist_without_id['index'], 0)
        self.assertEqual(migrated_playlist_without_id['buffer'], [])
        self.assertIsNone(migrated_playlist_without_id['spotify_playlist_id'])

        migrated_playlist_with_id = importer_instance._progress['playlists']['101']
        self.assertEqual(migrated_playlist_with_id['index'], 3)
        self.assertEqual(migrated_playlist_with_id['buffer'], ['p2'])
        self.assertEqual(migrated_playlist_with_id['spotify_playlist_id'], 'keep-id')

        with open(self.progress_path, 'r', encoding='utf-8') as file:
            saved_progress = json.load(file)

        self.assertEqual(saved_progress['schema_version'], 2)


if __name__ == '__main__':
    unittest.main()
