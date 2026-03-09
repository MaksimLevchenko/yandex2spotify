import json

from spotipy.exceptions import SpotifyException
from yandex_music import Artist, Client

from . import config
from .exceptions import NotFoundException, SearchException
from .logger import logger
from .not_found_reporter import NotFoundReporter
from .progress_store import ProgressStore
from .spotify_gateway import SpotifyGateway
from .utils import chunks


class Importer:
    def __init__(
        self,
        spotify_client,
        yandex_client: Client,
        ignore_list,
        strict_search,
        progress_store=None,
        not_found_reporter=None,
        spotify_gateway=None,
    ):
        self.spotify_client = spotify_client
        self.yandex_client = yandex_client
        self._strict_search = strict_search

        self.spotify_gateway = SpotifyGateway(spotify_client) if spotify_gateway is None else spotify_gateway
        self.progress_store = ProgressStore() if progress_store is None else progress_store
        self.not_found_reporter = NotFoundReporter() if not_found_reporter is None else not_found_reporter

        self._progress = self.progress_store.data
        self.not_imported = self.not_found_reporter.not_imported
        self.not_imported_records = self.not_found_reporter.records

        self.user = self.spotify_gateway.current_user_id()
        logger.info(f'User ID: {self.user}')

        self._importing_items = {
            'likes': self.import_likes,
            'playlists': self.import_playlists,
            'albums': self.import_albums,
            'artists': self.import_artists,
        }

        for item in ignore_list:
            del self._importing_items[item]

    @staticmethod
    def _to_non_negative_int(value, default=0):
        return ProgressStore.to_non_negative_int(value, default=default)

    @staticmethod
    def _normalize_buffer(value):
        return ProgressStore.normalize_buffer(value)

    def _save_progress(self, force=False):
        self.progress_store.save(force=force)

    def _bump_progress_dirty(self):
        self.progress_store.bump_dirty()

    def _get_item_search_context(self, item):
        if isinstance(item, str):
            return {
                'type': 'track',
                'item_name': item,
                'query': item,
                'artists': [],
                'title': item,
                'is_artist': False,
            }

        type_ = item.__class__.__name__.casefold()
        is_artist = isinstance(item, Artist)

        if is_artist:
            item_name = getattr(item, 'name', str(item))
            artists = []
            title = item_name
        else:
            artists = getattr(item, 'artists', []) or []
            artist_names = ', '.join([artist.name for artist in artists if hasattr(artist, 'name')])
            title = getattr(item, 'title', str(item))
            item_name = f'{artist_names} - {title}' if artist_names else title

        if len(item_name) > 100:
            item_name = item_name[:100]
            logger.info('Name too long... Trimming to 100 characters. May affect search accuracy')

        query = item_name.replace('- ', '')

        return {
            'type': type_,
            'item_name': item_name,
            'query': query,
            'artists': artists,
            'title': title,
            'is_artist': is_artist,
        }

    def _import_item(self, item):
        context = self._get_item_search_context(item)
        type_ = context['type']
        item_name = context['item_name']
        query = context['query']

        logger.info(f'Importing {type_}: {item_name}...')

        try:
            found_items = self.spotify_gateway.search_items(query, type_)
        except SpotifyException as exception:
            raise SearchException(item_name, query, exception) from exception

        logger.info(f'Searching "{query}"...')

        if (
            not self._strict_search
            and not context['is_artist']
            and not found_items
            and len(context['artists']) > 1
        ):
            query = f"{context['artists'][0].name} {context['title']}"
            try:
                found_items = self.spotify_gateway.search_items(query, type_)
            except SpotifyException as exception:
                raise SearchException(item_name, query, exception) from exception
            logger.info(f'Searching "{query}"...')

        if not found_items:
            raise NotFoundException(item_name, query)

        return found_items[0]['id'], item_name, query

    def _flush_buffer(self, buffer_ids, save_items_callback, progress_node):
        if not buffer_ids:
            progress_node['buffer'] = []
            return

        pending_ids = list(buffer_ids)
        progress_node['buffer'] = list(pending_ids)
        self._save_progress(force=True)

        while pending_ids:
            chunk = pending_ids[:config.FLUSH_SPOTIFY_BATCH]
            save_items_callback(self, chunk)
            pending_ids = pending_ids[len(chunk):]
            progress_node['buffer'] = list(pending_ids)
            self._save_progress(force=True)

        buffer_ids.clear()

    def _save_uris_to_library(self, uris):
        self.spotify_gateway.save_uris_to_library(uris)

    def _create_playlist(self, playlist_name):
        return self.spotify_gateway.create_playlist(playlist_name)

    def _playlist_exists(self, playlist_id):
        return self.spotify_gateway.playlist_exists(playlist_id)

    def _add_uris_to_playlist(self, playlist_id, uris):
        return self.spotify_gateway.add_uris_to_playlist(playlist_id, uris)

    def _record_not_imported(self, section, item_name, reason, query):
        self.not_found_reporter.record(section, item_name, reason, query)

    def _write_not_found_files(self):
        self.not_found_reporter.write_files()

    def _finalize_not_found_reports(self):
        self.not_found_reporter.finalize()

    def _add_items_to_spotify(self, items, section_name, save_items_callback, progress_node):
        spotify_items = []

        existing_buffer = self._normalize_buffer(progress_node.get('buffer'))
        if existing_buffer:
            logger.info(f'Flushing {len(existing_buffer)} items from progress buffer before continuing...')
            spotify_items.extend(existing_buffer)
            self._flush_buffer(spotify_items, save_items_callback, progress_node)

        for item in items:
            try:
                spotify_id, _, _ = self._import_item(item)
                if spotify_id is None:
                    logger.warning('Item ID is None, skipping...')
                else:
                    spotify_items.append(spotify_id)
                    logger.info('OK')
            except NotFoundException as exception:
                self._record_not_imported(section_name, exception.item_name, 'not_found', exception.query)
                logger.warning('NO')
            except SearchException as exception:
                self._record_not_imported(section_name, exception.item_name, 'search_error', exception.query)
                logger.warning('NO')

            progress_node['index'] = self._to_non_negative_int(progress_node.get('index', 0)) + 1
            progress_node['buffer'] = list(spotify_items)
            self._bump_progress_dirty()

            if len(spotify_items) >= config.FLUSH_SPOTIFY_BATCH:
                self._flush_buffer(spotify_items, save_items_callback, progress_node)

        if spotify_items:
            self._flush_buffer(spotify_items, save_items_callback, progress_node)

        progress_node['buffer'] = []
        self._save_progress(force=True)

    def import_likes(self):
        section_name = 'Likes'
        self.not_found_reporter.ensure_section(section_name)

        likes_tracks = self.yandex_client.users_likes_tracks().tracks
        tracks = self.yandex_client.tracks([f'{track.id}:{track.album_id}' for track in likes_tracks if track.album_id])
        ordered_tracks = list(reversed(tracks))

        logger.info('Importing liked tracks...')

        total = len(ordered_tracks)
        node = self._progress['likes']
        start_index = self._to_non_negative_int(node.get('index', 0))
        if start_index > total:
            start_index = total

        if start_index > 0:
            logger.info(f'Resuming liked tracks from index {start_index}/{total}...')

        tracks_to_import = ordered_tracks[start_index:]

        def save_tracks_callback(importer, spotify_tracks):
            logger.info(f'Saving {len(spotify_tracks)} tracks...')
            importer._save_uris_to_library([f'spotify:track:{track_id}' for track_id in spotify_tracks])
            logger.info('OK')

        self._add_items_to_spotify(tracks_to_import, section_name, save_tracks_callback, node)

    def import_playlists(self):
        playlists = self.yandex_client.users_playlists_list()

        for playlist in playlists:
            logger.info(f'Importing playlist {playlist.title}...')

            section_name = playlist.title
            self.not_found_reporter.ensure_section(section_name)

            playlist_tracks = playlist.fetch_tracks()
            if not playlist.collective:
                tracks = [track.track for track in playlist_tracks]
            elif playlist.collective and playlist_tracks:
                tracks = self.yandex_client.tracks([track.track_id for track in playlist_tracks])
            else:
                tracks = []

            ordered_tracks = list(reversed(tracks))
            total = len(ordered_tracks)

            key = str(playlist.kind)
            if key not in self._progress['playlists']:
                self._progress['playlists'][key] = {'index': 0, 'buffer': [], 'spotify_playlist_id': None}

            node = self._progress['playlists'][key]
            if 'spotify_playlist_id' not in node:
                node['spotify_playlist_id'] = None

            start_index = self._to_non_negative_int(node.get('index', 0))
            if start_index > total:
                start_index = total

            if start_index > 0:
                logger.info(f'Resuming playlist {playlist.title} from index {start_index}/{total}...')

            has_pending_buffer = bool(self._normalize_buffer(node.get('buffer')))
            if start_index >= total and not has_pending_buffer:
                logger.info(f'Playlist {playlist.title} already imported ({start_index}/{total}). Skipping.')
                continue

            spotify_playlist_id = node.get('spotify_playlist_id')
            if spotify_playlist_id and not self._playlist_exists(spotify_playlist_id):
                logger.warning(
                    f'Spotify playlist {spotify_playlist_id} is not available anymore. Creating a new one.'
                )
                spotify_playlist_id = None
                node['spotify_playlist_id'] = None
                self._save_progress(force=True)

            if not spotify_playlist_id:
                spotify_playlist = self._create_playlist(playlist.title)
                spotify_playlist_id = spotify_playlist['id']
                node['spotify_playlist_id'] = spotify_playlist_id
                self._save_progress(force=True)

                if playlist.cover.type == 'pic':
                    filename = f'{playlist.kind}-cover'
                    playlist.cover.download(filename, size='400x400')
                    self.spotify_gateway.upload_playlist_cover(spotify_playlist_id, filename)

            tracks_to_import = ordered_tracks[start_index:]

            def save_tracks_callback(importer, spotify_tracks, playlist_title=playlist.title, target_playlist_id=spotify_playlist_id):
                logger.info(f'Saving {len(spotify_tracks)} tracks in playlist {playlist_title}...')
                importer._add_uris_to_playlist(
                    target_playlist_id,
                    [f'spotify:track:{track_id}' for track_id in spotify_tracks],
                )
                logger.info('OK')

            self._add_items_to_spotify(tracks_to_import, section_name, save_tracks_callback, node)

    def import_albums(self):
        section_name = 'Albums'
        self.not_found_reporter.ensure_section(section_name)

        likes_albums = self.yandex_client.users_likes_albums()
        albums = [album.album for album in likes_albums]
        ordered_albums = list(reversed(albums))

        logger.info('Importing albums...')

        total = len(ordered_albums)
        node = self._progress['albums']
        start_index = self._to_non_negative_int(node.get('index', 0))
        if start_index > total:
            start_index = total

        if start_index > 0:
            logger.info(f'Resuming albums from index {start_index}/{total}...')

        albums_to_import = ordered_albums[start_index:]

        def save_albums_callback(importer, spotify_albums):
            logger.info(f'Saving {len(spotify_albums)} albums...')
            importer._save_uris_to_library([f'spotify:album:{album_id}' for album_id in spotify_albums])
            logger.info('OK')

        self._add_items_to_spotify(albums_to_import, section_name, save_albums_callback, node)

    def import_artists(self):
        section_name = 'Artists'
        self.not_found_reporter.ensure_section(section_name)

        likes_artists = self.yandex_client.users_likes_artists()
        artists = [artist.artist for artist in likes_artists]
        ordered_artists = list(reversed(artists))

        logger.info('Importing artists...')

        total = len(ordered_artists)
        node = self._progress['artists']
        start_index = self._to_non_negative_int(node.get('index', 0))
        if start_index > total:
            start_index = total

        if start_index > 0:
            logger.info(f'Resuming artists from index {start_index}/{total}...')

        artists_to_import = ordered_artists[start_index:]

        def save_artists_callback(importer, spotify_artists):
            logger.info(f'Saving {len(spotify_artists)} artists...')
            importer._save_uris_to_library([f'spotify:artist:{artist_id}' for artist_id in spotify_artists])
            logger.info('OK')

        self._add_items_to_spotify(artists_to_import, section_name, save_artists_callback, node)

    def import_all(self):
        try:
            for item in self._importing_items.values():
                item()
        finally:
            try:
                self._save_progress(force=True)
            except Exception:
                pass
            self._finalize_not_found_reports()

    def print_not_imported(self):
        self.not_found_reporter.print_summary()

    def import_from_json(self, file_path):
        section_name = 'JSON Import'
        self.not_found_reporter.ensure_section(section_name)

        with open(file_path, 'r', encoding='UTF-8') as file:
            tracks = json.load(file)

        spotify_tracks = []

        try:
            for track in tracks:
                query = f'{track["artist"]} {track["track"]}'

                try:
                    spotify_track_id, _, _ = self._import_item(query)
                    spotify_tracks.append(spotify_track_id)
                    logger.info('OK')
                except NotFoundException as exception:
                    self._record_not_imported(section_name, exception.item_name, 'not_found', exception.query)
                    logger.warning('NO')
                except SearchException as exception:
                    self._record_not_imported(section_name, exception.item_name, 'search_error', exception.query)
                    logger.warning('NO')

                if len(spotify_tracks) >= config.FLUSH_SPOTIFY_BATCH:
                    for chunk in chunks(spotify_tracks, config.FLUSH_SPOTIFY_BATCH):
                        logger.info(f'Saving {len(chunk)} tracks...')
                        self._save_uris_to_library([f'spotify:track:{track_id}' for track_id in chunk])
                        logger.info('OK')
                    spotify_tracks.clear()

            playlist_name = 'Imported from JSON'
            playlist = self._create_playlist(playlist_name)

            if spotify_tracks:
                for chunk in chunks(spotify_tracks, config.FLUSH_SPOTIFY_BATCH):
                    logger.info(f'Saving {len(chunk)} tracks...')
                    self._add_uris_to_playlist(playlist['id'], [f'spotify:track:{track_id}' for track_id in chunk])
                    logger.info('OK')
        finally:
            self._finalize_not_found_reports()
