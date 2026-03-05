import json
import argparse
import logging
import os
from base64 import b64encode
from time import sleep

import spotipy
from PIL import Image
from requests.exceptions import ReadTimeout
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyOAuth
from yandex_music import Client, Artist

REDIRECT_URI = 'https://open.spotify.com'
MAX_REQUEST_RETRIES = 5
PROGRESS_FILE = 'progress.json'
FLUSH_SPOTIFY_BATCH = 20
FLUSH_SPOTIFY_LIBRARY_BATCH = 10
PROGRESS_FLUSH_EVERY = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def encode_file_base64_jpeg(filename):
    img = Image.open(filename)
    if img.format != 'JPEG':
        img.convert('RGB').save(filename, 'JPEG')

    with open(filename, 'rb') as f:
        return b64encode(f.read())


def handle_spotify_exception(func):
    def wrapper(*args, **kwargs):
        retry = 1
        while True:
            try:
                return func(*args, **kwargs)
            except SpotifyException as exception:
                if exception.http_status != 429:
                    raise exception

                if 'retry-after' in exception.headers:
                    sleep(int(exception.headers['retry-after']) + 1)
            except ReadTimeout as exception:
                logger.info(f'Read timed out. Retrying #{retry}...')

                if retry > MAX_REQUEST_RETRIES:
                    logger.info('Max retries reached.')
                    raise exception

                logger.info('Trying again...')
                retry += 1

    return wrapper


class NotFoundException(SpotifyException):
    def __init__(self, item_name):
        self.item_name = item_name


class Importer:
    def __init__(self, spotify_client, yandex_client: Client, ignore_list, strict_search):
        self.spotify_client = spotify_client
        self.yandex_client = yandex_client

        self._importing_items = {
            'likes': self.import_likes,
            'playlists': self.import_playlists,
            'albums': self.import_albums,
            'artists': self.import_artists
        }

        for item in ignore_list:
            del self._importing_items[item]

        self._strict_search = strict_search

        self.user = handle_spotify_exception(spotify_client.me)()['id']
        logger.info(f'User ID: {self.user}')

        self.not_imported = {}
        self._progress = self._load_progress()
        self._progress_dirty = 0

    def _load_progress(self):
        if not os.path.exists(PROGRESS_FILE):
            return {
                'likes': {'index': 0, 'buffer': []},
                'albums': {'index': 0, 'buffer': []},
                'artists': {'index': 0, 'buffer': []},
                'playlists': {}
            }
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if 'likes' not in data:
                data['likes'] = {'index': 0, 'buffer': []}
            if 'albums' not in data:
                data['albums'] = {'index': 0, 'buffer': []}
            if 'artists' not in data:
                data['artists'] = {'index': 0, 'buffer': []}
            if 'playlists' not in data or not isinstance(data['playlists'], dict):
                data['playlists'] = {}

            for k in ['likes', 'albums', 'artists']:
                if 'index' not in data[k]:
                    data[k]['index'] = 0
                if 'buffer' not in data[k] or not isinstance(data[k]['buffer'], list):
                    data[k]['buffer'] = []

            for pk, pv in list(data['playlists'].items()):
                if not isinstance(pv, dict):
                    data['playlists'][pk] = {'index': 0, 'buffer': []}
                else:
                    if 'index' not in pv:
                        pv['index'] = 0
                    if 'buffer' not in pv or not isinstance(pv['buffer'], list):
                        pv['buffer'] = []

            return data
        except Exception:
            return {
                'likes': {'index': 0, 'buffer': []},
                'albums': {'index': 0, 'buffer': []},
                'artists': {'index': 0, 'buffer': []},
                'playlists': {}
            }

    def _save_progress(self, force=False):
        if not force and self._progress_dirty < PROGRESS_FLUSH_EVERY:
            return
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self._progress, f, ensure_ascii=False, indent=2)
        self._progress_dirty = 0

    def _bump_progress_dirty(self):
        self._progress_dirty += 1
        self._save_progress(force=False)

    def _import_item(self, item):
        # if the item is a string, it is a query from the JSON file
        if isinstance(item, str):
            query = item
            item_name = item
            type_ = 'track'  # Default type for string items
            artists = []  # Default artists for string items
        # else it is an object from Yandex
        else:
            type_ = item.__class__.__name__.casefold()
            item_name = item.name if isinstance(item, Artist) else f'{", ".join([artist.name for artist in item.artists])} - {item.title}'
            artists = item.artists if not isinstance(item, Artist) else []  # Artists for Yandex items

            # A workaround for when track name is too long (100+ characters) there is an exception happening
            # because spotify API can not process it.
            if len(item_name) > 100:
                item_name = item_name[:100]
                logger.info('Name too long... Trimming to 100 characters. May affect search accuracy')

            query = item_name.replace('- ', '')

        found_items = handle_spotify_exception(self.spotify_client.search)(query, type=type_)[f'{type_}s']['items']
        logger.info(f'Importing {type_}: {item_name}...')

        if not self._strict_search and not isinstance(item, Artist) and not len(found_items) and len(artists) > 1:
            query = f'{artists[0].name} {item.title}'
            found_items = handle_spotify_exception(self.spotify_client.search)(query, type=type_)[f'{type_}s']['items']

        logger.info(f'Searching "{query}"...')

        if not len(found_items):
            raise NotFoundException(item_name)

        return found_items[0]['id']

    def _flush_buffer(self, buffer_ids, save_items_callback):
        if not buffer_ids:
            return
        for chunk in chunks(buffer_ids, FLUSH_SPOTIFY_BATCH):
            save_items_callback(self, chunk)
        buffer_ids.clear()

    def _save_uris_to_library(self, uris):
        for uri_chunk in chunks(uris, FLUSH_SPOTIFY_LIBRARY_BATCH):
            handle_spotify_exception(self.spotify_client._put)(
                'me/library',
                args={'uris': ','.join(uri_chunk)}
            )

    def _create_playlist(self, playlist_name):
        return handle_spotify_exception(self.spotify_client._post)(
            'me/playlists',
            payload={'name': playlist_name, 'public': True}
        )

    def _add_uris_to_playlist(self, playlist_id, uris):
        return handle_spotify_exception(self.spotify_client._post)(
            f'playlists/{playlist_id}/items',
            payload={'uris': uris}
        )

    def _add_items_to_spotify(self, items, not_imported_section, save_items_callback, progress_node):
        spotify_items = []

        existing_buffer = progress_node.get('buffer', [])
        if existing_buffer:
            spotify_items.extend(existing_buffer)
            progress_node['buffer'] = []
            self._save_progress(force=True)
            self._flush_buffer(spotify_items, save_items_callback)
            self._save_progress(force=True)

        items.reverse()
        for item in items:
            try:
                spotify_id = self._import_item(item)
                if spotify_id is None:
                    logger.warning('Item ID is None, skipping...')
                    progress_node['index'] = int(progress_node.get('index', 0)) + 1
                    self._bump_progress_dirty()
                    continue
                spotify_items.append(spotify_id)
                logger.info('OK')

            except NotFoundException as exception:
                not_imported_section.append(exception.item_name)
                logger.warning('NO')
            except SpotifyException:
                not_imported_section.append(item.title if not isinstance(item, str) else item)
                logger.warning('NO')

            progress_node['index'] = int(progress_node.get('index', 0)) + 1
            if spotify_items:
                progress_node['buffer'] = list(spotify_items)
            self._bump_progress_dirty()

            if len(spotify_items) >= FLUSH_SPOTIFY_BATCH:
                progress_node['buffer'] = list(spotify_items)
                self._save_progress(force=True)
                self._flush_buffer(spotify_items, save_items_callback)
                progress_node['buffer'] = []
                self._save_progress(force=True)

        if spotify_items:
            progress_node['buffer'] = list(spotify_items)
            self._save_progress(force=True)
            self._flush_buffer(spotify_items, save_items_callback)
            progress_node['buffer'] = []
            self._save_progress(force=True)

        self._save_progress(force=True)

    def import_likes(self):
        self.not_imported['Likes'] = []

        likes_tracks = self.yandex_client.users_likes_tracks().tracks
        tracks = self.yandex_client.tracks([f'{track.id}:{track.album_id}' for track in likes_tracks if track.album_id])
        logger.info('Importing liked tracks...')

        total = len(tracks)
        node = self._progress['likes']
        start_index = int(node.get('index', 0))
        if start_index < 0:
            start_index = 0
        if start_index > total:
            start_index = total

        if start_index > 0:
            logger.info(f'Resuming liked tracks from index {start_index}/{total}...')

        tracks = tracks[start_index:]

        def save_tracks_callback(importer, spotify_tracks):
            logger.info(f'Saving {len(spotify_tracks)} tracks...')
            importer._save_uris_to_library([f'spotify:track:{track_id}' for track_id in spotify_tracks])
            logger.info('OK')

        self._add_items_to_spotify(tracks, self.not_imported['Likes'], save_tracks_callback, node)

    def import_playlists(self):
        playlists = self.yandex_client.users_playlists_list()
        for playlist in playlists:
            spotify_playlist = self._create_playlist(playlist.title)
            spotify_playlist_id = spotify_playlist['id']

            logger.info(f'Importing playlist {playlist.title}...')

            if playlist.cover.type == 'pic':
                filename = f'{playlist.kind}-cover'
                playlist.cover.download(filename, size='400x400')

                handle_spotify_exception(self.spotify_client.playlist_upload_cover_image)(spotify_playlist_id, encode_file_base64_jpeg(filename))

            self.not_imported[playlist.title] = []

            playlist_tracks = playlist.fetch_tracks()
            if not playlist.collective:
                tracks = [track.track for track in playlist_tracks]
            elif playlist.collective and playlist_tracks:
                tracks = self.yandex_client.tracks([track.track_id for track in playlist_tracks])
            else:
                tracks = []

            total = len(tracks)
            key = str(playlist.kind)
            if key not in self._progress['playlists']:
                self._progress['playlists'][key] = {'index': 0, 'buffer': []}

            node = self._progress['playlists'][key]
            start_index = int(node.get('index', 0))
            if start_index < 0:
                start_index = 0
            if start_index > total:
                start_index = total

            if start_index > 0:
                logger.info(f'Resuming playlist {playlist.title} from index {start_index}/{total}...')

            tracks = tracks[start_index:]

            def save_tracks_callback(importer, spotify_tracks):
                logger.info(f'Saving {len(spotify_tracks)} tracks in playlist {playlist.title}...')
                importer._add_uris_to_playlist(spotify_playlist_id, [f'spotify:track:{track_id}' for track_id in spotify_tracks])
                logger.info('OK')

            self._add_items_to_spotify(tracks, self.not_imported[playlist.title], save_tracks_callback, node)

    def import_albums(self):
        self.not_imported['Albums'] = []

        likes_albums = self.yandex_client.users_likes_albums()
        albums = [album.album for album in likes_albums]
        logger.info('Importing albums...')

        total = len(albums)
        node = self._progress['albums']
        start_index = int(node.get('index', 0))
        if start_index < 0:
            start_index = 0
        if start_index > total:
            start_index = total

        if start_index > 0:
            logger.info(f'Resuming albums from index {start_index}/{total}...')

        albums = albums[start_index:]

        def save_albums_callback(importer, spotify_albums):
            logger.info(f'Saving {len(spotify_albums)} albums...')
            importer._save_uris_to_library([f'spotify:album:{album_id}' for album_id in spotify_albums])
            logger.info('OK')

        self._add_items_to_spotify(albums, self.not_imported['Albums'], save_albums_callback, node)

    def import_artists(self):
        self.not_imported['Artists'] = []

        likes_artists = self.yandex_client.users_likes_artists()
        artists = [artist.artist for artist in likes_artists]
        logger.info('Importing artists...')

        total = len(artists)
        node = self._progress['artists']
        start_index = int(node.get('index', 0))
        if start_index < 0:
            start_index = 0
        if start_index > total:
            start_index = total

        if start_index > 0:
            logger.info(f'Resuming artists from index {start_index}/{total}...')

        artists = artists[start_index:]

        def save_artists_callback(importer, spotify_artists):
            logger.info(f'Saving {len(spotify_artists)} artists...')
            importer._save_uris_to_library([f'spotify:artist:{artist_id}' for artist_id in spotify_artists])
            logger.info('OK')

        self._add_items_to_spotify(artists, self.not_imported['Artists'], save_artists_callback, node)

    def import_all(self):
        try:
            for item in self._importing_items.values():
                item()
        finally:
            try:
                self._save_progress(force=True)
            except Exception:
                pass

        self.print_not_imported()

    def print_not_imported(self):
        logger.error('Not imported items:')
        for section, items in self.not_imported.items():
            logger.info(f'{section}:')
            for item in items:
                logger.info(item)

    def import_from_json(self, file_path):
        with open(file_path, 'r', encoding='UTF-8') as file:
            tracks = json.load(file)

        spotify_tracks = []
        not_imported = []

        for track in tracks:
            query = f'{track["artist"]} {track["track"]}'

            try:
                spotify_track_id = self._import_item(query)
                spotify_tracks.append(spotify_track_id)
                logger.info('OK')
            except NotFoundException as exception:
                not_imported.append(exception.item_name)
                logger.warning('NO')
            except SpotifyException:
                not_imported.append(query)
                logger.warning('NO')

            if len(spotify_tracks) >= FLUSH_SPOTIFY_BATCH:
                for chunk in chunks(spotify_tracks, FLUSH_SPOTIFY_BATCH):
                    logger.info(f'Saving {len(chunk)} tracks...')
                    self._save_uris_to_library([f'spotify:track:{track_id}' for track_id in chunk])
                    logger.info('OK')
                spotify_tracks.clear()

        playlist_name = 'Imported from JSON'
        playlist = self._create_playlist(playlist_name)

        if spotify_tracks:
            for chunk in chunks(spotify_tracks, FLUSH_SPOTIFY_BATCH):
                logger.info(f'Saving {len(chunk)} tracks...')
                self._add_uris_to_playlist(playlist['id'], [f'spotify:track:{track_id}' for track_id in chunk])
                logger.info('OK')

        logger.error('Not imported tracks:')
        for track in not_imported:
            logger.info(track)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Creates a playlist for user')
    parser.add_argument('-u', '-s', '--spotify', required=True, help='Username at spotify.com')

    spotify_oauth = parser.add_argument_group('spotify_oauth')
    spotify_oauth.add_argument('--id', required=True, help='Client ID of your Spotify app')
    spotify_oauth.add_argument('--secret', required=True, help='Client Secret of your Spotify app')

    parser.add_argument('-t', '--token', help='Token from music.yandex.com account')

    parser.add_argument('-i', '--ignore', nargs='+', help='Don\'t import some items',
                        choices=['likes', 'playlists', 'albums', 'artists'], default=[])

    parser.add_argument('-T', '--timeout', help='Request timeout for spotify', type=float, default=10)

    parser.add_argument('-S', '--strict-artists-search', help='Search for an exact match of all artists', default=False)

    parser.add_argument('-j', '--json-path', help='JSON file to import tracks from')

    arguments = parser.parse_args()

    try:
        auth_manager = SpotifyOAuth(
            client_id=arguments.id,
            client_secret=arguments.secret,
            redirect_uri=REDIRECT_URI,
            scope='playlist-modify-public user-library-modify user-follow-modify ugc-image-upload',
            username=arguments.spotify,
        )

        if arguments.token is None and arguments.json_path is None:
            raise ValueError('Either the -t (token) or -j (json_path) argument must be specified.')

        spotify_client_ = spotipy.Spotify(auth_manager=auth_manager, requests_timeout=arguments.timeout)
        yandex_client_ = None

        if arguments.token:
            yandex_client_ = Client(arguments.token)
            yandex_client_.init()

        importer_instance = Importer(spotify_client_, yandex_client_, arguments.ignore, arguments.strict_artists_search)

        if arguments.json_path:
            importer_instance.import_from_json(arguments.json_path)
        else:
            importer_instance.import_all()
    except KeyboardInterrupt:
        try:
            logger.error('Interrupted.')
        except Exception:
            pass
    except Exception as e:
        logger.error(f'An unexpected error occurred: {str(e)}')
