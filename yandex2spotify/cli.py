import argparse
import os

import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth
from yandex_music import Client

from .config import REDIRECT_URI
from .importer_service import Importer
from .logger import logger


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if normalized in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return default


def _load_env_defaults():
    load_dotenv()

    raw_ignore = os.getenv('IGNORE_ITEMS', '')
    ignore_items = [item.strip() for item in raw_ignore.replace(',', ' ').split() if item.strip()]

    timeout_value = os.getenv('SPOTIFY_TIMEOUT')
    timeout = 10.0
    if timeout_value:
        timeout = float(timeout_value)

    return {
        'spotify_username': os.getenv('SPOTIFY_USERNAME'),
        'spotify_client_id': os.getenv('SPOTIFY_CLIENT_ID'),
        'spotify_client_secret': os.getenv('SPOTIFY_CLIENT_SECRET'),
        'yandex_token': os.getenv('YANDEX_TOKEN'),
        'timeout': timeout,
        'strict_artists_search': _to_bool(os.getenv('STRICT_ARTISTS_SEARCH'), default=False),
        'json_path': os.getenv('JSON_PATH'),
        'ignore_items': ignore_items,
        'redirect_uri': os.getenv('SPOTIFY_REDIRECT_URI', REDIRECT_URI),
    }


def build_parser(defaults):
    parser = argparse.ArgumentParser(description='Creates a playlist for user')
    parser.add_argument('-u', '-s', '--spotify', default=defaults['spotify_username'], help='Username at spotify.com')

    spotify_oauth = parser.add_argument_group('spotify_oauth')
    spotify_oauth.add_argument('--id', default=defaults['spotify_client_id'], help='Client ID of your Spotify app')
    spotify_oauth.add_argument('--secret', default=defaults['spotify_client_secret'], help='Client Secret of your Spotify app')

    parser.add_argument('-t', '--token', default=defaults['yandex_token'], help='Token from music.yandex.com account')

    parser.add_argument(
        '-i',
        '--ignore',
        nargs='+',
        help='Don\'t import some items',
        choices=['likes', 'playlists', 'albums', 'artists'],
        default=defaults['ignore_items'],
    )

    parser.add_argument('-T', '--timeout', help='Request timeout for spotify', type=float, default=defaults['timeout'])
    parser.add_argument(
        '-S',
        '--strict-artists-search',
        help='Search for an exact match of all artists',
        default=defaults['strict_artists_search'],
    )
    parser.add_argument('-j', '--json-path', help='JSON file to import tracks from', default=defaults['json_path'])

    return parser


def main():
    try:
        defaults = _load_env_defaults()
        parser = build_parser(defaults)
        arguments = parser.parse_args()

        missing_spotify_values = []
        if not arguments.spotify:
            missing_spotify_values.append('SPOTIFY_USERNAME/--spotify')
        if not arguments.id:
            missing_spotify_values.append('SPOTIFY_CLIENT_ID/--id')
        if not arguments.secret:
            missing_spotify_values.append('SPOTIFY_CLIENT_SECRET/--secret')

        if missing_spotify_values:
            raise ValueError(
                f'Missing required Spotify credentials: {", ".join(missing_spotify_values)}.'
            )

        allowed_ignore_values = {'likes', 'playlists', 'albums', 'artists'}
        invalid_ignore_values = [item for item in arguments.ignore if item not in allowed_ignore_values]
        if invalid_ignore_values:
            raise ValueError(
                f'Invalid values in IGNORE_ITEMS/--ignore: {", ".join(invalid_ignore_values)}.'
            )

        auth_manager = SpotifyOAuth(
            client_id=arguments.id,
            client_secret=arguments.secret,
            redirect_uri=defaults['redirect_uri'],
            scope='playlist-modify-public user-library-modify user-follow-modify ugc-image-upload',
            username=arguments.spotify,
        )

        if arguments.token is None and arguments.json_path is None:
            raise ValueError('Either the -t (token) or -j (json_path) argument must be specified.')

        spotify_client = spotipy.Spotify(auth_manager=auth_manager, requests_timeout=arguments.timeout)
        yandex_client = None

        if arguments.token:
            yandex_client = Client(arguments.token)
            yandex_client.init()

        strict_artists_search = _to_bool(arguments.strict_artists_search, default=False)
        importer_instance = Importer(spotify_client, yandex_client, arguments.ignore, strict_artists_search)

        if arguments.json_path:
            importer_instance.import_from_json(arguments.json_path)
        else:
            importer_instance.import_all()
    except KeyboardInterrupt:
        try:
            logger.error('Interrupted.')
        except Exception:
            pass
    except Exception as error:
        logger.error(f'An unexpected error occurred: {str(error)}')
