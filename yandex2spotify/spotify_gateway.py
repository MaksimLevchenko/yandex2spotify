from time import sleep

from requests.exceptions import ReadTimeout
from spotipy.exceptions import SpotifyException

from . import config
from .logger import logger
from .utils import chunks, encode_file_base64_jpeg


class SpotifyGateway:
    def __init__(self, spotify_client):
        self.client = spotify_client

    def _call(self, func, *args, **kwargs):
        retry = 1
        while True:
            try:
                return func(*args, **kwargs)
            except SpotifyException as exception:
                if exception.http_status != 429:
                    raise exception

                headers = exception.headers or {}
                retry_after = headers.get('retry-after')
                if retry_after is not None:
                    sleep(int(retry_after) + 1)
                else:
                    sleep(1)
            except ReadTimeout as exception:
                logger.info(f'Read timed out. Retrying #{retry}...')

                if retry > config.MAX_REQUEST_RETRIES:
                    logger.info('Max retries reached.')
                    raise exception

                logger.info('Trying again...')
                retry += 1

    def current_user_id(self):
        return self._call(self.client.me)['id']

    def search_items(self, query, type_):
        return self._call(self.client.search, query, type=type_)[f'{type_}s']['items']

    def save_uris_to_library(self, uris):
        for uri_chunk in chunks(uris, config.FLUSH_SPOTIFY_LIBRARY_BATCH):
            self._call(
                self.client._put,
                'me/library',
                args={'uris': ','.join(uri_chunk)},
            )

    def create_playlist(self, playlist_name):
        return self._call(
            self.client._post,
            'me/playlists',
            payload={'name': playlist_name, 'public': True},
        )

    def add_uris_to_playlist(self, playlist_id, uris):
        return self._call(
            self.client._post,
            f'playlists/{playlist_id}/items',
            payload={'uris': uris},
        )

    def playlist_exists(self, playlist_id):
        try:
            self._call(self.client.playlist, playlist_id, fields='id')
            return True
        except SpotifyException as exception:
            if exception.http_status in (400, 403, 404):
                return False
            raise

    def upload_playlist_cover(self, playlist_id, filename):
        self._call(
            self.client.playlist_upload_cover_image,
            playlist_id,
            encode_file_base64_jpeg(filename),
        )
