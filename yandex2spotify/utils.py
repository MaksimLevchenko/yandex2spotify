from base64 import b64encode

from PIL import Image


def chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def encode_file_base64_jpeg(filename):
    image = Image.open(filename)
    if image.format != 'JPEG':
        image.convert('RGB').save(filename, 'JPEG')

    with open(filename, 'rb') as file:
        return b64encode(file.read())
