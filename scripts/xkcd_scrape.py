from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

from bs4 import BeautifulSoup


def find_image_link(data):
    soup = BeautifulSoup(data, 'html.parser')
    img = soup.select_one('a.img-comic-link > img')
    if img:
        link = img.get('src')
        return link


class Callback(StreamCallback):
    def __init__(self):
        self.link = None

    @staticmethod
    def get_lines(data):
        return data.split('\n')

    def process(self, inputStream, outputStream):
        # read the flowfile
        data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        self.link = find_image_link(data)
        outputStream.write(data.encode('utf-8'))


flowFile = session.get()

if flowFile != None:
    # write the output of Callback to flowfile
    c = Callback()
    flowFile = session.write(
        flowFile, c
    )
    if not c.link:
        log.error('Link not found !!!!!')
    else:
        # add an attribute to the flowfile
        flowFile = session.putAttribute(
            flowFile,
            "asset_link",
            c.link
        )
    session.transfer(flowFile, REL_SUCCESS)
    session.commit()
