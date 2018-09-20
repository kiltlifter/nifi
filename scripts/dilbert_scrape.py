from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback


class Callback(StreamCallback):
    def __init__(self):
        pass

    @staticmethod
    def get_lines(data):
        return data.split('\n')

    def process(self, inputStream, outputStream):
        # read the flowfile
        data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        # write the contents of data to flowfile
        outputStream.write(bytearray(data.encode('utf-8')))


flowFile = session.get()

if flowFile != None:
    # write the output of Callback to flowfile
    flowFile = session.write(
        flowFile, Callback()
    )
    # add an attribute to the flowfile
    flowFile = session.putAttribute(
        flowFile,
        "filename",
        'flowfile_' + flowFile.getAttribute('uuid') + '.txt'
    )
    session.transfer(flowFile, REL_SUCCESS)
    session.commit()
