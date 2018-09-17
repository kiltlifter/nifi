from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

import re
from datetime import datetime as dt
from pytz import timezone
import json


def format_ical_event(ical):
    begin = re.compile('^BEGIN:VEVENT')
    end = re.compile('^END:VEVENT')
    events = []
    event = []
    capture = False
    pacific = timezone('America/Los_Angeles')
    fmt = '%Y%m%dT%H%M%SZ'
    for line in ical:
        if begin.match(line):
            capture = True
        elif end.match(line):
            events.append(event)
            event = []
        else:
            if capture and re.match('^(?:SUMMARY|DTSTAMP)', line):
                try:
                    e = line.split(':')
                    item = e[1]
                    try:
                        t = dt.strptime(item, fmt)
                        t = pacific.fromutc(t)
                        event.append(t.isoformat())
                    except ValueError:
                        event.append(item)
                except IndexError:
                    event.append('')
    return events


def added_formatting(events):
    output = []
    for event in events:
        if not event[1].lower() == 'closed':
            output.append({
                'time': event[0],
                'summary': event[1]
            })
    return output


class Callback(StreamCallback):
    def __init__(self):
        pass

    @staticmethod
    def get_lines(data):
        return data.split('\r\n')

    def process(self, inputStream, outputStream):
        data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        data = self.get_lines(data)
        events = format_ical_event(data)
        results = sorted(added_formatting(events), key=lambda a: a['time'])
        outputStream.write(bytearray(json.dumps(results, indent=4).encode('utf-8')))


flowFile = session.get()

if flowFile != None:
    flowFile = session.write(flowFile, Callback())
    session.transfer(flowFile, REL_SUCCESS)
    session.commit()

