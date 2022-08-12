#!/usr/bin/env python3

import os

import requests

RADIANCE_HOST = os.getenv("RADIANCE_HOST")
RADIANCE_USER = os.getenv("RADIANCE_USER")
RADIANCE_PASSWORD = os.getenv("RADIANCE_PASSWORD")

LEADER_STATS = """
SELECT
    leader,
    median(diff),
    count()
FROM
(
    WITH
        leader,
        minIf(timestamp, type = 'firstShredReceived') AS tsReceived,
        minIf(timestamp, type = 'completed') AS tsCompleted
    SELECT
        source,
        slot,
        leader,
        toUnixTimestamp64Milli(tsCompleted) - toUnixTimestamp64Milli(tsReceived) AS diff
    FROM slot_status
    WHERE toDate(timestamp) = today() - 5
    GROUP BY
        slot,
        leader,
        source
)
GROUP BY leader
ORDER BY median(diff)
FORMAT JSON
"""


class RadianceClient():
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.session = requests.session()
        self.session.user_agent = "radiance-client/v0.1"
        self.session.auth = (self.user, self.password)

    def query(self, query):
        r = self.session.post(self.host, data=query)
        if r.status_code != 200:
            raise Exception("Failed to get leader stats: {}".format(r.text))
        return r.json()['data']

    def get_leader_stats(self):
        return self.query(LEADER_STATS)


if __name__ == '__main__':
    import pprint

    c = RadianceClient(RADIANCE_HOST, RADIANCE_USER, RADIANCE_PASSWORD)
    r = c.get_leader_stats()
    pprint.pprint(r)
