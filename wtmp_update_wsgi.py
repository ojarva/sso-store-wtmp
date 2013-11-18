from wtmpparser import WtmpParser
import random
import redis
import time
import datetime
import re
from config import Config
from instrumentation import *
import _mysql

class WtmpUpdate:
    def __init__(self, server_hostname, server_ip):
        self._db = None
        self.server_hostname = server_hostname
        self.server_ip = server_ip
        self.config = Config()
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))
        self.redis.rpush("ip-resolve-queue", server_ip)

    @property
    def db(self):
        if self._db:
            return self._db
        self._db = _mysql.connect(self.config.get("mysql-hostname"), self.config.get("mysql-username"), self.config.get("mysql-password"), self.config.get("mysql-database"))
        return self._db

    def escape(self, string):
        if string is None:
            return "null"
        return "'"+_mysql.escape_string(str(string))+"'"

    @timing("wtmp.update.session.open")
    def open_session(self, username, console, remote_ip, start_time, end_time=None, no_logout=False):
        statsd.incr("wtmp.update.session.open")
        now = datetime.datetime.now()
        if end_time is None:
            end_time_real = None
        else:
            end_time_real = now
        last_known = now
        if no_logout:
            last_known = None
        if end_time:
            last_known = end_time

        self.db.query("INSERT INTO wtmp_per_host VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (self.escape(username), self.escape(console), self.escape(remote_ip), self.escape(self.server_hostname), self.escape(self.server_ip), self.escape(start_time), self.escape(end_time), self.escape(now), self.escape(end_time_real), self.escape(last_known), self.escape(no_logout)))
        self.db.store_result()
        self.redis.rpush("ip-resolve-queue", remote_ip)

    @timing("wtmp.update.session.close")
    def close_session(self, username, console, remote_ip, end_time, no_logout=False):
        statsd.incr("wtmp.update.session.close")
        now = datetime.datetime.now()
        end_time_real = now
        self.db.query("UPDATE wtmp_per_host SET end_time_real=%s, end_time=%s, no_logout=%s WHERE server_ip=%s AND end_time_real is NULL AND username=%s AND console=%s AND remote_ip=%s" % (self.escape(end_time_real), self.escape(end_time), self.escape(no_logout), self.escape(self.server_ip), self.escape(username), self.escape(console), self.escape(remote_ip)))
        self.db.store_result()

    @timing("wtmp.update.session.update")
    def update_session(self, username, console, remote_ip):
        statsd.incr("wtmp.update.session.update")
        su_key = "wtmp2-last-last-update-%s-%s-%s-%s" % (self.server_ip, username, console, remote_ip)
        last_update = self.redis.get(su_key)
        if not last_update:
            last_update = 0
        if last_update - time.time() > 1000 * random.random():
            now = datetime.datetime.now()
            self.db.query("UPDATE wtmp_per_host SET last_known=%s WHERE server_ip=%s AND end_time_real is NULL AND username=%s AND console=%s AND remote_ip=%s" % (self.escape(now), self.escape(self.server_ip), self.escape(username), self.escape(console), self.escape(remote_ip)))
            self.db.store_result()
            self.redis.setex(su_key, time.time(), 3888000) # 45 days.

    def update(self, c):
        statsd.incr("wtmp.update.update")
        update_key = "wtmp4-exists-%s-%s_%s_%s_%s" % (self.server_ip, c["username"], c["console"], c["remote_ip"], c["login_time"])
        logout_key = "%s_%s" % (c["logout_time"], c["no_logout"])

        if not self.redis.exists(update_key):
            # Not connected. Open a new session.
            self.open_session(c["username"], c["console"], c["remote_ip"], c["login_time"], c["logout_time"], c["no_logout"])
            self.redis.setex(update_key, logout_key, 3888000) # 45 days
        else:
            update_value = self.redis.get(update_key)
            if update_value == logout_key:
                # Session did not change.
                if c["logged_in"]:
                    # Session is still active.
                    self.update_session(c["username"], c["console"], c["remote_ip"])
                else:
                    # Session did not change, and it is not active.
                    pass
            else:
                # Session did change.
                if not c["logged_in"]:
                    self.close_session(c["username"], c["console"], c["remote_ip"], c["logout_time"], c["no_logout"])
                else:
                    # WTF?
                    pass
                self.redis.setex(update_key, logout_key, 3888000) # 45 days.

def is_valid_hostname(hostname):
    if len(hostname) > 255:
        return False
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

@timing("wtmp.update.main")
def application(environ, start_response):
    statsd.incr("wtmp.update.main.counter")
    start_response("200 OK", [("Content-Type", "text/plain")])
    query_string = environ["QUERY_STRING"]
    query_string = query_string.split("&")
    hostname = False
    server_ip = environ["REMOTE_ADDR"]
    year = None
    for item in query_string:
        item = item.split("=")
        if len(item) == 2:
            if item[0] == "server":
                if is_valid_hostname(item[1]):
                    hostname = item[1]
            elif item[0] == "year":
                year = item[1]
            elif item[0] == "server_ip":
                server_ip = item[1]
    if not hostname:
        return ["Invalid hostname"]

    wtmp_update = WtmpUpdate(hostname, server_ip)
    wtmp = WtmpParser(environ["wsgi.input"], year)
    for entry in reversed(wtmp.entries):
        wtmp_update.update(entry)
    return ["OK"]

