import datetime

class WtmpParser:
    def __init__(self, file, year=None):
        self.file = file
        self.year = year

    @property
    def entries(self):
        entries = []

        for line in self.file:
            line = line.strip().split()
            if len(line) < 9:
                continue
            entry = {"logged_in": False, "no_logout": False, "logout_time": None, "login_time": None}
            entry["username"] = line[0]
            entry["console"] = line[1]
            entry["remote_ip"] = line[2]
            if not "." in entry["remote_ip"]:
                continue
            if entry["remote_ip"] == "0.0.0.0" or line[3] == "0.0.0.0":
                continue

            login_time_p = line[4] + " " + line[5] + " " +line[6]
            now = datetime.datetime.now()

            if self.year:
                year = self.year
            else:
                year = datetime.datetime.now().strftime("%Y")
            login_time = datetime.datetime.strptime(login_time_p + " "+year, "%b %d %H:%M %Y")
            if login_time > now and not self.year:
                year = str(int(year) - 1)
                login_time = datetime.datetime.strptime(login_time_p + " "+year, "%b %d %H:%M %Y")
            entry["login_time"] = login_time

            if line[7] == "gone":
                entry["no_logout"] = True

            elif line[7] == "still":
                entry["logged_in"] = True
            elif line[7] == "-":
                signed_in = line[9].replace("(", "").replace(")", "")
                signed_in_delta = datetime.timedelta()
                if "+" in signed_in:
                    signed_in = signed_in.split("+")
                    signed_in_delta += datetime.timedelta(days=int(signed_in[0]))
                    signed_in = signed_in[1]
                signed_in = signed_in.split(":")
                signed_in_delta += datetime.timedelta(hours=int(signed_in[0]))
                signed_in_delta += datetime.timedelta(minutes=int(signed_in[1]))

                entry["logout_time"] = login_time + signed_in_delta

            else:
                print "Invalid line:", line
                continue
            entries.append(entry)
        return entries
