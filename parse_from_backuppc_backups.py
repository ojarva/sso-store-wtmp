import glob
import os.path
import subprocess
import httplib2
h = httplib2.Http()
import socket

for pc in glob.glob("/var/lib/backuppc/pc/*"):
    pcname = os.path.basename(pc)
    try:
        server_ip = socket.gethostbyaddr(pcname)[2][0]
    except Exception, e:
        print "Invalid IP", pcname, e
        continue

    pcname = pcname.split(".")[0]
    print pcname
    for backup in glob.glob(pc+"/*/f*f/fvar/flog/fwtmp*"):
        p = subprocess.Popen(["/usr/share/backuppc/bin/BackupPC_zcat", backup], stdout=subprocess.PIPE)
        (stdout, _) = p.communicate()
        open("tmpfile", "w").write(stdout)

        p = subprocess.Popen(["last", "-i", "-f", "tmpfile"], stdout=subprocess.PIPE)
        (stdout, _) = p.communicate()
#        print stdout
        try:
            year = stdout.split("begin")[1].strip().split()[-1]
        except:
            print "Incomplete wtmp"
        print h.request(SERVER_HOSTNAME_URL+"/?server=%s&server_ip=%s&year=%s" % (pcname, server_ip, year), "POST", body=stdout)
