from cassandra.cluster import Cluster
from cassandra.query import BatchStatement,SimpleStatement
import datetime
import uuid
import os
import re
import gzip
import sys


def main(input_dir,keyspace,tab_name):
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)
    session.execute('TRUNCATE '+tab_name)
    batch = BatchStatement()
    count = 0
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
                split_tup = line_re.split(line)
                if len(split_tup) == 6:
                    batch.add(SimpleStatement("INSERT INTO "+tab_name+" (id,host,datetime,path,bytes) VALUES (%s, %s, %s, %s, %s )"), (uuid.uuid4(),split_tup[1],datetime.datetime.strptime(split_tup[2],"%d/%b/%Y:%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),split_tup[3],int(split_tup[4])))
                    count += 1
                    if count == 200:
                        session.execute(batch)
                        batch.clear()
                        count = 0
    session.execute(batch)
    rows = session.execute('SELECT path, bytes FROM tab_name WHERE host=%s', ['uplherc.upl.com'])

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    tab_name = sys.argv[3]
    main(input_dir,keyspace,tab_name)
