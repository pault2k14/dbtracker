import datetime
import logging
import sys
import smtplib
from io import StringIO
from dateutil.parser import parse
from dbtracker.configurator import read_config
from dbtracker.dbproviders import Storage, Mysql, Postgres
from dbtracker.console_graph import print_bars
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


logger = logging.getLogger(__name__)


class Cli(object):

    def __init__(self, args):
        self.args = args
        try:
            config = read_config(
                file=args.config) if args.config else read_config()
            self.storage = Storage(**config._sections['storage'])
            self.mysql = Mysql(**config._sections['mysql'])
            self.pg = Postgres(**config._sections['postgresql'])
        except KeyError:
            logger.error("Invalid configuration")
            sys.exit(1)

    def main(self):
        self.low = 0
        self.high = 0
        
        args = self.args
        if args.save:
            self.save()
        elif args.history:
            self.history()
        elif args.growth:
            self.growth()
            if args.min or args.max:
                if self.check_threshold():
                    self.email()
        elif args.count:
            self.count()
        elif args.dates:
            self.dates()
        else:
            print("Please pass -h for help")

    def email(self):
        
        old_stdout = sys.stdout
        
        result = StringIO()
        sys.stdout = result
        
        self.growth()
        
        sys.stdout = old_stdout
        result_string = result.getvalue()
        
        me = "noreply@pdx.edu"
        you = "pbt@pdx.edu"
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "dbtracker: Database exceeded threshold"
        msg['From'] = me
        msg['To'] = you
        
        part1 = MIMEText(result_string, 'plain')
        msg.attach(part1)
        s = smtplib.SMTP('localhost')
        s.sendmail(me, you, msg.as_string())
        
        s.quit()
    
    def find_high_and_low(self, database):
        
        for key, value in database.items():
            if value < self.low:
                self.low = value
            if value > self.high:
                self.high = value
    
    def check_threshold(self):
        min = self.args.min
        max = self.args.max
        low = self.low
        high = self.high
        
        if min != None and min >= low:
            return True
        
        if max != None and max <= high:
            return True
            
        return False
        
    def save(self):
        now = datetime.datetime.now()

        mysql_tables = self.mysql.get_tables()
        pg_tables = self.pg.get_tables()

        self.storage.save(mysql_tables, pg_tables, timestamp=now)

    def history(self):
        timestamps = self.storage.get_history(self.args.history)
        for i, timestamp in enumerate(timestamps):
            date = timestamp['datetime']
            print("{}: {} [{}]".format(i, date, date.strftime("%A")))

    def growth(self):
        runs = self.args.growth.split("-")
        if len(runs) == 1:
            r1 = 0
            r2 = int(runs[0])
        elif len(runs) == 2:
            r1 = int(runs[0])
            r2 = int(runs[1])
        else:
            logger.warning("Cant parse range")
            sys.exit(1)
        d1, d2 = self.get_datetime_from_run(r1, r2)
        mysql_diff, pg_diff = self.run_difference(d1, d2)
        self.find_high_and_low(mysql_diff)
        self.find_high_and_low(pg_diff)
        self.diff_printer(d1, d2, mysql=mysql_diff, pg=pg_diff)

    def email_growth(self):
        runs = self.args.growth.split("-")
        if len(runs) == 1:
            r1 = 0
            r2 = int(runs[0])
        elif len(runs) == 2:
            r1 = int(runs[0])
            r2 = int(runs[1])
        else:
            logger.warning("Cant parse range")
            sys.exit(1)
        d1, d2 = self.get_datetime_from_run(r1, r2)
        mysql_diff, pg_diff = self.run_difference(d1, d2)
        self.find_high_and_low(mysql_diff)
        self.find_high_and_low(pg_diff)
        self.email_diff_printer(d1, d2, mysql=mysql_diff, pg=pg_diff)
    
    def email_diff_printer(self, d1, d2, mysql=None, pg=None):
        print("==== PostgreSQL [{}] - [{}] ====".format(d1, d2))
        print_bars(pg)
        print("==== MySQL [{}] - [{}] ====".format(d1, d2))
        print_bars(mysql)
        
    def diff_printer(self, d1, d2, mysql=None, pg=None):
        print("==== PostgreSQL [{}] - [{}] ====".format(d1, d2))
        print_bars(pg)
        print("==== MySQL [{}] - [{}] ====".format(d1, d2))
        print_bars(mysql)

    def count_printer(self, d, mysql=None, pg=None):
        print("==== PostgreSQL [{}] ====".format(d))
        print_bars(pg)
        print("==== MySQL [{}] ====".format(d))
        print_bars(mysql)

    def dates(self):
        dates = self.args.dates.split(' - ')
        if len(dates) == 2:
            d1 = parse(dates[0], fuzzy=True)
            d2 = parse(dates[1], fuzzy=True)
            mysql_diff, pg_diff = self.run_difference(d1, d2)
            self.diff_printer(d1, d2, mysql=mysql_diff, pg=pg_diff)
        else:
            logger.warning("Cant parse range")
            sys.exit(1)

    def get_datetime_from_run(self, r1, r2):
        hrange = self.storage.get_history(max([r1, r2]) + 1)
        d1 = hrange[r1]['datetime']
        d2 = hrange[r2]['datetime']
        return d1, d2

    def run_difference(self, d1, d2):
        mysql_diff = self.difference(d1, d2, 'mysql')
        pg_diff = self.difference(d1, d2, 'pg')
        return mysql_diff, pg_diff

    def difference(self, d1, d2, provider):
        d1_tables = self.storage.get_timestamp(d1, provider)
        d2_tables = self.storage.get_timestamp(d2, provider)

        d1_totals = self.storage.db_rowcount(d1_tables)
        d2_totals = self.storage.db_rowcount(d2_tables)

        diff = {}
        for key in d1_totals:
            diff[key] = d1_totals[key] - d2_totals.get(key, 0)
        return diff

    def count(self):
        now = datetime.datetime.now()
        mysql_tables = self.mysql.get_tables()
        pg_tables = self.pg.get_tables()

        mysql_totals = self.storage.db_rowcount(mysql_tables)
        pg_totals = self.storage.db_rowcount(pg_tables)

        self.count_printer(now, mysql=mysql_totals, pg=pg_totals)
