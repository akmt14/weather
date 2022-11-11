import os
import psycopg2
from flask import Flask, render_template


class Database:
    def __init__(self, user, password, host, dbname):
        self.user=user
        self.password=password
        self.host=host
        self.dbname=dbname
        self.conn=None
    
    def connect(self):
        """_summary_ - connect to postgres instance
        
        parameters:
            host - hostname, localhost in this instance
            user - postgres username
            password - postgres password
            dbname - postgres database to connect to
        raises:
            e: DatabaseError 
        """
        if self.conn is None:
            try:
                self.conn=psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    dbname=self.dbname,
                    password=self.password,

                )
            except psycopg2.DatabaseError as e:
                raise e
            finally:
                print("*** Connection opened successfully! ***")

    def select_rows(self, query):
        """Run a SQL query to select rows from table."""
        with self.conn.cursor() as cur:
            cur.execute(query)
            records = [row for row in cur.fetchall()]
            cur.close()
            return records

app = Flask(__name__)

@app.route("/weather")
def index():
    #c=psycopg2.connect("postgresql://user:password@host:port/dbname")
    c = Database(host=os.getenv('HOST'), 
                 user=os.getenv('USERNAME'),
                 password=os.getenv('PASSWORD'),
                 dbname=os.getenv('DATABASE'))

    c.connect()
    results = c.select_rows(query='select * from  weather.f_daily LIMIT 10')
    return render_template('index.html', data=results)

if __name__ == '__main__':
    app.run(debug=True)