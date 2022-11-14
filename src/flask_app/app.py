import os
import psycopg2
from flask import Flask, render_template
import json

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
            records = cur.fetchall()
            cur.close()
            return records

app = Flask(__name__)

@app.route("/project")
def project():
    #c=psycopg2.connect("postgresql://user:password@host:port/dbname")
    c = Database(host=os.getenv('HOST'), 
                 user=os.getenv('USERNAME'),
                 password=os.getenv('PASSWORD'),
                 dbname=os.getenv('DATABASE'))

    c.connect()
    cols = ['Date', 'Latitude', 'Longitude', 'Max_Temperature', 'Min_Temperature', 'Avg_Temperature', 'City']
    results = c.select_rows(query="""SELECT Date, Latitude, Longitude, Max_Temperature, Min_Temperature, Avg_Temperature, City FROM weather.mv_temp WHERE City ilike '%York%' LIMIT 10""")
    json_data=[]
    for result in results:
        json_data.append(dict(zip(cols,result)))
    return render_template('project.html', data=json.dumps(json_data, default=str))

if __name__ == '__main__':
    app.run(debug=True)
