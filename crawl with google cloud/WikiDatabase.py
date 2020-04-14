import mysql.connector

class WikiDatabase:
    def __init__(self, host, user, password, database):
        self.db = mysql.connector.connect(
            host=host,
            user=user,
            passwd=password,
            database=database
        )
        
        self.cursor = self.db.cursor()
        
    def insert_hrefs(self, hrefs):
        sql = "INSERT INTO links (href) VALUES (%s) ON DUPLICATE KEY UPDATE href=href"
        val = [(href,) for href in hrefs]
        self._execute_many_query(sql, val)
        
    def update_hrefs(self, hrefs, pageids):
        sql = "INSERT INTO links (href, pageid) VALUES (%s, %s) ON DUPLICATE KEY UPDATE pageid=VALUES(pageid)"
        val = list(zip(hrefs, pageids))
        self._execute_many_query(sql, val)

    def insert_article(self, articles_data):
        sql = """INSERT INTO articles (title, html, pageid) VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE title=VALUES(title), html=VALUES(html), pageid=VALUES(pageid)"""
        self._execute_many_query(sql, articles_data)

    def get_null_hrefs(self, limit=100):
        sql = "SELECT href FROM links WHERE pageid IS NULL LIMIT {}".format(limit)
        self._execute_query(sql)
        return [href.decode() for href, in self.cursor.fetchall()]

    def _execute_query(self, sql):
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print("[ERROR]\t{}".format(e))
        
    def _execute_many_query(self, sql, values):
        try:
            self.cursor.executemany(sql, values)
            self.db.commit()
        except Exception as e:
            print("[ERROR]\t{}".format(e))