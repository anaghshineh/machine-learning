import psycopg2


class YelpDBMaker:

    def __init__(self, conn, datafiles):
        self.conn = conn
        self.datafiles = datafiles

    def create(self):
        if 'business.json' in self.datafiles:
            datafile = 'business.json'
            try:
                self._create_business_table()
            except psycopg2.Error as err:
                print(err.pgerror)
                while True:
                    input_text = input('Business table already exists. Drop existing table and recreate? (y/n) ')
                    answer = input_text
                    if answer != 'y' and answer != 'n':
                        print("Please enter 'y' or 'n' in response to the prompt.")
                        continue
                    else:
                        break
                if answer == 'y':
                    self._drop_existing_tables(datafile)
                    try:
                        self._create_business_table()
                    except psycopg2.Error as err:
                        print(err.pgerror)

        if 'review.json' in self.datafiles:
            datafile = 'review.json'
            try:
                self._create_review_table()
            except psycopg2.Error as err:
                print(err.pgerror)
                while True:
                    input_text = input('Review table already exists. Drop existing table and recreate? (y/n) ')
                    answer = input_text
                    if answer != 'y' and answer != 'n':
                        print("Please enter 'y' or 'n' in response to the prompt.")
                        continue
                    else:
                        break
                if answer == 'y':
                    self._drop_existing_tables(datafile)
                    try:
                        self._create_review_table()
                    except psycopg2.Error as err:
                        print(err.pgerror)

        if 'user.json' in self.datafiles:
            datafile = 'user_info.json'
            try:
                self._create_user_table()
            except psycopg2.Error as err:
                print(err.pgerror)
                while True:
                    input_text = input('User_info table already exists. Drop existing table and recreate? (y/n) ')
                    answer = input_text
                    if answer != 'y' and answer != 'n':
                        print("Please enter 'y' or 'n' in response to the prompt.")
                        continue
                    else:
                        break
                if answer == 'y':
                    self._drop_existing_tables(datafile)
                    try:
                        self._create_user_table()
                    except psycopg2.Error as err:
                        print(err.pgerror)

    def _drop_existing_tables(self, datafile):
        cur = self.conn.cursor()
        try:
            print('Dropping table ' + datafile.split('.')[0].lower() + '...')
            cur.execute('DROP TABLE IF EXISTS ' + datafile.split('.')[0].lower() + ';')
            self.conn.commit()
        except psycopg2.Warning as warn:
            print(warn.pgerror)
        except psycopg2.Error as err:
            print(err.pgerror)
            self.conn.rollback()

    def _create_business_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE business(
                business_id char(22) PRIMARY KEY,
                name text,
                address text,
                city text,
                state text,
                postal_code text,
                lat real,
                long real,
                stars real,
                review_count integer,
                is_open boolean,
                categories text,
                price_range text
            );
        """)
        self.conn.commit()
        cur.close()

    def _create_review_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE review (
                review_id char(22) PRIMARY KEY,
                user_id char(22),
                business_id char(22),
                stars integer,
                review_date date,
                review_text text,
                useful integer,
                funny integer,
                cool integer
            );
        """)
        self.conn.commit()
        cur.close()

    def _create_user_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE user_info (
                user_id char(22) PRIMARY KEY,
                name text,
                review_count integer,
                yelping_since date,
                friends text,
                useful integer,
                funny integer,
                cool integer,
                fans integer,
                elite text,
                average_stars real,
                compliment_hot integer,
                compliment_more integer,
                compliment_profile integer,
                compliment_cute integer,
                compliment_list integer,
                compliment_note integer,
                compliment_plain integer,
                compliment_cool integer,
                compliment_funny integer,
                compliment_writer integer,
                compliment_photos integer
            );
        """)
        self.conn.commit()
        cur.close()

if __name__ == '__main__':
    pass
