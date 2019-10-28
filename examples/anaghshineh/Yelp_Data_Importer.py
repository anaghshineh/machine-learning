import json
import os
import psycopg2
from tqdm._tqdm_notebook import tqdm_notebook


class YelpDataImporter:

    def __init__(self, conn, datafiles, dataset_path, per_commit=100000):
        self.conn = conn
        self.datafiles = datafiles
        self.dataset_path = dataset_path
        self.per_commit = per_commit

    def populate(self):
        if 'business.json' in self.datafiles:
            print('\nImporting data into business table...')
            self._populate_business_table()
        if 'review.json' in self.datafiles:
            print('\nImporting data into review table...')
            self._populate_review_table()
        if 'user.json' in self.datafiles:
            print('\nImporting data into user_info table...')
            self._populate_user_table()

    def _populate_business_table(self):
        cur = self.conn.cursor()
        n_processed = 0
        total_rows = 0
        with open(os.path.join(self.dataset_path, 'business.json'),'r',encoding='utf8') as f:
            for line in f:
                total_rows += 1
            f.seek(0)
            with tqdm_notebook(total=total_rows) as pbar:
                for line in f:
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError as err:
                        print('Encountered error decoding line in business.json')
                        print('\n' + err)
                        print('\n' + line[:60] + '\n')

                    try:
                        if data['attributes'] is not None:
                            if data['attributes'].get('RestaurantsPriceRange2', None) is not None:
                                if data['attributes']['RestaurantsPriceRange2'] == "None":
                                    data['attributes']['RestaurantsPriceRange2'] = None
                            else:
                                price_range = None
                            price_range = data['attributes'].get('RestaurantsPriceRange2', None)

                        else:
                            price_range = None

                        cur.execute("""
                            INSERT INTO business (business_id, name, address, city, state, postal_code, lat, long, stars, review_count, is_open, categories, price_range) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", (data['business_id'], data['name'], data['address'], data['city'], data['state'], data['postal_code'], data['latitude'], data['longitude'], str(data['stars']), data['review_count'], str(data['is_open']), data['categories'], price_range))
                        n_processed += 1
                        pbar.update(1)
                        if n_processed % self.per_commit == 0:
                            self.conn.commit()
                    except psycopg2.Error as e:
                        print(e.pgerror)
        self.conn.commit()
        print('Finished populating business table with {} total observations.'.format(total_rows))
        cur.close()

    def _populate_review_table(self):
        cur = self.conn.cursor()
        n_processed = 0
        total_rows = 0
        with open(os.path.join(self.dataset_path, 'review.json'), 'r', encoding='utf8') as f:
            for line in f:
                total_rows += 1
            f.seek(0)
            with tqdm_notebook(total=total_rows) as pbar:
                for line in f:
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError as err:
                        print('Encountered error decoding line in review.json')
                        print('\n' + err)
                        print('\n' + line[:60] + '\n')

                    try:
                        cur.execute("""
                            INSERT INTO review (review_id, user_id, business_id, stars, review_date, review_text, useful, funny, cool) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, (data['review_id'], data['user_id'], data['business_id'], data['stars'], data['date'], data['text'], data['useful'], data['funny'], data['cool']))
                        n_processed += 1
                        pbar.update(1)
                        if n_processed % self.per_commit == 0:
                            self.conn.commit()
                    except psycopg2.Error as e:
                        print(e.pgerror)
        self.conn.commit()
        print('Finished populating review table with {} total observations.'.format(n_processed))
        cur.close()

    def _populate_user_table(self):
        cur = self.conn.cursor()
        n_processed = 0
        total_rows = 0
        with open(os.path.join(self.dataset_path, 'user.json'),'r',encoding='utf8') as f:
            for line in f:
                total_rows += 1
            f.seek(0)
            with tqdm_notebook(total=total_rows) as pbar:
                for line in f:
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError as err:
                        print('Encountered error decoding line in user.json')
                        print('\n' + err)
                        print('\n' + line[:60] + '\n')

                    try:
                        cur.execute("""
                            INSERT INTO user_info (user_id, name, review_count, yelping_since, friends, useful, funny, cool, fans, elite, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, (data['user_id'], data['name'], data['review_count'], data['yelping_since'], data['friends'], data['useful'], data['funny'], data['cool'], data['fans'], data['elite'], data['average_stars'], data['compliment_hot'], data['compliment_more'], data['compliment_profile'], data['compliment_cute'], data['compliment_list'], data['compliment_note'], data['compliment_plain'], data['compliment_cool'], data['compliment_funny'], data['compliment_writer'], data['compliment_photos']))
                        n_processed += 1
                        pbar.update(1)
                        if n_processed % self.per_commit == 0:
                            self.conn.commit()
                    except psycopg2.Error as e:
                        print(e.pgerror)
        self.conn.commit()
        print('Finished populating user_info table with {} total observations.'.format(n_processed))
        cur.close()

if __name__ == '__main__':
    pass
