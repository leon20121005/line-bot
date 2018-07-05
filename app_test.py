from flask import Flask, g

from bs4 import BeautifulSoup
from threading import Thread
import os
import random
import requests
import sqlite3

DATABASE = 'database.db'

app = Flask(__name__)

def initialize_database():
    with app.app_context():
        connection = get_database()
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT NOT NULL, number TEXT NOT NULL)')
        connection.commit()
        connection.close()

def get_database():
    database = getattr(g, '_database', None)
    if database is None:
        database = g._database = sqlite3.connect(DATABASE)
    return database

@app.teardown_appcontext
def close_connection(exception):
    database = getattr(g, '_database', None)
    if database is not None:
        database.close()

@app.route('/callback')
def callback():
    connection = get_database()
    cursor = connection.cursor()
    random_number = random.randint(1, 2951)
    link = cursor.execute('SELECT link FROM links WHERE id = %s' % random_number)
    link = link.fetchone()[0]
    connection.close()
    return link

@app.route('/list')
def list_links():
    connection = get_database()
    cursor = connection.cursor()
    links = cursor.execute('SELECT * FROM links')
    result = ""
    for link in links:
        result += '(%s, %s, %s)' % (link[0], link[1], link[2]) + '<br>'
    connection.close()
    return result

@app.route('/update')
def update_links():
    thread = Thread(target = async_update_links)
    thread.start()
    return 'Start updating database'

def async_update_links():
    with app.app_context():
        home_url = 'http://www.dmm.co.jp/digital/videoa/-/list/=/sort=ranking'
        number_page = 15
        html_parser = 'html.parser'
        video_list = []

        for page in range(1, number_page + 1):
            print('Crawling page %s / %s' % (page, number_page))
            url = home_url + '/page=' + str(page)
            request = requests.get(url)
            if request.status_code == requests.codes.ok:
                soup = BeautifulSoup(request.content, html_parser)
                videos = soup.find_all('p', class_ = 'tmb')

                for video in videos:
                    url = video.find('a')['href']
                    request = requests.get(url)
                    if request.status_code == requests.codes.ok:
                        soup = BeautifulSoup(request.content, html_parser)
                        image = soup.find('div', id = 'sample-video')
                        image_url = image.find('a')['href']
                        number = parse_number(image_url)
                        if number is None:
                            continue
                        video_list.append((image_url, number))

        connection = get_database()
        cursor = connection.cursor()
        for video in video_list:
            cursor.execute("""INSERT INTO links (link, number)
                              SELECT * FROM (SELECT '%s', '%s')
                              WHERE NOT EXISTS (SELECT link FROM links WHERE link = '%s')
                              LIMIT 1""" % (video[0], video[1], video[0]))
        connection.commit()
        connection.close()
    print('Finish updating database')

def parse_number(image_url):
    try:
        number_section = image_url.split('/')[5]
        english_part = ''.join([char for char in number_section if not char.isdigit()])
        english_part = english_part.split('_')[1] if '_' in english_part else english_part
        digit_part = number_section[-3:]
        return english_part + digit_part
    except IndexError as error:
        print(error)
        return None

if __name__ == '__main__':
    initialize_database()
    port = int(os.environ.get('PORT', 5000))
    app.run(host = '0.0.0.0', port = port, threaded = True)
