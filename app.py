from flask import Flask, g, request, abort

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageSendMessage

from bs4 import BeautifulSoup
from threading import Thread
import os
import random
import requests
import sqlite3

DATABASE = 'database.db'

app = Flask(__name__)

line_bot_api = LineBotApi(os.environ['CHANNEL_ACCESS_TOKEN'])
handler = WebhookHandler(os.environ['CHANNEL_SECRET'])

def initialize_database():
    with app.app_context():
        connection = get_database()
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT NOT NULL)')
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

@app.route('/callback', methods = ['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text = True)
    app.logger.info('Request body: ' + body)

    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    # return status
    return 'OK'

@handler.add(MessageEvent, message = TextMessage)
def handle_message(event):
    if event.message.text == '抽':
        image_url = get_random_image_url()
        image_url = 'https' + image_url[4:]
        line_bot_api.reply_message(event.reply_token, ImageSendMessage(original_content_url = image_url, preview_image_url = image_url))

def get_random_image_url():
    connection = get_database()
    cursor = connection.cursor()
    random_number = random.randint(1, 1800)
    link = cursor.execute('SELECT link FROM links WHERE id = %s' % random_number)
    link = link.fetchone()[0]
    connection.close()
    return link

@app.route('/update')
def update():
    thread = Thread(target = async_update_links)
    thread.start()
    return 'Start updating database'

def async_update_links():
    with app.app_context():
        home_url = 'http://www.dmm.co.jp/digital/videoa/-/list/=/sort=ranking'
        html_parser = 'html.parser'
        image_urls = []

        for page in range(1, 16):
            app.logger.info('Crawling page %s' % page)
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
                        image_urls.append(image_url)

        connection = get_database()
        cursor = connection.cursor()
        for image_url in image_urls:
            cursor.execute("INSERT INTO links (link) VALUES ('%s')" % image_url)
        connection.commit()
        connection.close()

if __name__ == '__main__':
    initialize_database()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port = port, threaded = True)
