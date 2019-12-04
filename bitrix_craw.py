from bs4 import BeautifulSoup
import requests
import pymysql.cursors
import datetime
import threading

lock = threading.Lock()
dt = datetime.datetime.now()
db = pymysql.connect(host='127.0.0.1', port=3306, database='certme', user='root')
cur = db.cursor()
tests = []


def get_questions(link, parent_id):
    sql = "INSERT INTO questions (`name`, `parent`, `outerId`) VALUES (%s, %s, %s)"
    page_questions = requests.get('https://bx-cert.ru' + link)
    print('Ответ сервера по тесту', link, page_questions.status_code)
    soup = BeautifulSoup(page_questions.text, "html.parser")
    questions = soup.select('ol > li > a')
    for question in questions:
        question_name = question.text
        question_id = question.get('href').split('/')[-2]
        get_responses(question.get('href'))
        with lock:
            cur.execute(sql, (question_name, parent_id, question_id))
            db.commit()
    print('Вопросы из раздела', parent_id, 'записан в БД', datetime.datetime.now() - dt)


def get_responses(link):
    sql = "INSERT INTO responses (`text`, `question_id`) VALUES (%s, %s)"
    page_response = requests.get('https://bx-cert.ru' + link)
    print('Ответ сервера по вопросу', link, page_response.status_code)
    soup = BeautifulSoup(page_response.text, "html.parser")
    responses = soup.select('.answer-list > li')
    for response in responses:
        with lock:
            cur.execute(sql, (response.text.strip(), link.split('/')[-2]))
            db.commit()


url = 'https://bx-cert.ru/certification/bitrix/'
page = requests.get(url)
print('Ответ сервера на запрос страницы с разделами:', page.status_code, datetime.datetime.now() - dt)
soup = BeautifulSoup(page.text, "html.parser")
items = soup.findAll('li', class_='cert-list')
for item in items:
    item_name = item.a.text
    item_id = item.a.get('href').split('/')[-2]
    item_href = item.a.get('href')
    sql = "INSERT INTO tests (`name`, `outerId`, `link`) VALUES (%s, %s, %s)"
    with lock:
        cur.execute(sql, (item_name, item_id, item_href))
    threading.Thread(target=get_questions, args=(item_href, item_id)).start()
with lock:
    db.commit()
    print('Разделы записаны в БД')
