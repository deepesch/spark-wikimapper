from flask import render_template
from flask import request
from wtforms import Form
from wtforms import TextField, PasswordField, StringField
import sys
from flask import Flask
import csv

app = Flask(__name__)

class UserPasswordForm(Form):
    email = TextField('Email')
    password = PasswordField('Password')

@app.route("/")
def hello():
    form = UserPasswordForm()
    try:
        return render_template('login.html', form=form)
    except Exception as e:
        print str(e)

@app.route("/login", methods = ["POST"])
def login():
    error = None
    if request.method == 'POST':
       print 'post method'
       try:
            print request.form
       except Exception as e:
            print str(e)
    username = request.form["email"]
    print username
    with open('users.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader: 
            if username == row[0]:
                wikilist = data(row[1])
                return render_template('articles.html',wikilist = wikilist)

# @app.route("/get_data", methods = ['GET'])
def data(user_rank):
    print('here')
    with open('Workbook.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        next(spamreader)
        wikilist = []
        for row in spamreader:
            wikilist.append(row[1])
    user_rank = int(user_rank) # int(request.args.get('user')) 
    print type(user_rank), user_rank
    return wikilist[user_rank * 5 : (user_rank + 1) * 5]
if __name__ == "__main__":
    app.run()
