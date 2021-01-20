#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from flask import Flask, request, render_template, jsonify, Blueprint, flash, g, redirect, session, url_for
from werkzeug.utils import secure_filename
from flask import send_from_directory
from project import *
import logging

import pandas as pd
from sqlalchemy import create_engine

# Support for gomix's 'front-end' and 'back-end' UI.
app = Flask(__name__, static_folder='public', template_folder='views')

# Set the app secret key from the secret environment variables.
app.secret = os.environ.get('SECRET')
UPLOAD_FOLDER = '/app/uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# A very simple Flask Hello World app for you to get started with...

def allowed_file(filename):
    return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        print(request.form)
        data = request.form.to_dict()
        sep = data['separator']
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            
            #Run project.py
            UploadToDB(filename,sep)
            
            # return render_template('success.html')
    return render_template('index.html')

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run()