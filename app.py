import os
import uuid
from flask import Flask, flash, request, redirect, render_template, send_file, url_for
from werkzeug.utils import secure_filename

from .parser import get_tab_names, process_file

UPLOAD_FOLDER = 'files'
RESULTS_FOLDER = 'results'
ALLOWED_EXTENSIONS = {'xlsx'}

app = Flask(__name__)
app.secret_key = '8a2623ee7d5d5ac279a78b40e69c1c3d37bb7ea5d5ab03d2537f3e27546ab077'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename, file_extension = os.path.splitext(secure_filename(file.filename))
            filename = f'{filename}_{uuid.uuid4().hex}{file_extension}'
            if not os.path.exists(app.config['UPLOAD_FOLDER']):
                os.makedirs(app.config['UPLOAD_FOLDER'])
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            return redirect(url_for('tabs', filename=filename))
    return render_template('upload.html')


@app.route('/tabs/<filename>', methods=['GET', 'POST'])
def tabs(filename):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if request.method == 'POST':
        tabs = request.form.getlist('tab')
        zip_file = process_file(filepath, app.config['UPLOAD_FOLDER'], tabs)
        return send_file(zip_file, as_attachment=True)
    tabs = list(get_tab_names(filepath))
    return render_template('tabs.html', tabs=tabs)


if __name__ == '__main__':
    app.run()
