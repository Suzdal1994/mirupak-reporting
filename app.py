"""
Flask-приложение — Инструмент ежемесячной отчётности «Мир Упаковки»
Хранилище задач: /tmp/jobs/<job_id>.json (работает с несколькими воркерами gunicorn)
"""

import os
import json
import threading
import uuid
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

from analyzer import run_analysis
from gamma_client import GammaClient

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.secret_key = os.environ.get('SECRET_KEY', 'mirupak-secret-2026')

JOBS_DIR = '/tmp/jobs'

# Создаём рабочие папки при старте
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(JOBS_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def job_path(job_id):
    return os.path.join(JOBS_DIR, f"{job_id}.json")


def read_job(job_id):
    """Читает задачу из файла. Возвращает None если не найдена."""
    path = job_path(job_id)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_job(job_id, data):
    """Записывает задачу в файл атомарно."""
    path = job_path(job_id)
    tmp_path = path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def update_job(job_id, **kwargs):
    """Обновляет поля задачи."""
    job = read_job(job_id)
    if job is not None:
        job.update(kwargs)
        write_job(job_id, job)


def process_job(job_id: str, filepath: str, gamma_api_key: str, theme_id: str, folder_id: str, gdrive_url: str):
    """Фоновый поток: анализ + создание презентаций в Gamma."""
    try:
        # Шаг 1: Анализ данных
        update_job(job_id, status='analyzing', progress=5, message='Загружаем и анализируем данные...')

        result = run_analysis(filepath)
        branches = result['branches']
        files = result['files']
        fact_period = result['fact_period']
        period_name = result['period_name']

        update_job(job_id,
                   progress=20,
                   message=f'Анализ завершён. Найдено {len(branches)} филиалов. Создаём презентации...',
                   branches=branches,
                   fact_period=fact_period,
                   period_name=period_name,
                   markdowns=files)

        # Шаг 2: Создание презентаций в Gamma
        gamma_results = {}
        gamma_errors = {}

        if gamma_api_key:
            client = GammaClient(gamma_api_key)

            total_files = len(files)
            done = 0

            for filename, markdown in files.items():
                try:
                    update_job(job_id,
                               status='creating_presentations',
                               message=f'Создаём в Gamma: {filename} ({done+1}/{total_files})',
                               progress=20 + int(70 * done / total_files))

                    is_summary = 'СВОДНАЯ' in filename
                    branch_name = filename.replace(f'Аналитика_продаж_{fact_period}_', '').replace('.md', '')
                    title = f"Сводная аналитика {period_name}" if is_summary else f"Аналитика {period_name} — {branch_name}"

                    kw = {}
                    if theme_id:
                        kw['theme_id'] = theme_id
                    if folder_id:
                        kw['folder_id'] = folder_id

                    gamma_result = client.create_and_wait(
                        markdown_text=markdown,
                        title=title,
                        **kw
                    )
                    gamma_results[filename] = {
                        'url': gamma_result['url'],
                        'title': title,
                        'is_summary': is_summary,
                        'branch': branch_name
                    }

                except Exception as e:
                    gamma_errors[filename] = str(e)

                done += 1

        # Шаг 3: Завершение
        update_job(job_id,
                   status='completed',
                   progress=100,
                   message='Готово! Все презентации созданы.',
                   gamma_results=gamma_results,
                   gamma_errors=gamma_errors)

    except Exception as e:
        update_job(job_id,
                   status='error',
                   progress=0,
                   message=f'Ошибка: {str(e)}',
                   error=str(e))

    finally:
        try:
            os.remove(filepath)
        except:
            pass


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/start', methods=['POST'])
def start_job():
    """Запускает задачу анализа."""

    if 'file' not in request.files:
        return jsonify({'error': 'Файл не загружен'}), 400

    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({'error': 'Файл не выбран'}), 400

    if not allowed_file(file.filename):
        return jsonify({'error': 'Поддерживаются только файлы .xlsx и .xls'}), 400

    gamma_api_key = request.form.get('gamma_api_key', '').strip()
    theme_id = request.form.get('theme_id', '').strip()
    folder_id = request.form.get('folder_id', '').strip()
    gdrive_url = request.form.get('gdrive_url', '').strip()

    job_id = str(uuid.uuid4())
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}_{filename}")
    file.save(filepath)

    # Создаём задачу в файловой системе
    write_job(job_id, {
        'status': 'starting',
        'progress': 0,
        'message': 'Запускаем анализ...',
        'branches': [],
        'markdowns': {},
        'gamma_results': {},
        'gamma_errors': {},
        'fact_period': '',
        'period_name': '',
        'has_gamma': bool(gamma_api_key)
    })

    thread = threading.Thread(
        target=process_job,
        args=(job_id, filepath, gamma_api_key, theme_id, folder_id, gdrive_url),
        daemon=True
    )
    thread.start()

    return jsonify({'job_id': job_id})


@app.route('/api/status/<job_id>')
def job_status(job_id):
    job = read_job(job_id)

    if not job:
        return jsonify({'error': 'Задача не найдена'}), 404

    # Не возвращаем markdowns в статусе (слишком большие) — только счётчик
    markdowns = job.get('markdowns', {})
    response = {k: v for k, v in job.items() if k != 'markdowns'}
    response['markdown_count'] = len(markdowns)
    response['markdown_files'] = list(markdowns.keys())
    return jsonify(response)


@app.route('/api/download/<job_id>/<filename>')
def download_markdown(job_id, filename):
    job = read_job(job_id)

    if not job or 'markdowns' not in job:
        return jsonify({'error': 'Файл не найден'}), 404

    content = job['markdowns'].get(filename)
    if not content:
        return jsonify({'error': 'Файл не найден'}), 404

    tmp_path = f"/tmp/{filename}"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        f.write(content)

    return send_file(tmp_path, as_attachment=True, download_name=filename,
                     mimetype='text/markdown')


@app.route('/api/download-all/<job_id>')
def download_all(job_id):
    """Скачать все Markdown-файлы в ZIP."""
    import zipfile
    import io

    job = read_job(job_id)

    if not job or 'markdowns' not in job:
        return jsonify({'error': 'Задача не найдена'}), 404

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname, content in job['markdowns'].items():
            zf.writestr(fname, content.encode('utf-8'))

    zip_buffer.seek(0)
    period = job.get('fact_period', 'report')

    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name=f"Аналитика_продаж_{period}.zip",
        mimetype='application/zip'
    )


@app.route('/api/validate-gamma', methods=['POST'])
def validate_gamma():
    """Проверяет API-ключ Gamma."""
    data = request.get_json()
    api_key = data.get('api_key', '').strip()

    if not api_key:
        return jsonify({'valid': False, 'error': 'Ключ не указан'})

    try:
        client = GammaClient(api_key)
        themes = client.get_themes()
        folders = client.get_folders()
        return jsonify({
            'valid': True,
            'themes': themes[:20] if isinstance(themes, list) else [],
            'folders': folders[:20] if isinstance(folders, list) else []
        })
    except Exception as e:
        return jsonify({'valid': False, 'error': str(e)})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
