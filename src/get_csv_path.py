import os
def get_csv_path(filename):
    """Возвращает абсолютный путь к CSV файлу в папке data"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    return os.path.join(project_root, 'data', filename)