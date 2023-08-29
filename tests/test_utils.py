from zipfile import ZipFile

from config.paths import TEST_DATA_DIR


def get_html_text_from_a_test_data_zip_file(file_name):
    zf = ZipFile(f"{TEST_DATA_DIR}/{file_name}.html.zip")
    file_name = zf.open(f"{file_name}.html")
    html_page = file_name.read().decode("utf-8")
    return html_page
