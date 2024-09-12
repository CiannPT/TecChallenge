from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
import typing
import time
import os
import zipfile
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def Main():
    driver = webdriver.Edge()
    driver.implicitly_wait(10)
    driver.maximize_window()
    driver.get(R"https://www.ibge.gov.br/estatisticas/downloads-estatisticas.html")
    assert "IBGE" in driver.title
    WebDriverWait(driver, 30).until( EC.element_to_be_clickable((By.XPATH, "//button[@id='cookie-btn']"))).click()
    WebDriverWait(driver, 30).until( EC.element_to_be_clickable((By.XPATH, "//li[@id='Censos']/a"))).click()
    WebDriverWait(driver, 30).until( EC.element_to_be_clickable((By.XPATH, "//li[@id='Censos/Censo_Demografico_1991']/a"))).click()
    WebDriverWait(driver, 30).until( EC.element_to_be_clickable((By.XPATH, "//li[@id='Censos/Censo_Demografico_1991/Indice_de_Gini']/a"))).click()
    elems = driver.find_elements(By.XPATH, "//li[@id='Censos/Censo_Demografico_1991/Indice_de_Gini']/ul/li")
    data = pd.DataFrame(columns=['Area','Value'])
    for el in elems:
        if el.accessible_name.endswith(".zip"):
            file = get_data(el)
            print(f"{el.accessible_name} - {file}")
            data = pd.concat([data,read_excel(file)])
    data.drop_duplicates()
    database_upload(data)

    driver.close()
    return


def get_data(elem: WebElement) -> str:
    downloads_path = os.path.expandvars(R"%USERPROFILE%\Downloads")
    zip_name = download_zip(downloads_path, 30, elem.click)
    zip_path = os.path.join(downloads_path,zip_name)
    file_path = unzip_data(zip_path)
    return file_path


def download_zip(path_to_downloads: str, timeout: int, download_action: callable) -> str:
    seconds = 0
    original_files = os.listdir(path_to_downloads)
    download_action()
    while seconds < timeout:
        time.sleep(1)
        for fname in os.listdir(path_to_downloads):
            if fname not in original_files and not fname.endswith('.crdownload'):
                return fname
        seconds += 1
    return ""

def unzip_data(Zip_path: str, output_folder_path: str) -> str:
    original_files = os.listdir(output_folder_path)
    with zipfile.ZipFile(Zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_folder_path)
    current_files = os.listdir(output_folder_path)
    for file in current_files:
        if file not in original_files:
            return file
    return ""

def read_excel(file_path: str)-> pd.DataFrame:
    dfs = pd.read_excel(file_path, sheet_name="Plan1")
    dfs.columns = ['Area','Value']
    return dfs

def database_upload(df: pd.DataFrame):
    try:
        engine = create_engine('postgresql://postgres:olaolao@localhost:5432/postgres')
        df.to_sql('gini', engine, if_exists='replace')
    except ( Exception) as error:
        print(error)
    pass

if __name__ == "__main__":
    Main()