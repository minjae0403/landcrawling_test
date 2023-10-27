import selenium, time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

sit_url = "https://http://rtdown.molit.go.kr/"

options = Options()

# #아래는 크롤링 완료시 속도 높혀주기 위한 크롬 드라이버 창안띄우고 돌리는 옵션
# options.add_argument('--no-sandbox')        
# options.add_argument('--headless')       
# options.add_argument('--disable-dev-shm-usage')
# options.add_argument("--disable-setuid-sandbox") 
# options.add_argument('--disable-gpu')
# options.add_argument("--window-size=1920,1080")
# options.add_argument('--ignore-certificate-errors')
# options.add_argument('--allow-running-insecure-content')

driver = webdriver.Chrome(options=options)
driver.get(sit_url)
time.sleep(1)

# 위에서 크롬 드라이버문제가 뜬다. 아래는 드라이버 직접설치로 해결함 방법 찾기

# # server
# service = ChromeService(executable_path = "/usr/bin/chromedriver")
# driver = webdriver.Chrome(service=service, options=options)
# driver.get(Main_Page_Url)
# time.sleep(1)

# region = driver.find_element(By.ID, "expand")
# print(region)
# region.click()