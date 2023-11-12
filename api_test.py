import requests, json, time
import pandas as pd
from datetime import datetime
import multiprocessing as mp

print(mp.cpu_count())

current_date = datetime.now().date()

class api_option:
    KEY = '574a764b7870616e36376257624e5a'    #String(필수)	인증키	OpenAPI 에서 발급된 인증키
    # TYPE : 'json'  #String(필수)	요청파일타입	xml : xml, xml파일 : xmlf, 엑셀파일 : xls, json파일 : json
    SERVICE = 'tbLnOpendataRentV' #String(필수)	서비스명	tbLnOpendataRentV
    START_INDEX = '1'   #INTEGER(필수)	요청시작위치	정수 입력 (페이징 시작번호 입니다 : 데이터 행 시작번호)
    END_INDEX = '5'  #INTEGER(필수)	요청종료위치	정수 입력 (페이징 끝번호 입니다 : 데이터 행 끝번호)
    ACC_YEAR = '2023'    #STRING(선택)	접수연도	YYYY
    # SGG_CD =	#STRING(선택)	자치구코드	5자리 정수
    # SGG_NM =	#STRING(선택)	자치구명	문자열
    # BJDONG_CD =	#STRING(선택)	법정동코드	5자리 정수
    # LAND_GBN =	#STRING(선택)	지번구분	1:대지,2:산,3:블럭
    # BOBN =	#STRING(선택)	본번	4자리 정수
    # BUBN =	#STRING(선택)	부번	4자리 정수
    # CNTRCT_DE =   #STRING(선택)	계약일	YYYYMMDD
    # BLDG_NM = #STRING(선택)	건물명	문자열
    # HOUSE_GBN_NM =	#STRING(선택) 아파트/단독다가구/연립다세대/오피스텔 택1

# 시작 시간
total_start_time = time.time()

# data 담을 Dataframe를 정의
df_data = pd.DataFrame()
data_list = []
try:
    df_before = pd.read_csv(f'서울_부동산_데이터.csv')
    length = len(df_before)
    df_data = pd.concat((df_data,df_before), ignore_index=True)

except:
    length = 0

# url 연결 test
url = f'http://openapi.seoul.go.kr:8088/{api_option.KEY}/json/{api_option.SERVICE}/{api_option.START_INDEX}/{api_option.END_INDEX}/'
response = requests.get(url)

if response.status_code == 200:

    # api data 갯수 확인
    json_data = json.loads(response.text)
    list_total_count = int(json_data['tbLnOpendataRentV']['list_total_count'])
    print(f'{current_date}기준 매물 건수 :{list_total_count}')

    # 반복횟수
    repetitions = (list_total_count//1000)+1
    # print(f'반복횟수 : {repetitions}')

    # 시작 위치
    start_repitions = (length//1000)

    # 반복으로 dataframe에 합치기
    for num in range(int(start_repitions),int(repetitions)):
        start_time = time.time()
        print(f'index 범위 : {num*1000+1} ~ {(num+1)*1000}')
        start_index = num*1000+1
        end_index = (num+1)*1000
        end_time = time.time()
        print(f"index 정의: {end_time - start_time:.5f} sec")
        
        start_time = time.time()
        url = f'http://openapi.seoul.go.kr:8088/{api_option.KEY}/json/{api_option.SERVICE}/{start_index}/{end_index}/'
        response = requests.get(url)
        json_data = json.loads(response.text)
        end_time = time.time()
        print(f"url 요청: {end_time - start_time:.5f} sec")
        # print(json_data)

        start_time = time.time()
        result_data_list = json_data['tbLnOpendataRentV']['row']
        # print(result_data_list)
        # 방법 1 : 한줄씩 반복문으로 df로 바꿔서 합친다. -> 반복문으로 시간이 오래 걸린다.
        # for row_data in result_data_list:
        #     print(row_data)
        #     print('========')
        #     df_data = pd.DataFrame(row_data, index = [0])
        #     print(df_data)
        #     print('========')
        # 방법 2 : list로 이루어진 dict을 바로 dataframe으로 변경
        new_df_data = pd.DataFrame(result_data_list)
        # print(new_df_data)
        df_data = pd.concat((df_data,new_df_data), ignore_index=True)
        df_data.to_csv(f'서울_부동산_데이터.csv', index=False)

        end_time = time.time()
        print(f"data 저장 : {end_time - start_time:.5f} sec")

    total_end_time = time.time()
    print(f"{total_end_time - total_start_time:.5f} sec")

else:
    print(response.status_code)


# 전반적 문제 = api가 1000개씩 요청이 되는 상황이라서 시간이 오래 걸린다.
# 1000 개당 약 31초 -> 5585번 반복 = 2793분 = 하루상 걸린다....
# 이거를 타개하기 위해서는 처음에 시간을 들여서 DW를 구축하고 매일 1회 업데이트 하는 방향으로 가야한다.
# 그리고 전날에 업데이트 된 정보 이후만 불러와서 진행하는 방식으로 코드를 짜야한다.
# 데이터와 별개로 api요청 전에 이전에 어디까지 요청을 했는지 파일을 불러오고, 끝나면 현재 몇번까지 불렀었는지 저장하게 하기