from django.shortcuts import render
from project.models import ciall,ciaccident,cicahcause,cifloorinfo,cimanager,cioperation,ciplaceinfo
import pandas as pd
from django.views.decorators.csrf import csrf_exempt
import json
from django.http import JsonResponse
from numpyencoder import NumpyEncoder

"""

수정 최종본(중모) : 변수명 / 함수명 수정
수정 내용 주석 확인

"""

# CA -> ci_all
# cb -> ci_all_df
# CC -> ci_place
# cd -> ci_place_df
# CE -> ci_floor
# cf -> ci_floor_df
ci_all = ciall.objects.all().values()
ci_all_df = pd.DataFrame(ci_all)
ci_place = ciplaceinfo.objects.all().values()
ci_place_df = pd.DataFrame(ci_place)
ci_floor = cifloorinfo.objects.all().values()
ci_floor_df = pd.DataFrame(ci_floor)
# ss -> loc
# sia -> addr
def chatfor(loc):
    """ loc으로 해당 지역을 받고 그 지역에 해당하는 데이터 프레임을 받아옵니다 """
    if loc == '경북':
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith('경상북도',na=False) + ci_all_df['ciaddr'].str.startswith('경북',na=False)]
    elif loc == '경남':
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith('경상남도',na=False) + ci_all_df['ciaddr'].str.startswith('경남',na=False)]
    elif loc == '전북':
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith('전라북도',na=False) + ci_all_df['ciaddr'].str.startswith('전북',na=False)]
    elif loc == '전남':
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith('전라남도',na=False) + ci_all_df['ciaddr'].str.startswith('전남',na=False)]
    elif loc == '충북':
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith('충청북도',na=False) + ci_all_df['ciaddr'].str.startswith('충북',na=False)]
    elif loc == '경북':
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith('충청남도',na=False) + ci_all_df['ciaddr'].str.startswith('충남',na=False)]
    else:
        addr = ci_all_df[ci_all_df['ciaddr'].str.startswith(loc,na=False)]

    # 지역의 민간 시설의 갯수
    # b -> min_count
    min_count = len(addr[addr['cimingong'] == '민간'])

    # 지역의 공공 시설의 갯수
    # c -> gong_count
    gong_count = len(addr[addr['cimingong'] == '공공'])

    # 민간/공공 시설의 갯수를 비율로
    min_ratio = round(min_count / (min_count + gong_count) * 100)
    gong_ratio = round(gong_count / (min_count + gong_count) * 100)

    # 지역의 의무시설의 갯수
    # md -> duty_count
    duty_count = len(addr[addr['ciduty'] == '의무'])

    # 지역의 비의무시설의 갯수
    # nd -> nonduty_count
    nonduty_count = len(addr[addr['ciduty'] == '비의무'])

    # 지역의 실외시설의 갯수
    # ou -> out_count
    out_count = len(addr[addr['ciinout'] == '실외'])

    # 지역의 실내시설의 갯수
    # io -> in_count
    in_count = len(addr[addr['ciinout'] == '실내'])

    # 지역을 놀이시설별로 묶고 그중에서 가장 많은 놀이시설 5개 보여줌
    # cp -> count_top5
    count_top5 = ci_all_df['ciaddr'].str.startswith(loc,na='false').groupby(ci_all_df['ciplacecode']).count().sort_values(ascending=False).head(5)

    # 가장 많은 놀이시설
    # h -> top1 ~ top5
    top1 = count_top5.index[0]
    top2 = count_top5.index[1]
    top3 = count_top5.index[2]
    top4 = count_top5.index[3]
    top5 = count_top5.index[4]

    # 놀이시설 이름으로 가져오기
    # c -> place1 ~ place5
    place1 = ci_place_df[ci_place_df['ciplacecode'] == top1]['plcodename'].iloc[0]
    place2 = ci_place_df[ci_place_df['ciplacecode'] == top2]['plcodename'].iloc[0]
    place3 = ci_place_df[ci_place_df['ciplacecode'] == top3]['plcodename'].iloc[0]
    place4 = ci_place_df[ci_place_df['ciplacecode'] == top4]['plcodename'].iloc[0]
    place5 = ci_place_df[ci_place_df['ciplacecode'] == top5]['plcodename'].iloc[0]
    
    # 가장 많은 놀이시설의 갯수
    # cv -> place_top
    place_top1 = count_top5[0]
    place_top2 = count_top5[1]
    place_top3 = count_top5[2]
    place_top4 = count_top5[3]
    place_top5 = count_top5[4]

    # 지역의 바닥재별로 묶어서
    # flo -> floor
    floor = ci_all_df['ciaddr'].str.startswith(loc,na='false').groupby(ci_all_df['cifloorcode']).count()
    
    # 가장 많이 쓴 바닥재의 이름
    # f -> flo_top
    flo_top1 = floor.index[0]
    flo_top2 = floor.index[1]
    flo_top3 = floor.index[2]
    flo_top4 = floor.index[3]

    flo1 = ci_floor_df[ci_floor_df['cifloorcode'] == flo_top1]['flocodename'].iloc[0]
    flo2 = ci_floor_df[ci_floor_df['cifloorcode'] == flo_top2]['flocodename'].iloc[0]
    flo3 = ci_floor_df[ci_floor_df['cifloorcode'] == flo_top3]['flocodename'].iloc[0]
    flo4 = ci_floor_df[ci_floor_df['cifloorcode'] == flo_top4]['flocodename'].iloc[0]
    
    # 가장 많이 쓴 바닥재의 갯수
    # flov -> flo_count
    flo_count1 = floor[0]
    flo_count2 = floor[1]
    flo_count3 = floor[2]
    flo_count4 = floor[3]

    return min_ratio, gong_ratio, duty_count, nonduty_count, out_count, in_count,\
        place1, place2, place3, place4, place5, place_top1, place_top2, place_top3, place_top4, place_top5,\
        flo1, flo2, flo3, flo4, flo_count1, flo_count2, flo_count3, flo_count4

def index(request):
    list = [10410,21572,4160,3103,2900,3647,2187,330,2535,3124,3317,4626,5258,3423,3843,1986,1110]
    return render(request, 'index.html',
                {'seoul': list[0],'gyeonggi':list[1],'incheon':list[2],'gangwon':list[3],
                'chungbook':list[4],'chungnam':list[5],'daejeon':list[6],'sejong':list[7],
                'gwangju':list[8],'jeonbuk':list[9],'jeonnam':list[10],'gyeongbuk':list[11],
                'gyeongnam':list[12],'daegu':list[13],'busan': list[14],'ulsan':list[15],'jeju':list[16]})

@csrf_exempt
@csrf_exempt
def dashboard(request):
    list = ['서울', '강원', '경기', '인천', '세종', '대전', '대구', '부산', '울산', '광주', '제주', '경북', '경남', '충북', '충남', '전북', '전남']
    si = request.POST['gu']
    
    min_ratio, gong_ratio, duty_count, nonduty_count, out_count, in_count,\
    place1, place2, place3, place4, place5, place_top1, place_top2, place_top3, place_top4, place_top5,\
    flo1, flo2, flo3, flo4, flo_count1, flo_count2, flo_count3, flo_count4 = chatfor(si)
    print(chatfor(si))

    return render(request, 'dashboard.html',
                  {'guList': list, 'si': si, 'min_ratio' : min_ratio,'gong_ratio' : gong_ratio,
                  'duty_count' : duty_count ,'nonduty_count' : nonduty_count, 'out_count' : out_count,'in_count' : in_count,
                  'place1': place1,'place2': place2,'place3': place3,'place4':place4,'place5': place5,
                  'place_top1':place_top1,'place_top2' : place_top2,'place_top3' :place_top3,'place_top4':place_top4,'place_top5':place_top5,
                  'flo1': flo1,'flo2':flo2,'flo3': flo3,'flo4':flo4,
                  'flo_count1':flo_count1,'flo_count2':flo_count2,'flo_count3':flo_count3,'flo_count4':flo_count4})

@csrf_exempt
def child(request):
    select_si = request.POST['select_si']
    
    min_ratio, gong_ratio, duty_count, nonduty_count, out_count, in_count,\
    place1, place2, place3, place4, place5, place_top1, place_top2, place_top3, place_top4, place_top5,\
    flo1, flo2, flo3, flo4, flo_count1, flo_count2, flo_count3, flo_count4 = chatfor(select_si)

    result = {'guList': list, 'select_si': select_si, 'min_ratio' : min_ratio,'gong_ratio' : gong_ratio, 'duty_count' : duty_count,'nonduty_count' : nonduty_count, 'out_count' : out_count,'in_count' : in_count,
            'place1': place1,'place2': place2,'place3': place3,'place4':place4,'place5': place5,
            'place_top1':place_top1,'place_top2' : place_top2,'place_top3' :place_top3,'place_top4':place_top4,'place_top5':place_top5,
            'flo1': flo1,'flo2':flo2,'flo3': flo3,'flo4':flo4,
            'flo_count1':flo_count1,'flo_count2':flo_count2,'flo_count3':flo_count3,'flo_count4':flo_count4}

    # json형태로 주기 위해 딕셔너리 형태로
    result = (json.dumps(result, cls=NumpyEncoder, indent=4, default=str, ensure_ascii=False))

    # 반환시 json 형태로
    return JsonResponse(result, safe=False)



