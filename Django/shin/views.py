from django.shortcuts import render
from shin.models import ciall,ciaccident,cicahcause,cifloorinfo,cimanager,cioperation,ciplaceinfo
import pandas as pd
from django.views.decorators.csrf import csrf_exempt
import json
from django.http import JsonResponse
from numpyencoder import NumpyEncoder

CA = ciall.objects.all().values()
cb = pd.DataFrame(CA)
CC = ciplaceinfo.objects.all().values()
cd = pd.DataFrame(CC)
CE = cifloorinfo.objects.all().values()
cf = pd.DataFrame(CE)
def chatfor(ss):
    if ss == '경북':
        sia = cb[cb['ciaddr'].str.startswith('경상북도',na=False) + cb['ciaddr'].str.startswith('경북',na=False)]
    elif ss == '경남':
        sia = cb[cb['ciaddr'].str.startswith('경상남도',na=False) + cb['ciaddr'].str.startswith('경남',na=False)]
    elif ss == '전북':
        sia = cb[cb['ciaddr'].str.startswith('전라북도',na=False) + cb['ciaddr'].str.startswith('전북',na=False)]
    elif ss == '전남':
        sia = cb[cb['ciaddr'].str.startswith('전라남도',na=False) + cb['ciaddr'].str.startswith('전남',na=False)]
    elif ss == '충북':
        sia = cb[cb['ciaddr'].str.startswith('충청북도',na=False) + cb['ciaddr'].str.startswith('충북',na=False)]
    elif ss == '경북':
        sia = cb[cb['ciaddr'].str.startswith('충청남도',na=False) + cb['ciaddr'].str.startswith('충남',na=False)]
    else:
        sia = cb[cb['ciaddr'].str.startswith(ss,na=False)]
    #ss로 해당 지역을 받고 그 지역에 해당하는 데이터 프레임을 받아옵니다
    b = len(sia[sia['cimingong'] == '민간'])
    #b는 지역의 민간 시설의 갯수입니다.
    c = len(sia[sia['cimingong'] == '공공'])
    #c는 지역의 공공 시설의 갯수입니다.
    md = len(sia[sia['ciduty'] == '의무'])
    #md는 지역의 의무시설의 갯수를 나타냅니다
    nd = len(sia[sia['ciduty'] == '비의무'])
    #nd는 지역의 비의무시설의 갯수를 나타냅니다
    ou = len(sia[sia['ciinout'] == '실외'])
    #ou는 지역의 실외시설의 갯수를 나타냅니다
    io = len(sia[sia['ciinout'] == '실내'])
    #io는 지역의 실내시설의 갯수를 나타냅니다

    cp = cb['ciaddr'].str.startswith(ss,na='false').groupby(cb['ciplacecode']).count().sort_values(ascending=False).head(5)
    #cp는 지역을 놀이시설별로 묶고 그중에서 가장 많은 놀이시설 5개를 보여줍니다
    h1 = cp.index[0]
    #가장 많은 놀이시설의 이름입니다
    h2 = cp.index[1]
    h3 = cp.index[2]
    h4 = cp.index[3]
    h5 = cp.index[4]
    c1 = cd[cd['ciplacecode'] == h1]['plcodename'].iloc[0]
    #위에서 놀이시설은 코드로 되있어서 다른 테이블과 비교해서 보기 쉽게 한글명으로 가져왔습니다ex)'A01'에서 바꿈
    c2 = cd[cd['ciplacecode'] == h2]['plcodename'].iloc[0]
    c3 = cd[cd['ciplacecode'] == h3]['plcodename'].iloc[0]
    c4 = cd[cd['ciplacecode'] == h4]['plcodename'].iloc[0]
    c5 = cd[cd['ciplacecode'] == h5]['plcodename'].iloc[0]
    cv1 = cp[0]
    # 가장 많은 놀이시설의 갯수입니다
    cv2 = cp[1]
    cv3 = cp[2]
    cv4 = cp[3]
    cv5 = cp[4]

    flo = cb['ciaddr'].str.startswith(ss,na='false').groupby(cb['cifloorcode']).count()
    #flo는 지역의 바닥재별로 묶어서 보여줍니다
    f1 = flo.index[0]
    #가장 많이 쓴 바닥재의 이름입니다.
    f2 = flo.index[1]
    f3 = flo.index[2]
    f4 = flo.index[3]
    flo1 = cf[cf['cifloorcode'] == f1]['flocodename'].iloc[0]
    flo2 = cf[cf['cifloorcode'] == f2]['flocodename'].iloc[0]
    flo3 = cf[cf['cifloorcode'] == f3]['flocodename'].iloc[0]
    flo4 = cf[cf['cifloorcode'] == f4]['flocodename'].iloc[0]
    flov1 = flo[0]
    #가장 많이 쓴 바닥재의 갯수입니다.
    flov2 = flo[1]
    flov3 = flo[2]
    flov4 = flo[3]
    bPer = round(b / (b + c) * 100)
    #민간시설의 갯수를 퍼센트로 나타냈습니다
    cPer = round(c / (b + c) * 100)
    #공공시설의 갯수를 퍼센트로 나타냈습니다
    return bPer,cPer,md,nd,ou,io,c1,c2,c3,c4,c5,cv1,cv2,cv3,cv4,cv5,flo1,flo2,flo3,flo4,flov1,flov2,flov3,flov4



def index(request):
    list = [10410,21572,4160,3103,2900,3647,2187,330,2535,3124,3317,4626,5258,3423,3843,1986,1110]
    return render(request, 'index.html',{'seplay': list[0],'kkplay':list[1],'inplay':list[2],'kwplay':list[3],'cbplay':list[4],'cnplay':list[5],'djplay':list[6],'sjplay':list[7],'kjplay':list[8],'jbplay':list[9],'jnplay':list[10],'kbplay':list[11],'knplay':list[12],'dgplay':list[13],'buplay': list[14],'ulplay':list[15],'jeplay':list[16]})

#post형태로 받을때는 form위에 csrf를 써도 되지만 views에 선언한후 아래처럼써도 됩니다
@csrf_exempt
@csrf_exempt
def dashboard(request):
    list = [
        '서울', '강원', '경기', '인천', '세종', '대전', '대구', '부산', '울산', '광주', '제주', '경북', '경남', '충북', '충남', '전북', '전남']
    si = request.POST['gu']
    bPer,cPer,md,nd,ou,io,c1,c2,c3,c4,c5,cv1,cv2,cv3,cv4,cv5,flo1,flo2,flo3,flo4,flov1,flov2,flov3,flov4 = chatfor(si)
    print(chatfor(si))
    #각각의 변수에 함수로 리턴받은 값을 받아줍니다
    return render(request, 'dashboard.html',
                  {'guList': list, 'si': si,'bPer' : bPer,'cPer' : cPer, 'md' : md,'nd' : nd, 'ou' : ou,'io' : io,'c1': c1,'c2': c2,'c3': c3,'c4':c4,'c5': c5,'cv1':cv1,'cv2' : cv2,'cv3' :cv3,'cv4':cv4,'cv5':cv5,'flo1': flo1,'flo2':flo2,'flo3': flo3,'flo4':flo4,'flov1':flov1,'flov2':flov2,'flov3':flov3,'flov4':flov4})

@csrf_exempt
def sival(request):
    siva = request.POST['su']
    bPer,cPer,md,nd,ou,io,c1,c2,c3,c4,c5,cv1,cv2,cv3,cv4,cv5,flo1,flo2,flo3,flo4,flov1,flov2,flov3,flov4 = chatfor(siva)
    #각각의 변수에 함수로 리턴받은 값을 받아줍니다
    result = {'guList': list, 'siva': siva, 'bPer' : bPer,'cPer' : cPer, 'md' : md,'nd' : nd, 'ou' : ou,'io' : io,'c1': c1,'c2': c2,'c3': c3,'c4':c4,'c5': c5,'cv1':cv1,'cv2' : cv2,'cv3' :cv3,'cv4':cv4,'cv5':cv5,'flo1': flo1,'flo2':flo2,'flo3': flo3,'flo4':flo4,'flov1':flov1,'flov2':flov2,'flov3':flov3,'flov4':flov4}
    #이번에는 json형태로 주기 위해 딕셔너리 형태로 만들었습니다
    result = (json.dumps(result, cls=NumpyEncoder, indent=4, default=str, ensure_ascii=False))
    #반환시 json 형태로 주기위해 코딩을 해주었습니다
    return JsonResponse(result, safe=False)



