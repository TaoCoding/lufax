#coding=utf-8
# The program is for lufax. 稳赢安E###############
#单个项目的检测，获得余额和奖励，抢余额用              #
#                                              #         
#                                              #
################################################

import urllib           
import re
import time
import webbrowser
from BeautifulSoup import BeautifulSoup

def getHtml(url):
    page = urllib.urlopen(url);
    html = page.read();
    html=BeautifulSoup(''.join(html));
    return html;

def OnlyCharNum(s):            #using '' to replace the ,.
    format=',.'
    for c in s:
        if c in format:
            s=s.replace(c,'');
    return int(s);    #at last,translate string to int.

waitingtime=60        #Default duration time 
while True:   
    source="https://list.lufax.com/list/productDetail?productId=268494"
    
    time.sleep(waitingtime)    #Waiting 
    html = getHtml(source)

    re_amount=html.findAll('p',"remaining-amount")[0].strong.string
    reward=html.findAll('div',"invest-reward-body")[0].contents[1].contents[3].string

    re_amount_gbk=re_amount.encode('gbk');
    re_amount_number=re.findall(r'\d+,\d+\.',re_amount_gbk)[0];
    re_amount_number=OnlyCharNum(re_amount_number)      #get the remaining number

    reward_gbk=reward.encode('gbk');
    reward_number=re.findall(r'\d+0',reward_gbk)[0];
    reward_number=OnlyCharNum(reward_number);           #get the reward number

    temp=re_amount_number/reward_number

    ISOTIMEFORMAT='%Y-%m-%d %X'         #time format
    current_time=time.strftime(ISOTIMEFORMAT,time.localtime())   #current time  

    if temp<100:                                       #exec section
        print "It'm time to go.", current_time;
        print source;
        webbrowser.open(source)
        break;
    elif temp<301:
        waitingtime=1;
        print "Please be ready, will start.Now the index is",temp;
        print current_time;
    elif temp<1000:
        waitingtime=10;
        print "Waiting.... Now the index(the lower, the better) is ",temp;
        print current_time;
    else:
        print "Waiting.... Now the index(the lower, the better) is ",temp;
        print current_time;
        

