#!/usr/bin/env ptyhon
#coding=utf-8
############################################################
###       filename is lufax_product_transfer_list       ####
### get the list of transfer produt from lufax,         ####
### write in sqlite3 db                                 ####
###                                                     ####
############################################################

import re
import time
import urllib
import simplejson  # for translation of str and dict
import sqlite3    # for database
import os

def str_current_time():
        current_time=time.time()*1000
        return str(current_time)
    
def iso_current_time(): 
    ISOTIMEFORMAT='%Y-%m-%d %X'         #time format
    current_time=time.strftime(ISOTIMEFORMAT,time.localtime())   #current time
    return current_time
    
#format the souce link, then get the html, tranlate to dict
def get_html(page_number):  
    pre_link='https://list.lufax.com/list/service/product/transfer-product-list/listing/'
    page_number=str(page_number)
    suffix_link='?minAmount=0&maxAmount=100000000&column=currentPrice&tradingMode=00&order=asc&isDefault=false&_='
    #trading mode=00 只显示一口价，不显示竞拍, column=currentPrice,控制排序，否则得话会有重复
    source=pre_link+page_number+suffix_link+str_current_time()
    html=urllib.urlopen(source).read()   #the got data is a string,
    html=simplejson.loads(html)         # translate to dict
    return html


while True:
    print iso_current_time(),'start to capture'
    totalPage=get_html(1)['totalPage']  #get the total page
    product_sum=[]
    page_number=1
    while page_number<=totalPage:
        product_list=get_html(page_number)['data'];
        product_sum=product_sum+product_list;       #product_sum store all products info
        print "This is %d page"%page_number         #log
        print "Got %d products"%len(product_sum)    #log
        page_number=page_number+1;


    #store the data in sqlite3 db
    
    filepath=os.getcwd()+"\\result\\lufax.db"
    conn=sqlite3.connect(filepath) #if not exist, will create
    cursor=conn.cursor()
    #
    cursor.execute("""create table if not exists product_transfer_list(
    productId int primary key,
    link varchar(128),
    productCode int,
    publishedDate varchar(128),
    doneDate varchar(128),
    principal float,
    reduceDays float
    )""")
    i=1
    for product_detail in product_sum:
        
        
        productId_s=product_detail['productId']
        productCode_s=product_detail['code']
        link='https://list.lufax.com/list/productDetail?productId='+str(productId_s)
        publishedDate_s=product_detail['publishAtCompleteTime']
        principal_s=product_detail['principal']
        
        reducePrice_s=product_detail['reducePrice']
        interestRate_s=product_detail['interestRate']
        reduceDays_s=reducePrice_s*360/(principal_s*interestRate_s)
        reduceDays_s=round(reduceDays_s,2)
        
        if product_detail['productStatus']=='DONE':        #如果项目成交，添加成交时间
            doneDate_s=product_detail['updateAt'];
        else:
            doneDate_s=""

        cursor.execute('select productId from product_transfer_list where 1=1')
        productId_Exist=cursor.fetchall()
        productId_list=[]
        for product in productId_Exist:
            productId_list.append(product[0])
            
        if productId_s in productId_list:
            sql="update product_transfer_list set productCode=(?),link=(?),publishedDate=(?),doneDate=(?),principal=(?),reduceDays=(?) where productId=(?)"
            cursor.execute(sql,(productCode_s,link,publishedDate_s,doneDate_s,principal_s,reduceDays_s,productId_s,))
            print iso_current_time(),'productId exist, updating %d row'%i
        else:
            sql="insert into product_transfer_list(productId,productCode,link,publishedDate,doneDate,principal,reduceDays) values((?),(?),(?),(?),(?),(?),(?))"
            cursor.execute(sql,(productId_s,productCode_s,link,publishedDate_s,doneDate_s,principal_s,reduceDays_s))
            print iso_current_time(),'Inserting %d row' %i
        i=i+1
         
    cursor.close()
    conn.commit()
    conn.close()
    print iso_current_time(),'please wait 5 minutes'

    time.sleep(300)


    




