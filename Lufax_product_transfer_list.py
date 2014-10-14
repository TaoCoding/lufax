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

def str_current_time():
    current_time=time.time()*1000
    return str(current_time)


#format the souce link, then get the html, tranlate to dict
def get_html(page_number):  
    pre_link='https://list.lufax.com/list/service/product/transfer-product-list/listing/'
    page_number=str(page_number)
    suffix_link='?minAmount=0&maxAmount=100000000&column=floatingRate&tradingMode=&order=asc&isDefault=true&_='
    source=pre_link+page_number+suffix_link+str_current_time()
    html=urllib.urlopen(source).read()   #the got data is a string,
    html=simplejson.loads(html)         # translate to dict
    return html



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
conn=sqlite3.connect('d:\Workspace_python\lufax.db') #if not exist, will create
cursor=conn.cursor()
#
cursor.execute("""create table if not exists product_transfer_list(
productId varchar(20),
link varchar(128),
productCode varchar(32),
publishedDate varchar(128),
doneDate varchar(128),
principal varchar(16)
)""")
i=1
for product_detail in product_sum:
    
    sql="insert into product_transfer_list(productId,productCode,link,publishedDate,doneDate,principal) values((?),(?),(?),(?),(?),(?))"
    productId_s=product_detail['productId']
    productCode_s=product_detail['code']
    link='https://list.lufax.com/list/productDetail?productId='+str(productId_s)
    publishedDate_s=product_detail['publishAtCompleteTime']
    principal_s=product_detail['principal']
    doneDate_s=product_detail['updateAt']
    cursor.execute(sql,(productId_s,productCode_s,link,publishedDate_s,doneDate_s,principal_s))
    print 'Inserting %d row' %i
    i=i+1
     
cursor.close()
conn.commit()
conn.close()


    




