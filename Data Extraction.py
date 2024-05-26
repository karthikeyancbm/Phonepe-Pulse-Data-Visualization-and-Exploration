import os
import json
import pandas as pd
import mysql
import mysql.connector
import numpy as np
import plotly.express as px
import requests

connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
connection

mycursor = connection.cursor()

# Data Extraction and Table Creation

# Aggregate_transaction

def agg_trans_list():

    path1 = "C:/Users/ASUS/Documents/GUVI ZEN CLASSES/MAINT BOOT/PHONE PE PROJECT/pulse/data/aggregated/transaction/country/india/state/"
    agg_trans_list = os.listdir(path1)

    clm={'State':[], 'Year':[],'Quarter':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

    for state in agg_trans_list:
        updated_state_list = path1+state+"/"
        agg_year_list = os.listdir(updated_state_list)
        
        for year in agg_year_list:
            updated_year_list = updated_state_list+year+"/"
            agg_file_list = os.listdir(updated_year_list)
            
            for file in agg_file_list:
                cur_file = updated_year_list+file
                data = open(cur_file,"r")
                A = json.load(data)
                for z in A['data']['transactionData']:
                    Name=z['name']
                    count=z['paymentInstruments'][0]['count']
                    amount=z['paymentInstruments'][0]['amount']
                    clm['Transacion_type'].append(Name)
                    clm['Transacion_count'].append(count)
                    clm['Transacion_amount'].append(amount)
                    clm['State'].append(state)
                    clm['Year'].append(year)
                    clm['Quarter'].append(int(file.strip('.json')))

    Agg_Trans=pd.DataFrame(clm)
    Agg_Trans['Transacion_amount']=Agg_Trans['Transacion_amount'].astype(int) 

    Agg_Trans["State"] = Agg_Trans["State"].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
    Agg_Trans["State"] = Agg_Trans["State"].str.replace("-"," ")
    Agg_Trans["State"] = Agg_Trans["State"].str.title()
    Agg_Trans["State"] = Agg_Trans["State"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

    return Agg_Trans

agg_trans_list()

#aggregate_user

def aggregate_user():

    path2 = "C:/Users/ASUS/Documents/GUVI ZEN CLASSES/MAINT BOOT/PHONE PE PROJECT/pulse/data/aggregated/user/country/india/state/"
    agg_user_list = os.listdir(path2)

    clm2={'State':[], 'Year':[],'Quarter':[],'Brands':[], 'Count':[],'Percentage':[]}

    for state in agg_user_list:
        updated_state_list = path2+state+"/"
        agg_year_list = os.listdir(updated_state_list)

        for year in agg_year_list:
            updated_year_list = updated_state_list+year+"/"
            agg_file_list = os.listdir(updated_year_list)

            for file in agg_file_list:
                cur_file = updated_year_list+file
                data = open(cur_file,"r")
                B = json.load(data)

                try:            
                    for i in B["data"]["usersByDevice"]:
                        brand=i["brand"]
                        count=i["count"]
                        percentage=i["percentage"]
                        clm2['Brands'].append(brand)
                        clm2['Count'].append(count)
                        clm2['Percentage'].append(percentage)
                        clm2['State'].append(state)
                        clm2['Year'].append(year)
                        clm2['Quarter'].append(int(file.strip('.json')))
                except:
                    pass

    Aggregate_user=pd.DataFrame(clm2) 

    Aggregate_user["State"] = Aggregate_user["State"].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
    Aggregate_user["State"] = Aggregate_user["State"].str.replace("-"," ")
    Aggregate_user["State"] = Aggregate_user["State"].str.title()
    Aggregate_user["State"] = Aggregate_user["State"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

    return Aggregate_user

aggregate_user()
                
#map_transction

def map_transaction():

    path3= "C:/Users/ASUS/Documents/GUVI ZEN CLASSES/MAINT BOOT/PHONE PE PROJECT/pulse/data/map/transaction/hover/country/india/state/"
    map_trans_list = os.listdir(path3)

    clm3={'State':[], 'Year':[],'Quarter':[],'Districts':[], 'Transaction_count':[],'Transaction_amount':[]}

    for state in map_trans_list:
        updated_state_list = path3+state+"/"
        agg_year_list = os.listdir(updated_state_list)

        for year in agg_year_list:
            updated_year_list = updated_state_list+year+"/"
            agg_file_list = os.listdir(updated_year_list)

            for file in agg_file_list:
                cur_file = updated_year_list+file
                data = open(cur_file,"r")
                C = json.load(data)
                for i in C['data']['hoverDataList']:
                        district=i["name"]
                        count=i['metric'][0]['count']
                        amount=i['metric'][0]['amount']
                        clm3['Districts'].append(district)
                        clm3['Transaction_count'].append(count)
                        clm3['Transaction_amount'].append(amount)
                        clm3['State'].append(state)
                        clm3['Year'].append(year)
                        clm3['Quarter'].append(int(file.strip('.json')))
    Map_trans=pd.DataFrame(clm3)
    Map_trans['Transaction_amount']=Map_trans['Transaction_amount'].astype(int)

    Map_trans["State"] = Map_trans["State"].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
    Map_trans["State"] = Map_trans["State"].str.replace("-"," ")
    Map_trans["State"] = Map_trans["State"].str.title()
    Map_trans["State"] = Map_trans["State"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
    return Map_trans

map_transaction()       

#map_user

def map_user():

    path4= "C:/Users/ASUS/Documents/GUVI ZEN CLASSES/MAINT BOOT/PHONE PE PROJECT/pulse/data/map/user/hover/country/india/state/"
    map_user_list = os.listdir(path4)

    clm4={'State':[], 'Year':[],'Quarter':[],'Districts':[],'Registered_users':[],'AppOpens':[]}

    for state in map_user_list:
        updated_state_list = path4+state+"/"
        agg_year_list = os.listdir(updated_state_list)

        for year in agg_year_list:
            updated_year_list = updated_state_list+year+"/"
            agg_file_list = os.listdir(updated_year_list)

            for file in agg_file_list:
                cur_file = updated_year_list+file
                data = open(cur_file,"r")
                D = json.load(data)
                for i in D['data']['hoverData']:
                    district=i
                    clm4['Districts'].append(district)
                for metrics in D['data']['hoverData'].values():
                    reg_users=metrics['registeredUsers']
                    clm4['Registered_users'].append(reg_users)
                for metrics in D['data']['hoverData'].values():
                    app_opens=metrics['appOpens']
                    clm4['AppOpens'].append(app_opens)
                for i in D['data']['hoverData'].items():
                    clm4['State'].append(state)
                    clm4['Year'].append(year)
                    clm4['Quarter'].append(int(file.strip('.json')))
                    

    Map_user=pd.DataFrame(clm4)

    Map_user["State"] = Map_user["State"].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
    Map_user["State"] = Map_user["State"].str.replace("-"," ")
    Map_user["State"] = Map_user["State"].str.title()
    Map_user["State"] = Map_user["State"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
    
    return Map_user

map_user()   

#top_transaction

def top_transaction():

    path5= "C:/Users/ASUS/Documents/GUVI ZEN CLASSES/MAINT BOOT/PHONE PE PROJECT/pulse/data/top/transaction/country/india/state/"
    top_trans_list = os.listdir(path5)

    clm5={'State':[], 'Year':[],'Quarter':[],'Pincodes':[],'Transaction_count':[],'Transaction_amount':[]}

    for state in top_trans_list:
        updated_state_list = path5+state+"/"
        agg_year_list = os.listdir(updated_state_list)

        for year in agg_year_list:
            updated_year_list = updated_state_list+year+"/"
            agg_file_list = os.listdir(updated_year_list)

            for file in agg_file_list:
                cur_file = updated_year_list+file
                data = open(cur_file,"r")
                E = json.load(data)
                for i in E['data']['pincodes']:
                    pincodes=i['entityName']
                    count=i['metric']["count"]
                    amount=i['metric']['amount']
                    clm5['Pincodes'].append(pincodes)
                    clm5['Transaction_count'].append(count)
                    clm5['Transaction_amount'].append(amount)
                    clm5['State'].append(state)
                    clm5['Year'].append(year)
                    clm5['Quarter'].append(int(file.strip('.json')))

    top_trans=pd.DataFrame(clm5)
    top_trans['Transaction_amount']=top_trans['Transaction_amount'].astype(int)

    top_trans["State"] = top_trans["State"].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
    top_trans["State"] = top_trans["State"].str.replace("-"," ")
    top_trans["State"] = top_trans["State"].str.title()
    top_trans["State"] = top_trans["State"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

    return top_trans

top_transaction()

#top user

def top_user():

    path6= "C:/Users/ASUS/Documents/GUVI ZEN CLASSES/MAINT BOOT/PHONE PE PROJECT/pulse/data/top/user/country/india/state/"
    top_user_list = os.listdir(path6)

    clm6={'State':[], 'Year':[],'Quarter':[],'Pincodes':[],'Registered_users':[]}

    for state in top_user_list:
        updated_state_list = path6+state+"/"
        agg_year_list = os.listdir(updated_state_list)

        for year in agg_year_list:
            updated_year_list = updated_state_list+year+"/"
            agg_file_list = os.listdir(updated_year_list)

            for file in agg_file_list:
                cur_file = updated_year_list+file
                data = open(cur_file,"r")
                F = json.load(data)
                for i in F['data']['pincodes']:
                    pincodes=i['name']
                    users=i['registeredUsers']
                    clm6['Pincodes'].append(pincodes)
                    clm6['Registered_users'].append(users)
                    clm6['State'].append(state)
                    clm6['Year'].append(year)
                    clm6['Quarter'].append(int(file.strip('.json')))

    top_user = pd.DataFrame(clm6)

    top_user["State"] = top_user["State"].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
    top_user["State"] = top_user["State"].str.replace("-"," ")
    top_user["State"] = top_user["State"].str.title()
    top_user["State"] = top_user["State"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

    return top_user

top_user()

# Table creation

def Aggregate_transaction_table():

    connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
    connection

    mycursor = connection.cursor()

    # Aggregate_transaction table creation

    query_1 = '''CREATE TABLE IF NOT EXISTS Aggregate_transaction(State varchar(150),Year int,
                                                    Quarter int,Transacion_type varchar(200),
                                                    Transacion_count int,Transacion_amount bigint)'''


    mycursor.execute(query_1)
    connection.commit()

    

    for index,row in agg_trans_list().iterrows():
        insert_query_1 = '''insert into Aggregate_transaction(State,Year,
                                                    Quarter,Transacion_type,Transacion_count,
                                                    Transacion_amount)

                                        values(%s,%s,%s,%s,%s,%s)'''
        rows=(row['State'],row['Year'],row['Quarter'],row['Transacion_type'],row['Transacion_count'],row['Transacion_amount'])

        mycursor.execute(insert_query_1,rows)
        connection.commit()

Aggregate_transaction_table()

# Table creation

def aggregate_user_table():

    connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
    connection

    mycursor = connection.cursor()

    # Aggregate_user table creation

    query_1 = '''CREATE TABLE IF NOT EXISTS Aggregate_user(State varchar(150),Year int,
                                                    Quarter int,Brands varchar(150),Count int,
                                                    Percentage float)'''


    mycursor.execute(query_1)
    connection.commit()

    for index,row in aggregate_user().iterrows():
        insert_query_1 = '''insert into Aggregate_user(State,Year,
                                                    Quarter,Brands,Count,
                                                    Percentage)

                                        values(%s,%s,%s,%s,%s,%s)'''
        rows=(row['State'],row['Year'],row['Quarter'],row['Brands'],row['Count'],row['Percentage'])

        mycursor.execute(insert_query_1,rows)
        connection.commit()

aggregate_user_table()

# Table creation

def map_transaction_table():

    connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
    connection

    mycursor = connection.cursor()

    # Map_transaction table creation

    query_1 = '''CREATE TABLE IF NOT EXISTS map_transaction(State varchar(150),Year int,
                                                    Quarter int,Districts varchar(150),Transaction_count int,
                                                    Transaction_amount bigint)'''


    mycursor.execute(query_1)
    connection.commit()

    for index,row in map_transaction().iterrows():
        insert_query_1 = '''insert into map_transaction(State,Year,
                                                    Quarter,Districts,Transaction_count,
                                                    Transaction_amount)

                                        values(%s,%s,%s,%s,%s,%s)'''
        rows=(row['State'],row['Year'],row['Quarter'],row['Districts'],row['Transaction_count'],row['Transaction_amount'])

        mycursor.execute(insert_query_1,rows)
        connection.commit()

map_transaction_table()

# Table creation

def map_user_table():

    connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
    connection

    mycursor = connection.cursor()

    # Map_user table creation

    query_1 = '''CREATE TABLE IF NOT EXISTS map_user(State varchar(150),Year int,
                                                    Quarter int,Districts varchar(150),Registered_users int,
                                                    AppOpens bigint)'''


    mycursor.execute(query_1)
    connection.commit()

    for index,row in map_user().iterrows():
        insert_query_1 = '''insert into map_user(State,Year,
                                                    Quarter,Districts,Registered_users,
                                                    AppOpens)

                                        values(%s,%s,%s,%s,%s,%s)'''
        rows=(row['State'],row['Year'],row['Quarter'],row['Districts'],row['Registered_users'],row['AppOpens'])

        mycursor.execute(insert_query_1,rows)
        connection.commit()

map_user_table()

# Table creation

def top_transaction_table():
    connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
    connection

    mycursor = connection.cursor()

    # Top_transaction table creation

    query_1 = '''CREATE TABLE IF NOT EXISTS top_tranaction(State varchar(150),Year int,
                                                    Quarter int,Pincodes varchar(150),Transaction_count int,
                                                    Transaction_amount bigint)'''


    mycursor.execute(query_1)
    connection.commit()

    for index,row in top_transaction().iterrows():
        insert_query_1 = '''insert into top_tranaction(State,Year,
                                                    Quarter,Pincodes,Transaction_count,
                                                    Transaction_amount)

                                        values(%s,%s,%s,%s,%s,%s)'''
        rows=(row['State'],row['Year'],row['Quarter'],row['Pincodes'],row['Transaction_count'],row['Transaction_amount'])

        mycursor.execute(insert_query_1,rows)
        connection.commit()

top_transaction_table()

# Table creation

def top_user_table():

    connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")
    connection

    mycursor = connection.cursor()

    # Top_user table creation

    query_1 = '''CREATE TABLE IF NOT EXISTS top_user(State varchar(150),Year int,
                                                    Quarter int,Pincodes varchar(150),Registered_users int
                                                    )'''


    mycursor.execute(query_1)
    connection.commit()

    for index,row in top_user().iterrows():
        insert_query_1 = '''insert into top_user(State,Year,
                                                    Quarter,Pincodes,Registered_users
                                                    )

                                        values(%s,%s,%s,%s,%s)'''
        rows=(row['State'],row['Year'],row['Quarter'],row['Pincodes'],row['Registered_users'])

        mycursor.execute(insert_query_1,rows)
        connection.commit()

top_user_table()

