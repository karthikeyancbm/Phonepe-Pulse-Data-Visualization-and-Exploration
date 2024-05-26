import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import mysql
import mysql.connector
import plotly.express as px
import numpy as np
import requests
import json



st.set_page_config(layout="wide")

st.title(":violet[PHONEPE DATA] :rainbow[EXPLORATION AND VISUALIZATION]")

connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")

mycursor = connection.cursor()

# Aggregated transaction Dataframe

def agg_trans_frame():

    query = "select * from aggregate_transaction"
    mycursor.execute(query)
    result1 = mycursor.fetchall()

    Aggregated_transaction = pd.DataFrame(result1,columns=("State","Year","Quarter","Transaction_type","Transaction_count","Transaction_amount"))

    return Aggregated_transaction

# Aggregated user Dataframe

def agg_user_frame():

    query = "select * from aggregate_user"
    mycursor.execute(query)
    result2 = mycursor.fetchall()

    Aggregated_user = pd.DataFrame(result2,columns=("State","Year","Quarter","Brands","Count","Percentage"))

    return Aggregated_user

# Map transaction Dataframe

def map_trans_frame():

    query = "select * from map_transaction"
    mycursor.execute(query)
    result6 = mycursor.fetchall()

    Map_transaction = pd.DataFrame(result6,columns=("State","Year","Quarter","Districts","Transaction_count","Transaction_amount"))

    return Map_transaction

# Map User Dataframe

def map_user_frame():

    query = "select * from map_user"
    mycursor.execute(query)
    result3 = mycursor.fetchall()

    Map_user = pd.DataFrame(result3,columns=("State","Year","Quarter","Districts","Registered_users","AppOpens"))

    return Map_user

# Top_transaction Dataframe

def top_trans_frame():

    query = "select * from top_tranaction"
    mycursor.execute(query)
    result4 = mycursor.fetchall()

    Top_transaction = pd.DataFrame(result4,columns=("State","Year","Quarter","Pincodes","Transaction_count","Transaction_amount"))

    return Top_transaction

# Top user Dataframe

def top_user_frame():
    
    query = "select * from top_user"
    mycursor.execute(query)
    result5 = mycursor.fetchall()

    Top_user = pd.DataFrame(result5,columns=("State","Year","Quarter","Pincodes","Registered_users"))

    return Top_user


def agg_trans_one(state,year,quarter):
        
        if state == "All States":

            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            
            query ='''select State, sum(Transacion_amount/10000000) as Transaction_amount, sum(Transacion_count/10000000) as Transaction_count from aggregate_transaction where  year = %s and Quarter = %s
                    group by State order by Transaction_count desc'''

            mycursor.execute(query,(year,quarter))
            result1 = mycursor.fetchall()
            agg_sum_count = pd.DataFrame(result1,columns=("State","Transaction_amount","Transaction_count"))
            df = agg_sum_count
            fig = px.choropleth(
            df,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations='State',
            color='Transaction_amount',
            color_continuous_scale='purples',
            title='Transaction Data Analysis'
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800,width=800)

            df['State'] = df['State'].astype(str)
            df['Transaction_amount'] = df['Transaction_amount'].astype(float)
            df_fig = px.bar(df, x='State', y='Transaction_amount',
                            color='Transaction_amount', color_continuous_scale='thermal',
                            title=f'Transaction Analysis Chart:{state}', height=700,
                            text='Transaction_amount')
            df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')

            return agg_sum_count,fig,df_fig
        

        
        else:
    
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            
            query ='''select State, sum(Transacion_amount/10000000) as Transaction_amount, sum(Transacion_count/10000000) as Transaction_count from aggregate_transaction where state = %s and  year = %s and Quarter = %s
                    group by State order by Transaction_count desc'''

            mycursor.execute(query,(state,year,quarter))
            result1 = mycursor.fetchall()
            agg_sum_count = pd.DataFrame(result1,columns=("State","Transaction_amount","Transaction_count"))
            df = agg_sum_count
            fig = px.choropleth(
            df,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations='State',
            color='Transaction_amount',
            color_continuous_scale='purples',
            title='Transaction Data Analysis'
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800,width=800)

            df['State'] = df['State'].astype(str)
            df['Transaction_amount'] = df['Transaction_amount'].astype(float)
            df_fig = px.bar(df, x='State', y='Transaction_amount',
                            color='Transaction_amount', color_continuous_scale='thermal',
                            title=f'Transaction Analysis Chart:{state}', height=700,
                            text = 'Transaction_amount')
            df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')

            return agg_sum_count,fig,df_fig
        
def agg_trans_two(state,year,quarter):

    if state == "All States":

        query = '''select State as States,Transacion_type as Transaction_type,sum(Transacion_amount/1000000) as Transaction_amount from aggregate_transaction where  year = %s and quarter = %s group by State,Transacion_type order by Transaction_amount desc'''

        mycursor.execute(query,(year,quarter))
        result2 = mycursor.fetchall()
        agg_sum_type = pd.DataFrame(result2,columns=("States","Transaction_type","Transaction_amount"))
        df = agg_sum_type
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Transaction_amount',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart(Amt in Millions):{state}', height=700,
                        text='Transaction_amount')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')  
    
        return agg_sum_type,df_fig
    
    else:

        query = '''select State as States,Transacion_type as Transaction_type,sum(Transacion_amount/1000000) as Transaction_amount from aggregate_transaction where state = %s and  year = %s and quarter = %s group by State,Transacion_type order by Transaction_amount desc'''

        mycursor.execute(query,(state,year,quarter))
        result2 = mycursor.fetchall()
        agg_sum_type = pd.DataFrame(result2,columns=("States","Transaction_type","Transaction_amount"))
        df = agg_sum_type
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Transaction_amount',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart(Amt in Millions):{state}', height=700,
                        text='Transaction_amount')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')  
    
        return agg_sum_type,df_fig

def agg_trans_three(state,year,quarter):

    if state == "All States":

        query = '''select State as States,Transacion_type as Transaction_type,sum(Transacion_count) as Transaction_count from aggregate_transaction where  year = %s and quarter = %s group by State,Transacion_type order by Transaction_count desc'''

        mycursor.execute(query,(year,quarter))
        result3 = mycursor.fetchall()
        agg_sum_type_count = pd.DataFrame(result3,columns=("States","Transaction_type","Transaction_count"))
        df = agg_sum_type_count
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Transaction_count',
                        color='Transaction_count', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_count')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
    
        return agg_sum_type_count,df_fig
    else:
        query = '''select State as States,Transacion_type as Transaction_type,sum(Transacion_count) as Transaction_count from aggregate_transaction where state = %s and year = %s and quarter = %s group by State,Transacion_type order by Transaction_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result3 = mycursor.fetchall()
        agg_sum_type_count = pd.DataFrame(result3,columns=("States","Transaction_type","Transaction_count"))
        df = agg_sum_type_count
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Transaction_count',
                        color='Transaction_count', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_count')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
    
        return agg_sum_type_count,df_fig

def agg_trans_four(state,year,quarter):
    
    if state == "All States":

        query = '''select State as States,Transacion_type as Transaction_type,avg(Transacion_amount/10000000) as Average_Transaction_amount from aggregate_transaction where year = %s and quarter = %s group by State,Transacion_type order by Average_Transaction_amount desc'''

        mycursor.execute(query,(year,quarter))
        result4 = mycursor.fetchall()
        agg_trans_amt_avg = pd.DataFrame(result4,columns=("States","Transaction_type","Average_Transaction_amount"))
        df = agg_trans_amt_avg
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Average_Transaction_amount'] = df['Average_Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Average_Transaction_amount',
                        color='Average_Transaction_amount', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{state}', height=700,
                        text = 'Average_Transaction_amount')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_trans_amt_avg,df_fig
    else:
        query = '''select State as States,Transacion_type as Transaction_type,avg(Transacion_amount/10000000) as Average_Transaction_amount from aggregate_transaction where state = %s and year = %s and quarter = %s group by State,Transacion_type order by Average_Transaction_amount desc'''

        mycursor.execute(query,(state,year,quarter))
        result4 = mycursor.fetchall()
        agg_trans_amt_avg = pd.DataFrame(result4,columns=("States","Transaction_type","Average_Transaction_amount"))
        df = agg_trans_amt_avg
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Average_Transaction_amount'] = df['Average_Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Average_Transaction_amount',
                        color='Average_Transaction_amount', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{state}', height=700,
                        text = 'Average_Transaction_amount')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_trans_amt_avg,df_fig

def agg_trans_five(state,year,quarter):

    if state == "All States":

        query = '''select State as States,Transacion_type as Transaction_type,avg(Transacion_count/10000000) as Average_Transaction_count from aggregate_transaction where  year = %s and quarter = %s group by State,Transacion_type order by Average_Transaction_count desc'''

        mycursor.execute(query,(year,quarter))
        result5 = mycursor.fetchall()
        agg_avg_trans_count = pd.DataFrame(result5,columns=("States","Transaction_type","Average_Transaction_count"))
        df = agg_avg_trans_count
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Average_Transaction_count'] = df['Average_Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Average_Transaction_count',
                        color='Average_Transaction_count', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{state}', height=700,
                        text = 'Average_Transaction_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_avg_trans_count,df_fig
    
    else:

        query = '''select State as States,Transacion_type as Transaction_type,avg(Transacion_count/10000000) as Average_Transaction_count from aggregate_transaction where state = %s and  year = %s and quarter = %s group by State,Transacion_type order by Average_Transaction_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result5 = mycursor.fetchall()
        agg_avg_trans_count = pd.DataFrame(result5,columns=("States","Transaction_type","Average_Transaction_count"))
        df = agg_avg_trans_count
        df['Transaction_type'] = df['Transaction_type'].astype(str)
        df['Average_Transaction_count'] = df['Average_Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Transaction_type', y='Average_Transaction_count',
                        color='Average_Transaction_count', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{state}', height=700,
                        text = 'Average_Transaction_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_avg_trans_count,df_fig

def agg_user_one(state,year,quarter):

    if state == "All States":

        query ='''select State, Brands, sum(Count) as Total_count from Aggregate_user where  Year = %s and Quarter = %s
                group by State,Brands order by Total_count desc'''

        mycursor.execute(query,(year,quarter))
        result9 = mycursor.fetchall()
        agg_user = pd.DataFrame(result9,columns=("State","Brands","Total_count"))
        df = agg_user
        df['Brands'] = df['Brands'].astype(str)
        df['Total_count'] = df['Total_count'].astype(float)
        df_fig = px.bar(df, x='Brands', y='Total_count',
                        color='Total_count', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{state}', height=700,
                        text = 'Total_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')

        return agg_user,df_fig
    
    else:

        query ='''select State, Brands, sum(Count) as Total_count from Aggregate_user where state = %s and Year = %s and Quarter = %s
                group by State,Brands order by Total_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result9 = mycursor.fetchall()
        agg_user = pd.DataFrame(result9,columns=("State","Brands","Total_count"))
        df = agg_user
        df['Brands'] = df['Brands'].astype(str)
        df['Total_count'] = df['Total_count'].astype(float)
        df_fig = px.bar(df, x='Brands', y='Total_count',
                        color='Total_count', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{state}', height=700,
                        text = 'Total_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')

        return agg_user,df_fig

def agg_user_two(state,year,quarter):

     if state == "All States":

        query ='''select State, Brands, sum(Percentage) as Total_Percentage from Aggregate_user where Year = %s and Quarter = %s
                group by State,Brands order by Total_Percentage '''

        mycursor.execute(query,(year,quarter))
        result10 = mycursor.fetchall()
        agg_user_percent = pd.DataFrame(result10,columns=("State","Brands","Total_Percentage"))
        df = agg_user_percent
        df['Brands'] = df['Brands'].astype(str)
        df['Total_Percentage'] = df['Total_Percentage'].astype(float)
        df_fig = px.bar(df, x='Brands', y='Total_Percentage',
                        color='Total_Percentage', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{state}', height=700,
                        text = 'Total_Percentage')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return agg_user_percent,df_fig
     
     else:
         
        query ='''select State, Brands, sum(Percentage) as Total_Percentage from Aggregate_user where state = %s and Year = %s and Quarter = %s
                group by State,Brands order by Total_Percentage '''

        mycursor.execute(query,(state,year,quarter))
        result10 = mycursor.fetchall()
        agg_user_percent = pd.DataFrame(result10,columns=("State","Brands","Total_Percentage"))
        df = agg_user_percent
        df['Brands'] = df['Brands'].astype(str)
        df['Total_Percentage'] = df['Total_Percentage'].astype(float)
        df_fig = px.bar(df, x='Brands', y='Total_Percentage',
                        color='Total_Percentage', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{state}', height=700,
                        text = 'Total_Percentage')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return agg_user_percent,df_fig

def map_trans(state,year,quarter):

    if state == "All States":

        query ='''select State, Districts, sum(Transaction_amount/10000000) as Transaction_amount, sum(Transaction_count/10000000) as Transaction_count from Map_transaction where year = %s and Quarter = %s
                group by State, Districts order by Transaction_count desc'''

        mycursor.execute(query,(year,quarter))
        result6 = mycursor.fetchall()
        map_sum_count = pd.DataFrame(result6,columns=("State","Districts","Transaction_amount","Transaction_count"))
        df = map_sum_count
        df['State'] = df['State'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='State', y='Transaction_amount',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_amount')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count,df_fig
    
    else:

        query ='''select State, Districts, sum(Transaction_amount/10000000) as Transaction_amount, sum(Transaction_count/10000000) as Transaction_count from Map_transaction where state = %s and year = %s and Quarter = %s
                group by State, Districts order by Transaction_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result6 = mycursor.fetchall()
        map_sum_count = pd.DataFrame(result6,columns=("State","Districts","Transaction_amount","Transaction_count"))
        df = map_sum_count
        df['State'] = df['State'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='State', y='Transaction_amount',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_amount')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count,df_fig

def map_trans_amount(state,year,quarter):

    if state == "All States":

        query ='''select State, Districts, sum(Transaction_amount/10000000) as Transaction_amount from Map_transaction where year = %s and Quarter = %s
                group by State, Districts order by Transaction_amount desc'''

        mycursor.execute(query,(year,quarter))
        result7 = mycursor.fetchall()
        map_sum_amount_alone = pd.DataFrame(result7,columns=("State","Districts","Transaction_amount"))
        df = map_sum_amount_alone
        df['Districts'] = df['Districts'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Transaction_amount',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_amount')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_amount_alone,df_fig
    
    else:

        query ='''select State, Districts, sum(Transaction_amount/10000000) as Transaction_amount from Map_transaction where state = %s and year = %s and Quarter = %s
                group by State, Districts order by Transaction_amount desc'''

        mycursor.execute(query,(state,year,quarter))
        result7 = mycursor.fetchall()
        map_sum_amount_alone = pd.DataFrame(result7,columns=("State","Districts","Transaction_amount"))
        df = map_sum_amount_alone
        df['Districts'] = df['Districts'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Transaction_amount',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_amount')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_amount_alone,df_fig

def map_trans_count(state,year,quarter):

    if state == "All States":

        query ='''select State, Districts, sum(Transaction_count/10000000) as Transaction_count from Map_transaction where year = %s and Quarter = %s
                group by State, Districts order by Transaction_count desc'''

        mycursor.execute(query,(year,quarter))
        result8 = mycursor.fetchall()
        map_sum_count_alone = pd.DataFrame(result8,columns=("State","Districts","Transaction_count"))
        df = map_sum_count_alone
        df['Districts'] = df['Districts'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Transaction_count',
                        color='Transaction_count', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count_alone,df_fig
        
    
    else:

        query ='''select State, Districts, sum(Transaction_count/10000000) as Transaction_count from Map_transaction where state = %s and  year = %s and Quarter = %s
                group by State, Districts order by Transaction_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result8 = mycursor.fetchall()
        map_sum_count_alone = pd.DataFrame(result8,columns=("State","Districts","Transaction_count"))
        df = map_sum_count_alone
        df['Districts'] = df['Districts'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Transaction_count',
                        color='Transaction_count', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count_alone,df_fig
        
def get_map_user(state,year,quarter):

    if state == "All States":

        query ='''select State, Districts, sum(Registered_users) as Total_Registered_users, sum(AppOpens) as Total_AppOpens from Map_user where year = %s and Quarter = %s
                group by State, Districts order by Total_Registered_users desc'''

        mycursor.execute(query,(year,quarter))
        result11 = mycursor.fetchall()
        map_user = pd.DataFrame(result11,columns=("State","Districts","Total_Registered_users","Total_AppOpens"))
        df = map_user
        df['Total_AppOpens'] = df['Total_AppOpens'].astype(str)
        df['Total_Registered_users'] = df['Total_Registered_users'].astype(float)
        df_fig = px.bar(df, x='Total_AppOpens', y='Total_Registered_users',
                        color='Total_AppOpens', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{state}', height=700,
                        text = 'Total_Registered_users')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_user,df_fig
        
    
    else:

        query ='''select State, Districts, sum(Registered_users) as Total_Registered_users, sum(AppOpens) as Total_AppOpens from Map_user where state = %s and year = %s and Quarter = %s
                group by State, Districts order by Total_Registered_users desc'''

        mycursor.execute(query,(state,year,quarter))
        result11 = mycursor.fetchall()
        map_user = pd.DataFrame(result11,columns=("State","Districts","Total_Registered_users","Total_AppOpens"))
        df = map_user
        df['Total_AppOpens'] = df['Total_AppOpens'].astype(str)
        df['Total_Registered_users'] = df['Total_Registered_users'].astype(float)
        df_fig = px.bar(df, x='Total_AppOpens', y='Total_Registered_users',
                        color='Total_AppOpens', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{state}', height=700,
                        text = 'Total_Registered_users')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_user,df_fig
        
def get_map_reg_user(state,year,quarter):

    if state == "All States":
    
        query ='''select State, Districts, sum(Registered_users) as Total_Registered_users from Map_user where year = %s and Quarter = %s
                group by State, Districts order by Total_Registered_users desc'''

        mycursor.execute(query,(year,quarter))
        result12 = mycursor.fetchall()
        map_reg_user = pd.DataFrame(result12,columns=("State","Districts","Total_Registered_users"))
        df = map_reg_user
        df['Districts'] = df['Districts'].astype(str)
        df['Total_Registered_users'] = df['Total_Registered_users'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Total_Registered_users',
                        color='Total_Registered_users', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{state}', height=700,
                        text = 'Total_Registered_users')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_reg_user,df_fig
        
    
    else:

        query ='''select State, Districts, sum(Registered_users) as Total_Registered_users from Map_user where state = %s and  year = %s and Quarter = %s
                group by State, Districts order by Total_Registered_users desc'''

        mycursor.execute(query,(state,year,quarter))
        result12 = mycursor.fetchall()
        map_reg_user = pd.DataFrame(result12,columns=("State","Districts","Total_Registered_users"))
        df = map_reg_user
        df['Districts'] = df['Districts'].astype(str)
        df['Total_Registered_users'] = df['Total_Registered_users'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Total_Registered_users',
                        color='Total_Registered_users', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{state}', height=700,
                        text = 'Total_Registered_users')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_reg_user,df_fig
        
def get_map_appOpens(state,year,quarter):

    if state == "All States":

        query ='''select State, Districts, sum(AppOpens) as Total_AppOpens from Map_user where year = %s and Quarter = %s
                group by State, Districts order by Total_AppOpens desc'''

        mycursor.execute(query,(year,quarter))
        result13 = mycursor.fetchall()
        map_app_opens = pd.DataFrame(result13,columns=("State","Districts","Total_AppOpens"))
        df = map_app_opens
        df['Districts'] = df['Districts'].astype(str)
        df['Total_AppOpens'] = df['Total_AppOpens'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Total_AppOpens',
                        color='Total_AppOpens', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{state}', height=700,
                        text = 'Total_AppOpens')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_app_opens,df_fig
       
    
    else:

        query ='''select State, Districts, sum(AppOpens) as Total_AppOpens from Map_user where state = %s and year = %s and Quarter = %s
                group by State, Districts order by Total_AppOpens desc'''

        mycursor.execute(query,(state,year,quarter))
        result13 = mycursor.fetchall()
        map_app_opens = pd.DataFrame(result13,columns=("State","Districts","Total_AppOpens"))
        df = map_app_opens
        df['Districts'] = df['Districts'].astype(str)
        df['Total_AppOpens'] = df['Total_AppOpens'].astype(float)
        df_fig = px.bar(df, x='Districts', y='Total_AppOpens',
                        color='Total_AppOpens', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{state}', height=700,
                        text = 'Total_AppOpens')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_app_opens,df_fig
       
def get_top_trans(state,year,quarter):

    if state == "All States":

        query ='''select State, Pincodes, sum(Transaction_amount) as Transaction_amount, sum(Transaction_count) as Transaction_count from top_tranaction where year = %s and Quarter = %s
                group by State, Pincodes order by Transaction_count desc'''

        mycursor.execute(query,(year,quarter))
        result14 = mycursor.fetchall()
        top_sum = pd.DataFrame(result14,columns=("State","Pincodes","Transaction_amount","Transaction_count"))
        df = top_sum
        df['Transaction_amount'] = df['Transaction_amount'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Transaction_amount', y='Transaction_count',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_amount')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum,df_fig
        
    
    else:

        query ='''select State, Pincodes, sum(Transaction_amount) as Transaction_amount, sum(Transaction_count) as Transaction_count from top_tranaction where state = %s and year = %s and Quarter = %s
                group by State, Pincodes order by Transaction_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result14 = mycursor.fetchall()
        top_sum = pd.DataFrame(result14,columns=("State","Pincodes","Transaction_amount","Transaction_count"))
        df = top_sum
        df['Transaction_amount'] = df['Transaction_amount'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Transaction_amount', y='Transaction_count',
                        color='Transaction_amount', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Transaction_amount')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum,df_fig
        
def get_top_trans_amount(state,year,quarter):

    if state == "All States":

        query ='''select State, Pincodes, sum(Transaction_amount) as Transaction_amount from top_tranaction where year = %s and Quarter = %s
                group by State, Pincodes order by Transaction_amount desc'''

        mycursor.execute(query,(year,quarter))
        result15 = mycursor.fetchall()
        top_sum_amount = pd.DataFrame(result15,columns=("State","Pincodes","Transaction_amount"))
        df = top_sum_amount
        df['Pincodes'] = df['Pincodes'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Pincodes', y='Transaction_amount',
                        color='Pincodes', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Pincodes')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum_amount,df_fig
        
    
    else:

        query ='''select State, Pincodes, sum(Transaction_amount) as Transaction_amount from top_tranaction where state = %s and year = %s and Quarter = %s
                group by State, Pincodes order by Transaction_amount desc'''

        mycursor.execute(query,(state,year,quarter))
        result15 = mycursor.fetchall()
        top_sum_amount = pd.DataFrame(result15,columns=("State","Pincodes","Transaction_amount"))
        df = top_sum_amount
        df['Pincodes'] = df['Pincodes'].astype(str)
        df['Transaction_amount'] = df['Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='Pincodes', y='Transaction_amount',
                        color='Pincodes', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Pincodes')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum_amount,df_fig  

def get_top_trans_count(state,year,quarter):

    if state == "All States":

        query ='''select State, Pincodes, sum(Transaction_count) as Transaction_count from top_tranaction where year = %s and Quarter = %s
                group by State, Pincodes order by Transaction_count desc'''

        mycursor.execute(query,(year,quarter))
        result16 = mycursor.fetchall()
        top_sum_count = pd.DataFrame(result16,columns=("State","Pincodes","Transaction_count"))
        df = top_sum_count
        df['Pincodes'] = df['Pincodes'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Pincodes', y='Transaction_count',
                        color='Pincodes', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Pincodes')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum_count,df_fig  
        
    
    else:

        query ='''select State, Pincodes, sum(Transaction_count) as Transaction_count from top_tranaction where state = %s and year = %s and Quarter = %s
                group by State, Pincodes order by Transaction_count desc'''

        mycursor.execute(query,(state,year,quarter))
        result16 = mycursor.fetchall()
        top_sum_count = pd.DataFrame(result16,columns=("State","Pincodes","Transaction_count"))
        df = top_sum_count
        df['Pincodes'] = df['Pincodes'].astype(str)
        df['Transaction_count'] = df['Transaction_count'].astype(float)
        df_fig = px.bar(df, x='Pincodes', y='Transaction_count',
                        color='Pincodes', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Pincodes')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum_count,df_fig 

def get_top_user(state,year,quarter):

    if state == "All States":

        query ='''select State, Pincodes, sum(Registered_users) as Total_Registered_users from top_user where year = %s and Quarter = %s
                group by State, Pincodes order by Total_Registered_users desc'''

        mycursor.execute(query,(year,quarter))
        result17 = mycursor.fetchall()
        top_reg_user = pd.DataFrame(result17,columns=("State","Pincodes","Total_Registered_users"))
        df = top_reg_user
        df['Pincodes'] = df['Pincodes'].astype(str)
        df['Total_Registered_users'] = df['Total_Registered_users'].astype(float)
        df_fig = px.bar(df, x='Pincodes', y='Total_Registered_users',
                        color='Total_Registered_users', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Pincodes')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_reg_user,df_fig
        
    
    else:

        query ='''select State, Pincodes, sum(Registered_users) as Total_Registered_users from top_user where state = %s and year = %s and Quarter = %s
                group by State, Pincodes order by Total_Registered_users desc'''

        mycursor.execute(query,(state,year,quarter))
        result17 = mycursor.fetchall()
        top_reg_user = pd.DataFrame(result17,columns=("State","Pincodes","Total_Registered_users"))
        df = top_reg_user
        df['Pincodes'] = df['Pincodes'].astype(str)
        df['Total_Registered_users'] = df['Total_Registered_users'].astype(float)
        df_fig = px.bar(df, x='Pincodes', y='Total_Registered_users',
                        color='Total_Registered_users', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{state}', height=700,
                        text = 'Pincodes')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_reg_user,df_fig
        
# Top Charts

#Question 1 "Top brands of mobile used"

def top_mobile_brand(year,quarter):

    query ='''select Brands, sum(Count) as Brand_count from Aggregate_user where Year = %s and Quarter = %s
            group by Brands order by Brand_count desc'''

    mycursor.execute(query,(year,quarter))
    result18 = mycursor.fetchall()
    top_brand = pd.DataFrame(result18,columns=("Brands","Brand_count"))
    return top_brand

# Question 2 Top 10 District With Lowest Transaction Amount"

def low_transaction(year,quarter):

    query ='''select Districts, sum(Transaction_amount) as Transaction_amount from Map_transaction where Year = %s and Quarter = %s
            group by Districts order by Transaction_amount asc limit 10'''

    mycursor.execute(query,(year,quarter))
    result19 = mycursor.fetchall()
    top_brand = pd.DataFrame(result19,columns=("Districts","Transaction_amount"))
    return top_brand

# Question 3 Top 10 District With Highest Transaction Amount"

def high_transaction(year,quarter):

    query ='''select Districts, sum(Transaction_amount) as Transaction_amount from Map_transaction where Year = %s and Quarter = %s
            group by Districts order by Transaction_amount desc limit 10'''

    mycursor.execute(query,(year,quarter))
    result20 = mycursor.fetchall()
    high_trans = pd.DataFrame(result20,columns=("Districts","Transaction_amount"))
    return high_trans

# Question 4 "PhonePe users from 2018 to 2023"

def users_count(st_year,end_year):

    query ='''select Year, count(*) as Registered_users from Map_user where Year between  %s and  %s
            group by Year order by Year '''

    mycursor.execute(query,(st_year,end_year))
    result21 = mycursor.fetchall()
    users_sum = pd.DataFrame(result21,columns=("Year","Registered_users"))
    return users_sum

# Question 5 Top 10 Districts with Highest PhonePe User"

def high_users_count(year,quarter):

    query ='''select State, max(Registered_users) as Registered_users from Map_user where Year = %s and Quarter = %s
            group by State order by Registered_users desc limit 100'''

    mycursor.execute(query,(year,quarter))
    result22 = mycursor.fetchall()
    high_users = pd.DataFrame(result22,columns=("State","Registered_users"))
    return high_users

# Question 6 "Top 10 States with Lowest PhonePe User"

def low_users_count(year,quarter):

    query ='''select State, min(Registered_users) as Registered_users from Map_user where Year = %s and Quarter = %s
            group by State order by Registered_users asc limit 100'''

    mycursor.execute(query,(year,quarter))
    result23 = mycursor.fetchall()
    low_users = pd.DataFrame(result23,columns=("State","Registered_users"))
    return low_users

# Question 7 "Top 10 Districts with Highest PhonePe User"

def high_users_dist_count(year,quarter):

    query ='''select Districts, max(Registered_users) as Registered_users from Map_user where Year = %s and Quarter = %s
            group by Districts order by Registered_users desc limit 100'''

    mycursor.execute(query,(year,quarter))
    result24 = mycursor.fetchall()
    low_users = pd.DataFrame(result24,columns=("Districts","Registered_users"))
    return low_users

# Question 8 "Top 10 Districts with lowest PhonePe User"

def low_users_dist_count(year,quarter):

    query ='''select Districts, min(Registered_users) as Registered_users from Map_user where Year = %s and Quarter = %s
            group by Districts order by Registered_users asc limit 100'''

    mycursor.execute(query,(year,quarter))
    result25 = mycursor.fetchall()
    low_users = pd.DataFrame(result25,columns=("Districts","Registered_users"))
    return low_users

# Question 9 "Top 10 District with Highest Transaction Count"

def high_map_transaction(year,quarter):

    query ='''select Districts, max(Transaction_count) as Transaction_count from Map_transaction where Year = %s and Quarter = %s
            group by Districts order by Transaction_count desc limit 100'''

    mycursor.execute(query,(year,quarter))
    result26 = mycursor.fetchall()
    high_trans = pd.DataFrame(result26,columns=("Districts","Transaction_count"))
    return high_trans

# Question 10 "Top 10 District with lowest Transaction Count"

def low_map_transaction(year,quarter):

    query ='''select Districts, min(Transaction_count) as Transaction_count from Map_transaction where Year = %s and Quarter = %s
            group by Districts order by Transaction_count asc limit 100'''

    mycursor.execute(query,(year,quarter))
    result27 = mycursor.fetchall()
    high_trans = pd.DataFrame(result27,columns=("Districts","Transaction_count"))
    return high_trans

def get_year():

    year = np.array (agg_trans_frame()["Year"].unique()).tolist()

    year_1 = tuple([str(d) for d in year])

    return year_1

def get_quarter():

    quarter = np.array(agg_trans_frame()["Quarter"].unique()).tolist()

    quarter_1 = tuple([str(d) for d in quarter])

    return quarter_1

def get_state():

    state = np.array (agg_trans_frame()["State"].unique()).tolist()

    state.insert(0,"All States")

    state_choice = tuple([str(e) for e in state])

    return state_choice

def get_type():

    Type = ["None","Transaction amount and Transaction count","Transaction type vs Transaction amount","Transaction type vs Transaction count",
        
        "Transaction_type vs Average_Transaction_amount","Transaction_type vs Average_Transaction_count"       
        ]

    type_1 = tuple([str(i) for i in Type])

    return type_1

def get_type_2():

    Type_agg_user = ["None","Brands vs Total count","Brands vs Total Percentage"]

    type_2 = tuple([str(j) for j in Type_agg_user])

    return type_2

def get_map_type():

    map_trans_type = ["None","Transaction_amount and Transaction_count","Districts vs Transaction_amount","Districts vs Transaction_count"]

    map_type = tuple([str(k) for k in map_trans_type])

    return map_type

def get_map_user_data():

    map_user_type = ["None","Total_Registered_users and Total_AppOpens","Districts vs Total_Registered_users","Districts vs Total_AppOpens"]

    map_user_data = tuple([str(l) for l in map_user_type])

    return map_user_data

def get_top_trans_data():

    top_trans_type = ["None","Transaction_amount and Transaction_count","Pincodes vs Transaction_amount","Pincodes vs Transaction_count"]

    top_trans_data = tuple([str(m) for m in top_trans_type])

    return top_trans_data

def get_top_user_data():

    top_user_type = ["None","Pincodes vs Total Registered_users"]

    top_user_data = tuple([str(n) for n in top_user_type])

    return top_user_data


# Streamlit Part

with st.sidebar:

    select = option_menu("Main Menu",["HOME","DATA EXPLORATION","TOP CHARTS"])

if select == "HOME":

    col1, col2 = st.columns(2)

    with col1:
        st.header("**PhonePe**")
        st.subheader("_India's Best Transaction App_")
        st.write("""_PhonePe is a digital wallet and mobile payment platform in India.It uses the Unified Payment Interface (UPI) system to allow users to send and receive money recharge mobile, DTH, data cards,make utility payments,pay at shops,invest in tax saving funds,buy insurance, mutual funds and digital gold._""")
        st.write("****FEATURES****")
        st.write("   **- Fund Transfer**")
        st.write("   **- Payment to Merchant**")
        st.write("   **- Recharge and Bill payments**")
        st.write("   **- Autopay of Bills**")
        st.write("   **- Cashback and Rewards and much more**")
        st.link_button(":violet[**DOWNLOAD THE APP NOW**]", "https://www.phonepe.com/app-download/")
        
    with col2:
        st.subheader("Video about PhonePe")
        st.video("https://youtu.be/aXnNA4mv1dU?si=HnSu_ETm4X29Lrvf")
        st.write("***To know more about PhonePe click below***")
        st.link_button(":violet[**PhonePe**]", "https://www.phonepe.com/")

elif select == "DATA EXPLORATION":

    tab1,tab2,tab3 = st.tabs(["Aggregated Data Analysis","Map Data Analysis","Top Data Analysis"])

    with tab1:

        option = st.radio("Select the Option",["Transaction Data Analysis","User Data Analysis"])

        if option == "Transaction Data Analysis":

            col1,col2 = st.columns(2)

            with col1:

                select_type = st.selectbox("Select the Type",get_type(), index=0, placeholder="Select the Type...")

                selected_year = st.selectbox("Select the Year",get_year(), index=0, placeholder="Select the Year...")

            with col2:
                selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the State",get_state(), index=0, placeholder="Select the State...")

            if st.button(":violet[**Get Data**]"):

                if select_type == "Transaction amount and Transaction count":
                    agg_trans_1,fig,df_fig = agg_trans_one(selected_state,selected_year, selected_quarter)
                    st.plotly_chart(fig)
                    col1,col2 = st.columns(2)
                    with col1:
                        st.header(':violet[Transaction Data Analysis]')
                        st.dataframe(agg_trans_1)
                    with col2:
                        st.plotly_chart(df_fig)

                elif select_type == "Transaction type vs Transaction amount":
                    agg_trans_2,df_fig= agg_trans_two(selected_state,selected_year, selected_quarter)
                    #state_data_fig = state_alone(selected_state)
                    st.header(':violet[Transaction Type Data Analysis]')
                    st.dataframe(agg_trans_2)
                    st.plotly_chart(df_fig)                                        

                elif select_type == "Transaction type vs Transaction count":
                    agg_trans_3,df_fig = agg_trans_three(selected_state,selected_year, selected_quarter)
                    st.header(':violet[Transaction Type Count Data Analysis]')
                    st.dataframe(agg_trans_3)
                    st.plotly_chart(df_fig)
                
                elif select_type == "Transaction_type vs Average_Transaction_amount":
                    agg_trans_4,df_fig = agg_trans_four(selected_state,selected_year, selected_quarter)
                    st.header(':violet[Transaction Type Data Analysis]')
                    st.dataframe(agg_trans_4)
                    st.plotly_chart(df_fig)

                elif select_type == "Transaction_type vs Average_Transaction_count":
                    agg_trans_5,df_fig = agg_trans_five(selected_state,selected_year, selected_quarter)
                    col1,col2 = st.columns(2)
                    with col1:
                        st.header(':violet[Transaction Type Count Data Analysis]')
                        st.dataframe(agg_trans_5)
                    with col2:
                        st.plotly_chart(df_fig)              

                    
        elif option == "User Data Analysis":

            col1,col2 = st.columns(2)

            with col1:
                select_type = st.selectbox("Select the Type",get_type_2(), index=0, placeholder="Select the Type...")            
                selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            with col2:
                selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the State",get_state(), index=0, placeholder="Select the State...")


            if st.button(":violet[**Get Data**]"):

                if select_type == "Brands vs Total count":
                    agg_user_1,df_fig = agg_user_one(selected_state,selected_year,selected_quarter)
                    st.dataframe(agg_user_1)
                    st.plotly_chart(df_fig)

                elif select_type == "Brands vs Total Percentage":
                    agg_user_2,df_fig = agg_user_two(selected_state,selected_year,selected_quarter)
                    st.dataframe(agg_user_2)
                    st.plotly_chart(df_fig)
                
    with tab2:

        option1 = st.radio("Select the Option",["Map Transaction Data Analysis","Map User Data Analysis"])

        if option1 == "Map Transaction Data Analysis":

            col1,col2 = st.columns(2)

            with col1:
            
                select_type = st.selectbox("Select the Types",get_map_type(), index=0, placeholder="Select the Type...")
                selected_year = st.selectbox("Select the Years", get_year(), index=0, placeholder="Select the Year...")

            with col2:

                selected_quarter = st.selectbox("Select the Quarters", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the States",get_state(), index=0, placeholder="Select the State...")

            if st.button(":violet[**Submit**]"):

                if select_type == "Transaction_amount and Transaction_count":
                    map_trans_1,df_fig = map_trans(selected_state,selected_year,selected_quarter)
                    st.dataframe(map_trans_1)
                    st.plotly_chart(df_fig)
                
                elif select_type == "Districts vs Transaction_amount":
                    map_trans_2,df_fig = map_trans_amount(selected_state,selected_year, selected_quarter)
                    st.dataframe(map_trans_2)
                    st.plotly_chart(df_fig)

                elif select_type == "Districts vs Transaction_count":
                    map_trans_3,df_fig = map_trans_count(selected_state,selected_year, selected_quarter)
                    st.dataframe(map_trans_3)
                    st.plotly_chart(df_fig)

        
        elif option1 == "Map User Data Analysis":

            col1,col2 = st.columns(2)

            with col1:            
                select_type = st.selectbox("Select the Type",get_map_user_data(),index=0, placeholder="Select the Type...")            
                selected_year = st.selectbox("Select the years", get_year(), index=0, placeholder="Select the Year...")
            with col2:
                selected_quarter = st.selectbox("Select the quarters", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the States",get_state(), index=0, placeholder="Select the State...")

            if st.button(":violet[**Submit**]"):
                
                if select_type == "Total_Registered_users and Total_AppOpens":
                    map_user_1,df_fig = get_map_user(selected_state,selected_year,selected_quarter)
                    st.dataframe(map_user_1)
                    st.plotly_chart(df_fig)
                
                elif select_type == "Districts vs Total_Registered_users":
                    map_user_2,df_fig = get_map_reg_user(selected_state,selected_year, selected_quarter)
                    st.dataframe(map_user_2)
                    st.plotly_chart(df_fig)

                elif select_type == "Districts vs Total_AppOpens":
                    map_user_3,df_fig = get_map_appOpens(selected_state,selected_year, selected_quarter)
                    st.dataframe(map_user_3)
                    st.plotly_chart(df_fig)


    with tab3:

        option2 = st.radio("Select the Options",["Transaction Data Analysis","User Data Analysis"])

        if option2 == "Transaction Data Analysis":

            col1,col2 = st.columns(2)

            with col1:

                select_type = st.selectbox("Select the Type",get_top_trans_data(), index=0, placeholder="Select the Type...")
                selected_year = st.selectbox("Select the year", get_year(), index=0, placeholder="Select the Year...")
            with col2:
                selected_quarter = st.selectbox("Select the quarter", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the state",get_state(), index=0, placeholder="Select the State...")


            if st.button(":violet[**SUBMIT**]"):
                
                if select_type == "Transaction_amount and Transaction_count":
                    top_trans_1,df_fig = get_top_trans(selected_state,selected_year,selected_quarter)
                    st.dataframe(top_trans_1)
                    st.plotly_chart(df_fig)
                
                elif select_type == "Pincodes vs Transaction_amount":
                    top_trans_2,df_fig = get_top_trans_amount(selected_state,selected_year,selected_quarter)
                    st.dataframe(top_trans_2)
                    st.plotly_chart(df_fig)

                elif select_type == "Pincodes vs Transaction_count":
                    top_trans_3,df_fig = get_top_trans_count(selected_state,selected_year,selected_quarter)
                    st.dataframe(top_trans_3)
                    st.plotly_chart(df_fig)

        if option2 == "User Data Analysis":

            col1,col2 = st.columns(2)

            with col1:

                select_type = st.selectbox("Select the Type",get_top_user_data(), index=0, placeholder="Select the Type...")
                selected_year = st.selectbox("Select the year", get_year(), index=0, placeholder="Select the Year...")
            with col2:
                selected_quarter = st.selectbox("Select the quarter", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the state",get_state(), index=0, placeholder="Select the State...")

            if st.button(":violet[**SUBMIT**]"):
                
                if select_type == "Pincodes vs Total Registered_users":
                    top_user_1,df_fig= get_top_user(selected_state,selected_year,selected_quarter)
                    st.dataframe(top_user_1)
                    st.plotly_chart(df_fig)
                
                
elif select == "TOP CHARTS":

        options = st.selectbox(":violet[_Insights_]",("Select the Quries to be Analysed:",
            "1.Top brands of mobile used",
            "2.Top 10 District With Lowest Transaction Amount",
            "3.Top 10 District With Highest Transaction Amount",
            "4.PhonePe users from 2018 to 2023",
            "5.Top 10 States with Highest PhonePe User",
            "6.Top 10 States with Lowest PhonePe User",
            "7.Top 10 Districts with Highest PhonePe User",
            "8.Top 10 Districts with Lowest PhonePe User",
            "9.Top 10 District with Highest Transaction Count",
            "10.Top 10 District With Lowest Transaction Count",
            ),
            index=None,
            placeholder="Select the Query...",
            )


        st.write('You selected:', options)

        if options == "1.Top brands of mobile used":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")
        
            df1 = top_mobile_brand(selected_year,selected_quarter)
            df = df1
            df['Brands'] = df['Brands'].astype(str)
            df['Brand_count'] = df['Brand_count'].astype(float)
            df_fig = px.bar(df, x='Brands', y='Brand_count',
                            color='Brands', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Brand_count')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df1)
            st.plotly_chart(df_fig)

        elif options == "2.Top 10 District With Lowest Transaction Amount":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df2 = low_transaction(selected_year,selected_quarter)
            df = df2
            df['Districts'] = df['Districts'].astype(str)
            df['Transaction_amount'] = df['Transaction_amount'].astype(float)
            df_fig = px.bar(df, x='Districts', y='Transaction_amount',
                            color='Districts', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Transaction_amount')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df2)
            st.plotly_chart(df_fig)

        elif options == "3.Top 10 District With Highest Transaction Amount":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df3 = high_transaction(selected_year,selected_quarter)
            df = df3
            df['Districts'] = df['Districts'].astype(str)
            df['Transaction_amount'] = df['Transaction_amount'].astype(float)
            df_fig = px.bar(df, x='Districts', y='Transaction_amount',
                            color='Districts', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Transaction_amount')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df3)
            st.plotly_chart(df_fig)
            

        elif options == "4.PhonePe users from 2018 to 2023":

            year_1 = list(range(2018, 2024))  
    
            st_year = st.selectbox("Select the Start Year", get_year(), index=0, placeholder="Select the Start Year...")
            end_year = st.selectbox("Select the End Year", get_year(), index=len(year_1)-1, placeholder="Select the End Year...")
            
            df4 = users_count(st_year,end_year)
            df = df4
            df['Year'] = df['Year'].astype(str)
            df['Registered_users'] = df['Registered_users'].astype(float)
            df_fig = px.bar(df, x='Year', y='Registered_users',
                            color='Year', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Registered_users')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df4)
            st.plotly_chart(df_fig)
            

        elif options == "5.Top 10 States with Highest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df5 = high_users_count(selected_year,selected_quarter)
            df = df5
            df['State'] = df['State'].astype(str)
            df['Registered_users'] = df['Registered_users'].astype(float)
            df_fig = px.bar(df, x='State', y='Registered_users',
                            color='State', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Registered_users')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df5)
            st.plotly_chart(df_fig)
            

        elif options == "6.Top 10 States with Lowest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df6 = low_users_count(selected_year,selected_quarter)
            df = df6
            df['State'] = df['State'].astype(str)
            df['Registered_users'] = df['Registered_users'].astype(float)
            df_fig = px.bar(df, x='State', y='Registered_users',
                            color='State', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Registered_users')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df6)
            st.plotly_chart(df_fig)
            

        elif options == "7.Top 10 Districts with Highest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df7 = high_users_dist_count(selected_year,selected_quarter)
            df = df7
            df['Districts'] = df['Districts'].astype(str)
            df['Registered_users'] = df['Registered_users'].astype(float)
            df_fig = px.bar(df, x='Districts', y='Registered_users',
                            color='Districts', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Registered_users')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df7)
            st.plotly_chart(df_fig)
            

        elif options == "8.Top 10 Districts with Lowest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df8 = low_users_dist_count(selected_year,selected_quarter)
            df = df8
            df['Districts'] = df['Districts'].astype(str)
            df['Registered_users'] = df['Registered_users'].astype(float)
            df_fig = px.bar(df, x='Districts', y='Registered_users',
                            color='Districts', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Registered_users')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df8)
            st.plotly_chart(df_fig)
            

        elif options == "9.Top 10 District with Highest Transaction Count":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df9 = high_map_transaction(selected_year,selected_quarter)
            df = df9
            df['Districts'] = df['Districts'].astype(str)
            df['Transaction_count'] = df['Transaction_count'].astype(float)
            df_fig = px.bar(df, x='Districts', y='Transaction_count',
                            color='Districts', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Transaction_count')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df9)
            st.plotly_chart(df_fig)
            

        elif options == "10.Top 10 District With Lowest Transaction Count":

            selected_year = st.selectbox("Select the Year",get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            df10 = low_map_transaction(selected_year,selected_quarter)
            df = df10
            df['Districts'] = df['Districts'].astype(str)
            df['Transaction_count'] = df['Transaction_count'].astype(float)
            df_fig = px.bar(df, x='Districts', y='Transaction_count',
                            color='Districts', color_continuous_scale='thermal',
                            title=f'Top Analysis Chart', height=700,
                            text = 'Transaction_count')
            df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')
            st.write(df10)
            st.plotly_chart(df_fig)
            


