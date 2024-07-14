import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import mysql
import mysql.connector
import plotly.express as px
import numpy as np
import requests
import json
import plotly.graph_objects as go



st.set_page_config(layout="wide")

st.title(":violet[PHONEPE DATA] :rainbow[EXPLORATION AND VISUALIZATION]")

connection = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="phonepe_data")

mycursor = connection.cursor()

# Aggregated transaction Dataframe

def agg_trans_frame():

    query = "select * from aggregate_transaction"
    mycursor.execute(query)
    result1 = mycursor.fetchall()

    Aggregated_transaction = pd.DataFrame(result1,columns=("STATE","YEAR","QUARTER","TRANSACTION_TYPE","TRANSACTION_COUNT","TRANSACTION_AMOUNT"))

    return Aggregated_transaction

# Aggregated user Dataframe

def agg_user_frame():

    query = "select * from aggregate_user"
    mycursor.execute(query)
    result2 = mycursor.fetchall()

    Aggregated_user = pd.DataFrame(result2,columns=("STATE","YEAR","QUARTER","BRANDS","COUNT","PERCENTAGE"))

    return Aggregated_user

# Map transaction Dataframe

def map_trans_frame():

    query = "select * from map_transaction"
    mycursor.execute(query)
    result6 = mycursor.fetchall()

    Map_transaction = pd.DataFrame(result6,columns=("STATE","YEAR","QUARTER","DISTRICTS","TRANSACTION_COUNT","TRANSACTION_AMOUNT"))

    return Map_transaction

# Map User Dataframe

def map_user_frame():

    query = "select * from map_user"
    mycursor.execute(query)
    result3 = mycursor.fetchall()

    Map_user = pd.DataFrame(result3,columns=("STATE","YEAR","QUARTER","DISTRICTS","REGISTERED_USERS","APP_OPENS"))

    return Map_user

# Top_transaction Dataframe

def top_trans_frame():

    query = "select * from top_transaction"
    mycursor.execute(query)
    result4 = mycursor.fetchall()

    Top_transaction = pd.DataFrame(result4,columns=("STATE","YEAR","QUARTER","DISTRICTS","TRANSACTION_COUNT","TRANSACTION_AMOUNT"))

    return Top_transaction

# Top user Dataframe

def top_user_frame():
    
    query = "select * from top_user"
    mycursor.execute(query)
    result5 = mycursor.fetchall()

    Top_user = pd.DataFrame(result5,columns=("STATE","YEAR","DISTRICTS","DISTRICTS","REGISTERED_USERS"))

    return Top_user


def agg_trans_one(STATE,YEAR,QUARTER):
        
        if STATE == "All States":

            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            
            query ='''select STATE, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT 
                    from aggregate_transaction where  YEAR = %s and QUARTER = %s group by STATE order by TRANSACTION_AMOUNT desc'''

            mycursor.execute(query,(YEAR,QUARTER))
            result1 = mycursor.fetchall()
            agg_sum_count = pd.DataFrame(result1,columns=("STATE","TRANSACTION_AMOUNT","TRANSACTION_COUNT"))
            df = agg_sum_count
            fig = px.choropleth(
            df,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations='STATE',
            color='TRANSACTION_AMOUNT',
            color_continuous_scale='purples',
            title='Transaction Data Analysis'
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800,width=800)

            df['STATE'] = df['STATE'].astype(str)
            df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
            df_fig = px.bar(df, x='STATE', y='TRANSACTION_AMOUNT',
                            color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                            title=f'Transaction Analysis Chart:{STATE}', height=700,
                            text='TRANSACTION_AMOUNT')
            df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')

            return agg_sum_count,fig,df_fig
        

        
        else:
    
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            
            query ='''select STATE, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT 
                    from aggregate_transaction where STATE = %s and  YEAR = %s and QUARTER = %s
                    group by STATE order by TRANSACTION_COUNT desc'''

            mycursor.execute(query,(STATE,YEAR,QUARTER))
            result1 = mycursor.fetchall()
            agg_sum_count = pd.DataFrame(result1,columns=("STATE","TRANSACTION_AMOUNT","TRANSACTION_COUNT"))
            df = agg_sum_count
            fig = px.choropleth(
            df,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations='STATE',
            color='TRANSACTION_AMOUNT',
            color_continuous_scale='purples',
            title='Transaction Data Analysis'
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800,width=800)

            df['STATE'] = df['STATE'].astype(str)
            df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
            df_fig = px.bar(df, x='STATE', y='TRANSACTION_AMOUNT',
                            color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                            title=f'Transaction Analysis Chart:{STATE}', height=700,
                            text = 'TRANSACTION_AMOUNT')
            df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            df_fig.update_traces(textposition='outside')

            return agg_sum_count,fig,df_fig
        
def agg_trans_two(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,sum(TRANSACTION_AMOUNT) 
                    as TRANSACTION_AMOUNT from aggregate_transaction where  YEAR = %s and QUARTER = %s 
                    group by STATE,TRANSACTION_TYPE order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result2 = mycursor.fetchall()
        agg_sum_type = pd.DataFrame(result2,columns=("STATES","TRANSACTION_TYPE","TRANSACTION_AMOUNT"))
        df = agg_sum_type
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='TRANSACTION_AMOUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart(Amt in Millions):{STATE}', height=700,
                        text='TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')  
    
        return agg_sum_type,df_fig
    
    else:

        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,sum(TRANSACTION_AMOUNT)
                   as TRANSACTION_AMOUNT from aggregate_transaction where STATE = %s and  YEAR = %s and 
                   QUARTER = %s group by STATE,TRANSACTION_TYPE order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result2 = mycursor.fetchall()
        agg_sum_type = pd.DataFrame(result2,columns=("STATE","TRANSACTION_TYPE","TRANSACTION_AMOUNT"))
        df = agg_sum_type
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='TRANSACTION_AMOUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart(Amt in Millions):{STATE}', height=700,
                        text='TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')  
    
        return agg_sum_type,df_fig

def agg_trans_three(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,sum(TRANSACTION_COUNT) as TRANSACTION_COUNT 
                   from aggregate_transaction where  YEAR = %s and QUARTER = %s group by 
                   STATE,TRANSACTION_TYPE order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result3 = mycursor.fetchall()
        agg_sum_type_count = pd.DataFrame(result3,columns=("STATES","TRANSACTION_TYPE","TRANSACTION_COUNT"))
        df = agg_sum_type_count
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='TRANSACTION_COUNT',
                        color='TRANSACTION_COUNT', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_COUNT')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
    
        return agg_sum_type_count,df_fig
    else:
        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,sum(TRANSACTION_COUNT) as 
                TRANSACTION_COUNT from aggregate_transaction where STATE = %s and YEAR = %s and QUARTER = %s group by 
                STATE,TRANSACTION_TYPE order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result3 = mycursor.fetchall()
        agg_sum_type_count = pd.DataFrame(result3,columns=("STATES","TRANSACTION_TYPE","TRANSACTION_COUNT"))
        df = agg_sum_type_count
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='TRANSACTION_COUNT',
                        color='TRANSACTION_COUNT', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_COUNT')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
    
        return agg_sum_type_count,df_fig

def agg_trans_four(STATE,YEAR,QUARTER):
    
    if STATE == "All States":

        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,avg(TRANSACTION_AMOUNT) as Average_Transaction_amount 
                  from aggregate_transaction where YEAR = %s and QUARTER = %s group by 
                  STATE,TRANSACTION_TYPE order by Average_Transaction_amount desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result4 = mycursor.fetchall()
        agg_trans_amt_avg = pd.DataFrame(result4,columns=("STATES","TRANSACTION_TYPE","Average_Transaction_amount"))
        df = agg_trans_amt_avg
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['Average_Transaction_amount'] = df['Average_Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='Average_Transaction_amount',
                        color='Average_Transaction_amount', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{STATE}', height=700,
                        text = 'Average_Transaction_amount')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_trans_amt_avg,df_fig
    else:
        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,avg(TRANSACTION_AMOUNT) as Average_Transaction_amount
                 from aggregate_transaction where state = %s and year = %s and quarter = %s group by 
                 STATE,TRANSACTION_TYPE order by Average_Transaction_amount desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result4 = mycursor.fetchall()
        agg_trans_amt_avg = pd.DataFrame(result4,columns=("STATES","TRANSACTION_TYPE","Average_Transaction_amount"))
        df = agg_trans_amt_avg
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['Average_Transaction_amount'] = df['Average_Transaction_amount'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='Average_Transaction_amount',
                        color='Average_Transaction_amount', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{STATE}', height=700,
                        text = 'Average_Transaction_amount')
        df_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_trans_amt_avg,df_fig

def agg_trans_five(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,avg(TRANSACTION_COUNT) as Average_Transaction_count 
                 from aggregate_transaction where  YEAR = %s and QUARTER = %s group by 
                 STATE,TRANSACTION_TYPE order by Average_Transaction_count desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result5 = mycursor.fetchall()
        agg_avg_trans_count = pd.DataFrame(result5,columns=("STATES","TRANSACTION_TYPE","Average_Transaction_count"))
        df = agg_avg_trans_count
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['Average_Transaction_count'] = df['Average_Transaction_count'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='Average_Transaction_count',
                        color='Average_Transaction_count', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{STATE}', height=700,
                        text = 'Average_Transaction_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_avg_trans_count,df_fig
    
    else:

        query = '''select STATE as STATES,TRANSACTION_TYPE as TRANSACTION_TYPE,avg(TRANSACTION_COUNT) as Average_Transaction_count 
                 from aggregate_transaction where  STATE = %s and YEAR = %s and QUARTER = %s group by 
                 STATE,TRANSACTION_TYPE order by Average_Transaction_count desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result5 = mycursor.fetchall()
        agg_avg_trans_count = pd.DataFrame(result5,columns=("STATES","TRANSACTION_TYPE","Average_Transaction_count"))
        df = agg_avg_trans_count
        df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].astype(str)
        df['Average_Transaction_count'] = df['Average_Transaction_count'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_TYPE', y='Average_Transaction_count',
                        color='Average_Transaction_count', color_continuous_scale='thermal',
                        title=f'Transaction Analysis Chart:{STATE}', height=700,
                        text = 'Average_Transaction_count')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        
        return agg_avg_trans_count,df_fig

def agg_user_one(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, BRANDS, sum(COUNT) as TOTAL_COUNT from Aggregate_user where  YEAR = %s and QUARTER = %s
                group by STATE,BRANDS order by TOTAL_COUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result9 = mycursor.fetchall()
        agg_user = pd.DataFrame(result9,columns=("STATE","BRANDS","TOTAL_COUNT"))
        df = agg_user
        df['BRANDS'] = df['BRANDS'].astype(str)
        df['TOTAL_COUNT'] = df['TOTAL_COUNT'].astype(float)
        df_fig = px.bar(df, x='BRANDS', y='TOTAL_COUNT',
                        color='TOTAL_COUNT', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_COUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')

        return agg_user,df_fig
    
    else:

        query ='''select STATE, BRANDS, sum(COUNT) as TOTAL_COUNT from Aggregate_user where STATE = %s and  YEAR = %s and QUARTER = %s
                group by STATE,BRANDS order by TOTAL_COUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result9 = mycursor.fetchall()
        agg_user = pd.DataFrame(result9,columns=("STATE","BRANDS","TOTAL_COUNT"))
        df = agg_user
        df['BRANDS'] = df['BRANDS'].astype(str)
        df['TOTAL_COUNT'] = df['TOTAL_COUNT'].astype(float)
        df_fig = px.bar(df, x='BRANDS', y='TOTAL_COUNT',
                        color='TOTAL_COUNT', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_COUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')

        return agg_user,df_fig

def agg_user_two(STATE,YEAR,QUARTER):

     if STATE == "All States":

        query ='''select STATE, BRANDS, sum(PERCENTAGE) as TOTAL_PERCENTAGE from Aggregate_user where YEAR = %s and QUARTER = %s
                group by STATE,BRANDS order by TOTAL_PERCENTAGE  desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result10 = mycursor.fetchall()
        agg_user_percent = pd.DataFrame(result10,columns=("STATE","BRANDS","TOTAL_PERCENTAGE"))
        df = agg_user_percent
        df['BRANDS'] = df['BRANDS'].astype(str)
        df['TOTAL_PERCENTAGE'] = df['TOTAL_PERCENTAGE'].astype(float)
        df_fig = px.bar(df, x='BRANDS', y='TOTAL_PERCENTAGE',
                        color='TOTAL_PERCENTAGE', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_PERCENTAGE')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return agg_user_percent,df_fig
     
     else:
         
        query ='''select STATE, BRANDS, sum(PERCENTAGE) as TOTAL_PERCENTAGE from Aggregate_user where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE,BRANDS order by TOTAL_PERCENTAGE desc '''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result10 = mycursor.fetchall()
        agg_user_percent = pd.DataFrame(result10,columns=("STATE","BRANDS","TOTAL_PERCENTAGE"))
        df = agg_user_percent
        df['BRANDS'] = df['BRANDS'].astype(str)
        df['TOTAL_PERCENTAGE'] = df['TOTAL_PERCENTAGE'].astype(float)
        df_fig = px.bar(df, x='BRANDS', y='TOTAL_PERCENTAGE',
                        color='TOTAL_PERCENTAGE', color_continuous_scale='thermal',
                        title=f'Transaction User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_PERCENTAGE')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return agg_user_percent,df_fig

def map_trans(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT 
                from Map_transaction where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result6 = mycursor.fetchall()
        map_sum_count = pd.DataFrame(result6,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT","TRANSACTION_COUNT"))
        df = map_sum_count
        df['STATE'] = df['STATE'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='STATE', y='TRANSACTION_AMOUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count,df_fig
    
    else:

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT 
                from Map_transaction where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result6 = mycursor.fetchall()
        map_sum_count = pd.DataFrame(result6,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT","TRANSACTION_COUNT"))
        df = map_sum_count
        df['STATE'] = df['STATE'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='STATE', y='TRANSACTION_AMOUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count,df_fig

def map_trans_amount(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT 
                  from Map_transaction where YEAR = %s and QUARTER = %s
                  group by STATE, DISTRICTS order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result7 = mycursor.fetchall()
        map_sum_amount_alone = pd.DataFrame(result7,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT"))
        df = map_sum_amount_alone
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_AMOUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_amount_alone,df_fig
    
    else:

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT 
                  from Map_transaction where STATE = %s and YEAR = %s and QUARTER = %s
                  group by STATE, DISTRICTS order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result7 = mycursor.fetchall()
        map_sum_amount_alone = pd.DataFrame(result7,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT"))
        df = map_sum_amount_alone
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_AMOUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_amount_alone,df_fig

def map_trans_count(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT from Map_transaction 
                  where YEAR = %s and QUARTER = %s 
                group by STATE, DISTRICTS order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result8 = mycursor.fetchall()
        map_sum_count_alone = pd.DataFrame(result8,columns=("STATE","DISTRICTS","TRANSACTION_COUNT"))
        df = map_sum_count_alone
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_COUNT',
                        color='TRANSACTION_COUNT', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_COUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count_alone,df_fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT from Map_transaction 
                  where STATE = %s and YEAR = %s and QUARTER = %s 
                group by STATE, DISTRICTS order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result8 = mycursor.fetchall()
        map_sum_count_alone = pd.DataFrame(result8,columns=("STATE","DISTRICTS","TRANSACTION_COUNT"))
        df = map_sum_count_alone
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_COUNT',
                        color='TRANSACTION_COUNT', color_continuous_scale='thermal',
                        title=f'Map Transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_COUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_sum_count_alone,df_fig
        
def get_map_user(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(REGISTERED_USERS) as TOTAL_REGISTERED_USERS, sum(APP_OPENS) as TOTAL_APP_OPENS 
                from Map_user where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_REGISTERED_USERS desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result11 = mycursor.fetchall()
        map_user = pd.DataFrame(result11,columns=("STATE","DISTRICTS","TOTAL_REGISTERED_USERS","TOTAL_APP_OPENS"))
        df = map_user
        df['TOTAL_APP_OPENS'] = df['TOTAL_APP_OPENS'].astype(str)
        df['TOTAL_REGISTERED_USERS'] = df['TOTAL_REGISTERED_USERS'].astype(float)
        df_fig = px.bar(df, x='TOTAL_APP_OPENS', y='TOTAL_REGISTERED_USERS',
                        color='TOTAL_APP_OPENS', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_REGISTERED_USERS')
        df_fig.update_xaxes(tickformat=',d')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_user,df_fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(REGISTERED_USERS) as TOTAL_REGISTERED_USERS, sum(APP_OPENS) as TOTAL_APP_OPENS 
                from Map_user where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_REGISTERED_USERS desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result11 = mycursor.fetchall()
        map_user = pd.DataFrame(result11,columns=("STATE","DISTRICTS","TOTAL_REGISTERED_USERS","TOTAL_APP_OPENS"))
        df = map_user
        df['TOTAL_APP_OPENS'] = df['TOTAL_APP_OPENS'].astype(str)
        df['TOTAL_REGISTERED_USERS'] = df['TOTAL_REGISTERED_USERS'].astype(float)
        df_fig = px.bar(df, x='TOTAL_APP_OPENS', y='TOTAL_REGISTERED_USERS',
                        color='TOTAL_APP_OPENS', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_REGISTERED_USERS')
        df_fig.update_xaxes(tickformat=',d')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_user,df_fig
        
def get_map_reg_user(STATE,YEAR,QUARTER):

    if STATE == "All States":
    
        query ='''select STATE, DISTRICTS, sum(REGISTERED_USERS) as TOTAL_REGISTERED_USERS from Map_user where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_REGISTERED_USERS desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result12 = mycursor.fetchall()
        map_reg_user = pd.DataFrame(result12,columns=("STATE","DISTRICTS","TOTAL_REGISTERED_USERS"))
        df = map_reg_user
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TOTAL_REGISTERED_USERS'] = df['TOTAL_REGISTERED_USERS'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TOTAL_REGISTERED_USERS',
                        color='TOTAL_REGISTERED_USERS', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_REGISTERED_USERS')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_reg_user,df_fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(REGISTERED_USERS) as TOTAL_REGISTERED_USERS from Map_user where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_REGISTERED_USERS desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result12 = mycursor.fetchall()
        map_reg_user = pd.DataFrame(result12,columns=("STATE","DISTRICTS","TOTAL_REGISTERED_USERS"))
        df = map_reg_user
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TOTAL_REGISTERED_USERS'] = df['TOTAL_REGISTERED_USERS'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TOTAL_REGISTERED_USERS',
                        color='TOTAL_REGISTERED_USERS', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_REGISTERED_USERS')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_reg_user,df_fig
        
def get_map_appOpens(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(APP_OPENS) as TOTAL_APP_OPENS from Map_user where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_APP_OPENS desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result13 = mycursor.fetchall()
        map_app_opens = pd.DataFrame(result13,columns=("STATE","DISTRICTS","TOTAL_APP_OPENS"))
        df = map_app_opens
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TOTAL_APP_OPENS'] = df['TOTAL_APP_OPENS'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TOTAL_APP_OPENS',
                        color='TOTAL_APP_OPENS', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_APP_OPENS')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_app_opens,df_fig
       
    
    else:

        query ='''select STATE, DISTRICTS, sum(APP_OPENS) as TOTAL_APP_OPENS from Map_user where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_APP_OPENS desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result13 = mycursor.fetchall()
        map_app_opens = pd.DataFrame(result13,columns=("STATE","DISTRICTS","TOTAL_APP_OPENS"))
        df = map_app_opens
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TOTAL_APP_OPENS'] = df['TOTAL_APP_OPENS'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TOTAL_APP_OPENS',
                        color='TOTAL_APP_OPENS', color_continuous_scale='thermal',
                        title=f'Map User Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_APP_OPENS')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return map_app_opens,df_fig
       
def get_top_trans(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT
                 from top_transaction where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result14 = mycursor.fetchall()
        top_sum = pd.DataFrame(result14,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT","TRANSACTION_COUNT"))
        df = top_sum
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_AMOUNT', y='TRANSACTION_COUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum,df_fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT
                 from top_transaction where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result14 = mycursor.fetchall()
        top_sum = pd.DataFrame(result14,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT","TRANSACTION_COUNT"))
        df = top_sum
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='TRANSACTION_AMOUNT', y='TRANSACTION_COUNT',
                        color='TRANSACTION_AMOUNT', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum,df_fig
        
def get_top_trans_amount(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT from top_transaction 
                 where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result15 = mycursor.fetchall()
        top_sum_amount = pd.DataFrame(result15,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT"))
        df = top_sum_amount
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_AMOUNT',
                        color='DISTRICTS', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum_amount,df_fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT from top_transaction 
                 where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_AMOUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result15 = mycursor.fetchall()
        top_sum_amount = pd.DataFrame(result15,columns=("STATE","DISTRICTS","TRANSACTION_AMOUNT"))
        df = top_sum_amount
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_AMOUNT',
                        color='DISTRICTS', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_AMOUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_sum_amount,df_fig  

def get_top_trans_count(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT from top_transaction 
                where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result16 = mycursor.fetchall()
        top_sum_count = pd.DataFrame(result16,columns=("STATE","DISTRICTS","TRANSACTION_COUNT"))
        df = top_sum_count
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_COUNT',
                        color='DISTRICTS', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_COUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        fig = px.scatter(df,x='DISTRICTS', y='TRANSACTION_COUNT',color="TRANSACTION_COUNT")
        fig.update_layout(xaxis_tickangle=-45)
        return top_sum_count,df_fig,fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(TRANSACTION_COUNT) as TRANSACTION_COUNT from top_transaction 
                where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TRANSACTION_COUNT desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result16 = mycursor.fetchall()
        top_sum_count = pd.DataFrame(result16,columns=("STATE","DISTRICTS","TRANSACTION_COUNT"))
        df = top_sum_count
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_COUNT',
                        color='DISTRICTS', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TRANSACTION_COUNT')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        fig = px.scatter(df,x='DISTRICTS', y='TRANSACTION_COUNT',color="TRANSACTION_COUNT")
        fig.update_layout(xaxis_tickangle=-45)

        return top_sum_count,df_fig,fig

def get_top_user(STATE,YEAR,QUARTER):

    if STATE == "All States":

        query ='''select STATE, DISTRICTS, sum(REGISTERED_USERS) as TOTAL_REGISTERED_USERS from top_user 
                where YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_REGISTERED_USERS desc'''

        mycursor.execute(query,(YEAR,QUARTER))
        result17 = mycursor.fetchall()
        top_reg_user = pd.DataFrame(result17,columns=("STATE","DISTRICTS","TOTAL_REGISTERED_USERS"))
        df = top_reg_user
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TOTAL_REGISTERED_USERS'] = df['TOTAL_REGISTERED_USERS'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TOTAL_REGISTERED_USERS',
                        color='TOTAL_REGISTERED_USERS', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_REGISTERED_USERS')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_reg_user,df_fig
        
    
    else:

        query ='''select STATE, DISTRICTS, sum(REGISTERED_USERS) as TOTAL_REGISTERED_USERS from top_user 
                where STATE = %s and YEAR = %s and QUARTER = %s
                group by STATE, DISTRICTS order by TOTAL_REGISTERED_USERS desc'''

        mycursor.execute(query,(STATE,YEAR,QUARTER))
        result17 = mycursor.fetchall()
        top_reg_user = pd.DataFrame(result17,columns=("STATE","DISTRICTS","TOTAL_REGISTERED_USERS"))
        df = top_reg_user
        df['DISTRICTS'] = df['DISTRICTS'].astype(str)
        df['TOTAL_REGISTERED_USERS'] = df['TOTAL_REGISTERED_USERS'].astype(float)
        df_fig = px.bar(df, x='DISTRICTS', y='TOTAL_REGISTERED_USERS',
                        color='TOTAL_REGISTERED_USERS', color_continuous_scale='thermal',
                        title=f'Top transaction Analysis Chart:{STATE}', height=700,
                        text = 'TOTAL_REGISTERED_USERS')
        df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
        df_fig.update_traces(textposition='outside')
        return top_reg_user,df_fig
        
# Top Charts

#Question 1 "Top brands of mobile used"

def top_mobile_brand(YEAR,QUARTER):

    query ='''select BRANDS, sum(COUNT) as BRAND_COUNT from Aggregate_user where YEAR = %s and QUARTER = %s
            group by BRANDS order by BRAND_COUNT desc'''

    mycursor.execute(query,(YEAR,QUARTER))
    result18 = mycursor.fetchall()
    top_brand = pd.DataFrame(result18,columns=("BRANDS","BRAND_COUNT"))
    return top_brand

# Question 2 Top 10 District With Lowest Transaction Amount"

def low_transaction(YEAR,QUARTER):

    query ='''select DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT from Map_transaction where YEAR = %s and QUARTER = %s
            group by DISTRICTS order by TRANSACTION_AMOUNT asc limit 10'''

    mycursor.execute(query,(YEAR,QUARTER))
    result19 = mycursor.fetchall()
    top_brand = pd.DataFrame(result19,columns=("DISTRICTS","TRANSACTION_AMOUNT"))
    return top_brand

# Question 3 Top 10 District With Highest Transaction Amount"

def high_transaction(YEAR,QUARTER):

    query ='''select DISTRICTS, sum(TRANSACTION_AMOUNT) as TRANSACTION_AMOUNT from Map_transaction where YEAR = %s and QUARTER = %s
            group by DISTRICTS order by TRANSACTION_AMOUNT desc limit 10'''

    mycursor.execute(query,(YEAR,QUARTER))
    result20 = mycursor.fetchall()
    high_trans = pd.DataFrame(result20,columns=("DISTRICTS","TRANSACTION_AMOUNT"))
    return high_trans

# Question 4 "PhonePe users from 2018 to 2023"

def users_count(ST_YEAR,END_YEAR):

    query ='''select YEAR, sum(REGISTERED_USERS) as REGISTERED_USERS from Map_user where YEAR between  %s and  %s
            group by YEAR order by YEAR '''

    mycursor.execute(query,(ST_YEAR,END_YEAR))
    result21 = mycursor.fetchall()
    users_sum = pd.DataFrame(result21,columns=("YEAR","REGISTERED_USERS"))
    return users_sum

# Question 5 Top 10 Districts with Highest PhonePe User"

def high_users_count(YEAR,QUARTER):

    query ='''select STATE, max(REGISTERED_USERS) as REGISTERED_USERS from Map_user where YEAR = %s and QUARTER = %s
            group by STATE order by REGISTERED_USERS desc limit 100'''

    mycursor.execute(query,(YEAR,QUARTER))
    result22 = mycursor.fetchall()
    high_users = pd.DataFrame(result22,columns=("STATE","REGISTERED_USERS"))
    return high_users

# Question 6 "Top 10 States with Lowest PhonePe User"

def low_users_count(YEAR,QUARTER):

    query ='''select STATE, min(REGISTERED_USERS) as REGISTERED_USERS from Map_user where YEAR = %s and QUARTER = %s
            group by STATE order by REGISTERED_USERS asc limit 100'''

    mycursor.execute(query,(YEAR,QUARTER))
    result23 = mycursor.fetchall()
    low_users = pd.DataFrame(result23,columns=("STATE","REGISTERED_USERS"))
    return low_users

# Question 7 "Top 10 Districts with Highest PhonePe User"

def high_users_dist_count(YEAR,QUARTER):

    query ='''select DISTRICTS, max(REGISTERED_USERS) as REGISTERED_USERS from Map_user where YEAR = %s and QUARTER = %s
            group by DISTRICTS order by REGISTERED_USERS desc limit 100'''

    mycursor.execute(query,(YEAR,QUARTER))
    result24 = mycursor.fetchall()
    low_users = pd.DataFrame(result24,columns=("DISTRICTS","REGISTERED_USERS"))
    return low_users

# Question 8 "Top 10 Districts with lowest PhonePe User"

def low_users_dist_count(YEAR,QUARTER):

    query ='''select DISTRICTS, min(REGISTERED_USERS) as REGISTERED_USERS from Map_user where YEAR = %s and QUARTER = %s
            group by DISTRICTS order by REGISTERED_USERS asc limit 100'''

    mycursor.execute(query,(YEAR,QUARTER))
    result25 = mycursor.fetchall()
    low_users = pd.DataFrame(result25,columns=("DISTRICTS","REGISTERED_USERS"))
    return low_users

# Question 9 "Top 10 District with Highest Transaction Count"

def high_map_transaction(YEAR,QUARTER):

    query ='''select DISTRICTS, max(TRANSACTION_COUNT) as TRANSACTION_COUNT from Map_transaction where YEAR = %s and QUARTER = %s
            group by DISTRICTS order by TRANSACTION_COUNT desc limit 100'''

    mycursor.execute(query,(YEAR,QUARTER))
    result26 = mycursor.fetchall()
    high_trans = pd.DataFrame(result26,columns=("DISTRICTS","TRANSACTION_COUNT"))
    return high_trans

# Question 10 "Top 10 District with lowest Transaction Count"

def low_map_transaction(YEAR,QUARTER):

    query ='''select DISTRICTS, min(TRANSACTION_COUNT) as TRANSACTION_COUNT from Map_transaction where YEAR = %s and QUARTER = %s
            group by DISTRICTS order by TRANSACTION_COUNT asc '''

    mycursor.execute(query,(YEAR,QUARTER))
    result27 = mycursor.fetchall()
    high_trans = pd.DataFrame(result27,columns=("DISTRICTS","TRANSACTION_COUNT"))
    return high_trans

def get_year():

    year = np.array (agg_trans_frame()["YEAR"].unique()).tolist()

    year_1 = tuple([str(d) for d in year])

    return year_1

def get_quarter():

    quarter = np.array(agg_trans_frame()["QUARTER"].unique()).tolist()

    quarter_1 = tuple([str(d) for d in quarter])

    return quarter_1

def get_state():

    state = np.array (agg_trans_frame()["STATE"].unique()).tolist()

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

    map_trans_type = ["None","State vs Transaction_amount","Districts vs Transaction_amount","Districts vs Transaction_count"]

    map_type = tuple([str(k) for k in map_trans_type])

    return map_type

def get_map_user_data():

    map_user_type = ["None","Districts vs Total_Registered_users","Districts vs Total_AppOpens"]

    map_user_data = tuple([str(l) for l in map_user_type])

    return map_user_data

def get_top_trans_data():

    top_trans_type = ["None","Districts vs Transaction_amount","Districts vs Transaction_count"]

    top_trans_data = tuple([str(m) for m in top_trans_type])

    return top_trans_data

def get_top_user_data():

    top_user_type = ["None","Districts vs Total Registered_users"]

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

                if select_type == "State vs Transaction_amount":
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
                          
                if select_type == "Districts vs Transaction_amount":
                   top_trans_2,df_fig = get_top_trans_amount(selected_state,selected_year,selected_quarter)
                   st.dataframe(top_trans_2)
                   st.plotly_chart(df_fig)
                    

                elif select_type == "Districts vs Transaction_count":
                    top_trans_3,df_fig,fig= get_top_trans_count(selected_state,selected_year,selected_quarter)
                    st.dataframe(top_trans_3)
                    st.plotly_chart(df_fig)
                    st.plotly_chart(fig)

        if option2 == "User Data Analysis":

            col1,col2 = st.columns(2)

            with col1:

                select_type = st.selectbox("Select the Type",get_top_user_data(), index=0, placeholder="Select the Type...")
                selected_year = st.selectbox("Select the year", get_year(), index=0, placeholder="Select the Year...")
            with col2:
                selected_quarter = st.selectbox("Select the quarter", get_quarter(), index=0, placeholder="Select the Quarter...")
                selected_state = st.selectbox("Select the state",get_state(), index=0, placeholder="Select the State...")

            if st.button(":violet[**SUBMIT**]"):
                
                if select_type == "Districts vs Total Registered_users":
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

            if st.button(":violet[**Get Data**]",use_container_width=True):
        
                df1 = top_mobile_brand(selected_year,selected_quarter)
                df = df1
                df['BRANDS'] = df['BRANDS'].astype(str)
                df['BRAND_COUNT'] = df['BRAND_COUNT'].astype(float)
                df_fig = px.bar(df, x='BRANDS', y='BRAND_COUNT',
                                color='BRANDS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'BRAND_COUNT')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df1)
                st.plotly_chart(df_fig)

        elif options == "2.Top 10 District With Lowest Transaction Amount":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df2 = low_transaction(selected_year,selected_quarter)
                df = df2
                df['DISTRICTS'] = df['DISTRICTS'].astype(str)
                df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
                df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_AMOUNT',
                                color='DISTRICTS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'TRANSACTION_AMOUNT')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df2)
                st.plotly_chart(df_fig)

        elif options == "3.Top 10 District With Highest Transaction Amount":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df3 = high_transaction(selected_year,selected_quarter)
                df = df3
                df['DISTRICTS'] = df['DISTRICTS'].astype(str)
                df['TRANSACTION_AMOUNT'] = df['TRANSACTION_AMOUNT'].astype(float)
                df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_AMOUNT',
                                color='DISTRICTS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'TRANSACTION_AMOUNT')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df3)
                st.plotly_chart(df_fig)
            

        elif options == "4.PhonePe users from 2018 to 2023":

            year_1 = list(range(2018, 2024))  
    
            st_year = st.selectbox("Select the Start Year", get_year(), index=0, placeholder="Select the Start Year...")
            end_year = st.selectbox("Select the End Year", get_year(), index=len(year_1)-1, placeholder="Select the End Year...")

            if st.button(":violet[**Get Data**]",use_container_width=True):
            
                df4 = users_count(st_year,end_year)
                df = df4
                df['YEAR'] = df['YEAR'].astype(str)
                df['REGISTERED_USERS'] = df['REGISTERED_USERS'].astype(float)
                df_fig = px.bar(df, x='YEAR', y='REGISTERED_USERS',
                                color='YEAR', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'REGISTERED_USERS')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df4)
                st.plotly_chart(df_fig)
            

        elif options == "5.Top 10 States with Highest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df5 = high_users_count(selected_year,selected_quarter)
                df = df5
                df['STATE'] = df['STATE'].astype(str)
                df['REGISTERED_USERS'] = df['REGISTERED_USERS'].astype(float)
                df_fig = px.bar(df, x='STATE', y='REGISTERED_USERS',
                                color='STATE', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'REGISTERED_USERS')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df5)
                st.plotly_chart(df_fig)
            

        elif options == "6.Top 10 States with Lowest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df6 = low_users_count(selected_year,selected_quarter)
                df = df6
                df['STATE'] = df['STATE'].astype(str)
                df['REGISTERED_USERS'] = df['REGISTERED_USERS'].astype(float)
                df_fig = px.bar(df, x='STATE', y='REGISTERED_USERS',
                                color='STATE', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'REGISTERED_USERS')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df6)
                st.plotly_chart(df_fig)
            

        elif options == "7.Top 10 Districts with Highest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df7 = high_users_dist_count(selected_year,selected_quarter)
                df = df7
                df['DISTRICTS'] = df['DISTRICTS'].astype(str)
                df['REGISTERED_USERS'] = df['REGISTERED_USERS'].astype(float)
                df_fig = px.bar(df, x='DISTRICTS', y='REGISTERED_USERS',
                                color='DISTRICTS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'REGISTERED_USERS')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df7)
                st.plotly_chart(df_fig)
            

        elif options == "8.Top 10 Districts with Lowest PhonePe User":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df8 = low_users_dist_count(selected_year,selected_quarter)
                df = df8
                df['DISTRICTS'] = df['DISTRICTS'].astype(str)
                df['REGISTERED_USERS'] = df['REGISTERED_USERS'].astype(float)
                df_fig = px.bar(df, x='DISTRICTS', y='REGISTERED_USERS',
                                color='DISTRICTS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'REGISTERED_USERS')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df8)
                st.plotly_chart(df_fig)
            

        elif options == "9.Top 10 District with Highest Transaction Count":

            selected_year = st.selectbox("Select the Year", get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df9 = high_map_transaction(selected_year,selected_quarter)
                df = df9
                df['DISTRICTS'] = df['DISTRICTS'].astype(str)
                df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
                df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_COUNT',
                                color='DISTRICTS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'TRANSACTION_COUNT')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df9)
                st.plotly_chart(df_fig)
                

        elif options == "10.Top 10 District With Lowest Transaction Count":

            selected_year = st.selectbox("Select the Year",get_year(), index=0, placeholder="Select the Year...")
            selected_quarter = st.selectbox("Select the Quarter", get_quarter(), index=0, placeholder="Select the Quarter...")

            if st.button(":violet[**Get Data**]",use_container_width=True):

                df10 = low_map_transaction(selected_year,selected_quarter)
                df = df10
                df['DISTRICTS'] = df['DISTRICTS'].astype(str)
                df['TRANSACTION_COUNT'] = df['TRANSACTION_COUNT'].astype(float)
                df_fig = px.bar(df, x='DISTRICTS', y='TRANSACTION_COUNT',
                                color='DISTRICTS', color_continuous_scale='thermal',
                                title=f'Top Analysis Chart', height=700,
                                text = 'TRANSACTION_COUNT')
                df_fig.update_layout(title_font=dict(size=37), title_font_color='#AD71EF')
                df_fig.update_traces(textposition='outside')
                st.write(df10)
                st.plotly_chart(df_fig)
            


