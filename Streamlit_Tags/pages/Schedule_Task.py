# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 15:25:14 2023

@author: User
"""

import streamlit as st
import  snowflake.connector


st.set_page_config(page_title="Schedule Task - TAGS", layout="wide", page_icon='snowflakelogo.png')

header_style = '''
    <style>
        thead, tbody, tfoot, tr, td, th{
            background-color:#fff;
            color:#000;
        } 
        
        section[data-testid="stSidebar"] div.stButton button {
          background-color: #049dbf;
          width: 300px;
          color: #fff;
        }
        .css-cgx0ld {
          background-color: #049dbf;
          color: #fff;
          font-size: 20px;
          float: right;
        }
        div.stButton button {
          background-color: #f7f7f7;
          color: #29b5e8;
          margin-top: 25px;
        }
        div[data-baseweb="select"] > div {
          background-color: #f7f7f7;
          color: #000;
        }
        
        .css-1b1e6xd {
          color: #000;
          font-weight: bold;
        }
    
    </style>
'''
st.markdown(header_style, unsafe_allow_html=True)

@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake4"], client_session_keep_alive=True
    )

con = init_connection()

cur = con.cursor()

st.markdown(" ### :card_file_box: **Task**")

output = ""

with st.container():
    col1, col2, col3 = st.columns(3)           
        
    with col1:
        def scheduletime_changed():
            st.session_state.time = st.session_state.time
            
        scheduletime = st.selectbox(
            'Schedule the automated task time',
            options=('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'),
            key='time',
            on_change = scheduletime_changed)
        
        tasktime = st.session_state.time
        #st.write(tasktime)
        
    with col2:
        st.text("")
        
    with col3:
        def update():
            st.session_state.time = st.session_state.time                      
            
        
        scheduletaskbtn = st.button(":clock12: Schedule Task", on_click=update, use_container_width=True)
        
        if scheduletaskbtn:
            cur.execute("ALTER TASK TagAutomation SUSPEND;") 
            
            sql8 = "ALTER TASK TagAutomation SET SCHEDULE = '" 'USING CRON 0 ' + tasktime + ' * * * UTC' "';"
            #st.write(sql8)
            cur.execute(sql8)
            output = "Task re-scheduled successfully."
            
            cur.execute("ALTER TASK TagAutomation RESUME;")          
            
            
with st.container():    
    
    if output != "":
    
        st.markdown("<div style='text-align: center;font-size:30px;font-weight: bold;color: green'>"+ output +"</div>",
                unsafe_allow_html=True)