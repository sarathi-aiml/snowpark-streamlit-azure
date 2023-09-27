# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 14:02:27 2023

@author: User
"""

import streamlit as st
import pandas as pd
import  snowflake.connector


st.set_page_config(page_title="Manage Tags - TAGS", layout="wide", page_icon='snowflakelogo.png')

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

st.markdown(" ### :card_file_box: **Tags**")

with st.sidebar:
    sql = "select distinct database_name from SNOWFLAKE.account_usage.databases"    
    cur.execute(sql)    
    results = cur.fetchall()    
    dblist = []
    
    for row in results:
        qryres = str(row[0])
        dblist.append(qryres) 

      
    default_ix = dblist.index('SNOWFLAKE')     
    

    def db_changed():
        st.session_state.name = st.session_state.name
        
    if 'name' in st.session_state:
        ##st.write(st.session_state['name'])
        default_ix = dblist.index(st.session_state['name'])
          
    option = st.selectbox(
                 'Select Database',
                 options=dblist,
                 index=default_ix,
                 key='name',
                 on_change = db_changed
             )
    ##st.write('You selected:', option)    
    
    db = st.session_state.name
    ##st.write(db)    
    
    schsql = 'select SCHEMA_NAME from ' + db + '.information_Schema.schemata;'
    cur.execute(schsql)
    schresults = cur.fetchall()
    schemalist = []
    
    for row in schresults:
        schqryres = str(row[0])
        schemalist.append(schqryres)
        
    
    def schema_changed():
        st.session_state.schname = st.session_state.schname
        
        
    schemaoption = st.selectbox(
                        'Select Schema',
                        options=schemalist,
                        key='schname',
                        on_change = schema_changed
                        )
    
    schema = st.session_state.schname
    ##st.write(schema)    
    
results7 = "" 
    
with st.container():
    col1, col2, col3 = st.columns(3)
    
    with col1:
              
        sql1 = "SELECT distinct tag_name FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS WHERE TAG_DATABASE='" + db + "' AND TAG_SCHEMA='" + schema + "' ORDER BY TAG_NAME"    
        cur.execute(sql1)    
        results1 = cur.fetchall()    
        taglist = []
        
        for row in results1:
            qryres1 = str(row[0])            
            taglist.append(qryres1)
            
                    
        def tag_changed():
            st.session_state.tagname1 = st.session_state.tagname1             
        
        tagoption = st.selectbox(
                     'Select Tag',
                     options=taglist,                     
                     key='tagname1',
                     on_change = tag_changed
                 )
        
        tag = st.session_state.tagname1
        ##st.write(tag)
        
    with col2:
        st.text("")
        
    with col3:
        def update():
            st.session_state.name = st.session_state.name
            st.session_state.schname = st.session_state.schname            
            st.session_state.tagname1 = st.session_state.tagname1           
            
        
        viewobjectsbtn = st.button(":eye: View Objects", on_click=update, use_container_width=True)
        
        if viewobjectsbtn:
            if tagoption:
                sql7 = "select tag_name AS "' "TAG NAME" '", tag_value AS "' "TAG VALUE" '", domain AS "' "OBJECT TYPE" '", object_name AS "' "OBJECT NAME" '",column_name AS "' "COLUMN NAME" '" from snowflake.account_usage.tag_references where tag_name="" '"+ tag +"' "" and TAG_DATABASE="" '"+ db +"' "";"
                #st.write(sql7)
                cur.execute(sql7)
                results7 = cur.fetchall() 
                #st.write(results7)
                
st.text("")
st.text("")    
            
with st.container():    
    
    if results7 != "":
        @st.cache_data(ttl=60)
        def defaultonload():
            df = pd.DataFrame(results7, columns=[desc[0] for desc in cur.description])            
            return df
        
        qryres7 = defaultonload()
        st.dataframe(qryres7, hide_index=True, use_container_width=True)