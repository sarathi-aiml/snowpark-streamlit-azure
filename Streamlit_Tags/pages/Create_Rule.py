# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 13:23:06 2023

@author: User
"""

import streamlit as st
import pandas as pd
import  snowflake.connector
import re

st.set_page_config(page_title="Create Rule - TAGS", layout="wide", page_icon='snowflakelogo.png')

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

st.markdown(" ### :card_file_box: **Rule Generation**")

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
    
    def objtype_changed():
        st.session_state.objtype = st.session_state.objtype
        
    table2 = st.selectbox(
        'Select Type',
        options=('Table', 'View', 'Column'),
        key='objtype',
        on_change = objtype_changed)
    
    objecttype = st.session_state.objtype 
    #st.write(st.session_state)  
    
    
output = ""  

with st.container():
   col1, col2, col3  = st.columns(3, gap="large")

   with col1:                  
        if objecttype == 'Column':
            table3 = st.selectbox(
               'Name pattern',
               options=('Starts with', 'Ends with', 'Contains'),
               key='tblpatt')
            
            tablepattern = st.session_state.tblpatt
            
        else:
            table3 = st.selectbox(
               'Name pattern',
               options=('Starts with', 'Ends with', 'Contains', 'Has role', 'Created by user'),
               key='tblpatt')
            
            tablepattern = st.session_state.tblpatt

   with col2:
       table4 = st.text_input('Type pattern here eg cust_tbl', key='name1')
       
       patternvalue = st.session_state.name1
       
       #st.write('pattern typed:', patternvalue)
       
   with col3:
       st.text("")
       

with st.container():
   col1, col2, col3 = st.columns(3, gap='large')
    
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
       if tagoption:        
           sql2 = "select system$get_tag_allowed_values('" + db + "." + schema + "." + tag + "');"
           cur.execute(sql2)
           results2 = cur.fetchall()        
           
           for row in results2:
               qryres2 = str(row[0])   
               #st.write(qryres2)
               res = re.findall(r'\w+', qryres2)
               #st.write(res)               
                  
           def tagvalue_changed():
               st.session_state.tagvalue = st.session_state.tagvalue
               
           
           tagvalueoption = st.selectbox(
                        'Select TagValue',
                        options=res,                     
                        key='tagvalue',
                        on_change = tagvalue_changed
                        )
           
           tagvalue = st.session_state.tagvalue                  
           
       else:
           tagvalue = '-'
            

   with col3:
       def update():
           st.session_state.name = st.session_state.name
           st.session_state.schname = st.session_state.schname
           st.session_state.objtype = st.session_state.objtype
           st.session_state.tblpatt = st.session_state.tblpatt
           st.session_state.name1 = st.session_state.name1
           st.session_state.tagname1 = st.session_state.tagname1           
           st.session_state.tagvalue = st.session_state.tagvalue
       
       createrulebtn = st.button(":heavy_check_mark: Create Rule", on_click=update, use_container_width=True)
       
       if createrulebtn:
           if tagoption:
               sql7 = "INSERT INTO RulesforTagApplication(DBName, SchemaName, ObjectType, NamePattern, PatternValue, TagName, TagValue, CreatedDate, IsActive) VALUES  ("" '"+ db +"' "","" '"+ schema +"' "","" '"+ objecttype +"' "","" '"+ tablepattern +"' "","" '"+ patternvalue +"' "","" '"+ tag +"' "","" '"+ tagvalue +"' "",CURRENT_TIMESTAMP(),True);"
               cur.execute(sql7)             
               #st.write(sql7)
               
               output = "Rule Created Successfully."                        
               
with st.container():    
    
    if output != "":
    
        st.markdown("<div style='text-align: center;font-size:30px;font-weight: bold;color: green'>"+ output +"</div>",
                unsafe_allow_html=True)       

st.text("")
st.text("")
st.text("Existing rules (all)")
@st.cache_data(ttl=60)
def defaultonload():       
    sql6 =  ("select UNIQUEID , DBNAME AS "' "DATABASE NAME" '", SCHEMANAME AS "' "SCHEMA NAME" '", OBJECTTYPE AS "' "OBJECT TYPE" '", NAMEPATTERN AS "' "NAME PATTERN" '", PATTERNVALUE AS "' "PATTERN VALUE" '", TAGNAME AS "' "TAG NAME" '", TAGVALUE AS "' "TAG VALUE" '", CREATEDDATE AS "' "CREATED DATE" '", ISACTIVE from RulesforTagApplication;")         
    cur.execute(sql6)            
    results6 = cur.fetchall()             
    df = pd.DataFrame(results6, columns=[desc[0] for desc in cur.description])
    return df         
        
qryres6 = defaultonload()
#st.write(qryres6)

# edited_df = st.data_editor(qryres6,hide_index=True,disabled=["UniqueID", "DBName", "SchemaName", "ObjectType", "NamePattern", "PatternValue", "TagName", "TagValue"], use_container_width=True)
# selected_rows = qryres6[edited_df.ISACTIVE]
# #st.write(selected_rows)
# nonselected_rows = qryres6[edited_df.ISACTIVE == False]
# # st.write(nonselected_rows)

# for row in selected_rows.iterrows():
#     #st.write(row)
#     sql8 = "UPDATE RulesforTagApplication SET ISACTIVE = TRUE WHERE UniqueID = " + str(row[1].UNIQUEID) + "; "
#     #st.write(sql8)
#     cur.execute(sql8)
    
# for row in nonselected_rows.iterrows():
#     #st.write(row)
#     sql8 = "UPDATE RulesforTagApplication SET ISACTIVE = FALSE WHERE UniqueID = " + str(row[1].UNIQUEID) + "; "
#     #st.write(sql8)
#     cur.execute(sql8)

if "df_value" not in st.session_state:
    st.session_state.df_value = qryres6
    
outputres = "" 

def updatechkval(edited_df, uniqueid):
    # st.write("changed")
    #st.write(uniqueid)     
    
    selected_rows = qryres6[edited_df.ISACTIVE]
    #st.write(selected_rows)          
    
    for uniqueval in uniqueid:
        #st.write(uniqueval)      
    
        if (uniqueval in selected_rows['UNIQUEID'].values):
            #st.write(uniqueval)
            sql8 = "UPDATE RulesforTagApplication SET ISACTIVE = TRUE WHERE UniqueID = " + str(uniqueval) + " ;"
            #st.write(sql8)        
            cur.execute(sql8)  
            
            outputres = "Rule Activated Successfully."
            
            if outputres != "":
            
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: green'>"+ outputres +"</div>",
                        unsafe_allow_html=True)
         
        else:
            sql8 = "UPDATE RulesforTagApplication SET ISACTIVE = FALSE WHERE UniqueID = " + str(uniqueval) + " ;"
            #st.write(sql8)        
            cur.execute(sql8) 
            
            outputres = "Rule Deactivated Successfully."     
            
            if outputres != "":
            
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: green'>"+ outputres +"</div>",
                        unsafe_allow_html=True)
          
        
edited_df = st.data_editor(qryres6,disabled=["UNIQUEID", "DATABASE NAME", "SCHEMA NAME", "OBJECT TYPE", "NAME PATTERN", "PATTERN VALUE", "TAG NAME", "TAG VALUE"],hide_index=True, use_container_width=True)


if edited_df is not None and not edited_df.equals(st.session_state["df_value"]):
    # This will only run if
    # 1. Some widget has been changed (including the dataframe editor), triggering a
    # script rerun, and
    # 2. The new dataframe value is different from the old value  
    
    seluniqueid = edited_df.iloc[(qryres6["ISACTIVE"] != edited_df["ISACTIVE"]).values]["UNIQUEID"].tolist()
    
    updatechkval(edited_df, seluniqueid)    
      
    st.session_state["df_value"] = edited_df