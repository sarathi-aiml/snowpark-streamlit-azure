CREATE OR REPLACE PROCEDURE APPLYTAG("DB" VARCHAR(16777216), "SCHE" VARCHAR(16777216), "TBL" VARCHAR(16777216), "TAGNAME" VARCHAR(16777216), "TAGVALUE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  select_statement VARCHAR;
  res RESULTSET;
BEGIN  
  select_statement := ''ALTER TABLE IF EXISTS '' || DB ||''.'' || SCHE || ''.'' || TBL || '' SET TAG '' || TAGNAME || ''='''''' || TAGVALUE || '''''''';
  res := (EXECUTE IMMEDIATE :select_statement);  
  return ''Tag applied successfully'';
END';

-- Metadata table to store tag rules ---------------

CREATE or replace TABLE RULESFORTAGAPPLICATION (
	UNIQUEID int identity(1,1),
	DBNAME VARCHAR(30),
	SCHEMANAME VARCHAR(20),
	OBJECTTYPE VARCHAR(10),
	NAMEPATTERN VARCHAR(20),
	PATTERNVALUE VARCHAR(50),
	TAGNAME VARCHAR(20),
	TAGVALUE VARCHAR(30),
	CREATEDDATE TIMESTAMP_LTZ(9),
	ISACTIVE BOOLEAN,
	--ISEXECUTED BOOLEAN,
	LASTRUNDATETIME TIMESTAMP_NTZ(9),
	LASTRUNSTATUS BOOLEAN,
	LASTRUNERROR VARCHAR(8000)
);

select * from RulesforTagApplication;
 
---- procedure to apply rule and set tags for tables, views, columns taken from the metadata table ----------------------- 

 
CREATE OR REPLACE PROCEDURE trial_123.public.TagAutomation_test()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
try
{       
    var Query_Get_Metadata="",ResultSet_MetaData="",uniqueid="",dbname="",schemaname="",objecttype="",namepattern="",patternvalue="",tagname="";    
    var tagvalue="",rulestatus="",isexecuted="";    
    var Query_SchemaInfo="",stmt_SchemaInfo="",ResultSet_SchemaInfo="";
    var stmt_metadata="",ResultSet_MetaData="";
    
    Query_Get_Metadata +=" select uniqueid,dbname,schemaname,objecttype,namepattern,patternvalue,tagname,tagvalue, ";
    Query_Get_Metadata +=" ifnull(isactive,false) as rulestatus ";
    Query_Get_Metadata +=" from trial_123.public.RulesforTagApplication ";
    Query_Get_Metadata +=" where isactive = true";    
     
    stmt_metadata = snowflake.createStatement({sqlText:Query_Get_Metadata});
    ResultSet_MetaData = stmt_metadata.execute(); 
 
    if(!ResultSet_MetaData.next())
    {
        return "No records found in metadata table to do tag action";
    } 
    var querytext="";
    for (var i = 1; i <= stmt_metadata.getRowCount(); i++) 
    {
        uniqueid = ResultSet_MetaData.getColumnValue(1);                       
        dbname = ResultSet_MetaData.getColumnValue(2);                       
        schemaname = ResultSet_MetaData.getColumnValue(3);                       
        objecttype = ResultSet_MetaData.getColumnValue(4);                       
        namepattern = ResultSet_MetaData.getColumnValue(5);                       
        patternvalue = ResultSet_MetaData.getColumnValue(6);                       
        tagname = ResultSet_MetaData.getColumnValue(7);                       
        tagvalue = ResultSet_MetaData.getColumnValue(8);     
        rulestatus = ResultSet_MetaData.getColumnValue(9);                               

        var tablename_schema ="",columnname_schema="",fun_columnname="";
    
        if(objecttype.toUpperCase() == "TABLE")
        {
            tablename_schema ="information_schema.tables";
            columnname_schema="table_catalog,table_schema,table_name";
            fun_columnname="Table_name";
            objecttype = "BASE TABLE";           
        }
        else if (objecttype.toUpperCase() == "VIEW")
        {
            fun_columnname="Table_name";
            tablename_schema ="information_schema.tables";
            columnname_schema="table_catalog,table_schema,table_name";
            objecttype = "VIEW";
        }
        else if (objecttype.toUpperCase() == "COLUMN")
        {
            fun_columnname="column_name";
            columnname_schema="table_catalog,table_schema,table_name,column_name";
            tablename_schema ="information_schema.columns";
            objecttype = "COLUMN";
        }

        
        namepattern = namepattern.toUpperCase();
        patternvalue = patternvalue.toUpperCase();
        
        var sWhereclause="";
        
        if(namepattern == "STARTS WITH")
        {
            sWhereclause =" STARTSWITH("+ fun_columnname +", '"+ patternvalue +"')";            
        }
        else if (namepattern == "ENDS WITH")
        {
            sWhereclause =" ENDSWITH("+ fun_columnname +", '"+ patternvalue +"')";
        }
        else if (namepattern == "CONTAINS")
        {
            sWhereclause =" CONTAINS("+ fun_columnname +", '"+ patternvalue +"')";            
        }
        else if (namepattern == "CREATED BY")
        {
            sWhereclause =" UPPER(LAST_DDL_BY) ='"+ patternvalue +"'";                        
        }
        else if (namepattern == "HAS ROLE")
        {
            sWhereclause =" UPPER(table_owner) ='"+ patternvalue +"'";                        
        }
        else
        {
            Query_SchemaInfo="";
        }           
           

        Query_SchemaInfo = "select "+ columnname_schema +" from "+ dbname +"."+ tablename_schema +" where "+sWhereclause;        
        Query_SchemaInfo += " and UPPER(table_schema)='"+ schemaname +"' ";
        Query_SchemaInfo += " and UPPER(table_catalog)='"+ dbname +"' ;" ; 
        if(objecttype.toUpperCase() == "TABLE" || objecttype.toUpperCase() == "VIEW")
        {
            Query_SchemaInfo +=" and UPPER(table_type)='"+ objecttype +"' ";
        }       
        //return Query_SchemaInfo;
        var stmt_tag = snowflake.createStatement({sqlText:Query_SchemaInfo});
        var ResultSet_tag = stmt_tag.execute(); 
     
        if(!ResultSet_tag.next())
        {
            return Query_SchemaInfo;//"No records found to set tag";
        }
        var samptext="";
        for (var j = 1; j <= stmt_tag.getRowCount(); j++) 
        {
            var table_catalog = ResultSet_tag.getColumnValue(1);  
            var table_schema = ResultSet_tag.getColumnValue(2);  
            var table_name = ResultSet_tag.getColumnValue(3);
            var query_apply_tag="";

            //return "objecttype "+ objecttype + "rulestatus "+rulestatus;
            if(objecttype.toUpperCase() == "COLUMN")
            {
                var column_name = ResultSet_tag.getColumnValue(4); 
                if(rulestatus == true)
                {
                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" ";
                    query_apply_tag +=" modify column "+ column_name +" SET TAG "+tagname +" = '"+ tagvalue +"'";
                }
                else
                {
                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" ";
                    query_apply_tag +="modify column "+ column_name +" UNSET TAG "+tagname +" ";
                }                                
            }
            else
            {
                if(rulestatus == true)
                {
                     query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" SET TAG "+tagname +" = '"+ tagvalue +"'";
                }
                else
                {
                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" UNSET TAG "+tagname +" ";
                }            
            }           
            return query_apply_tag;
            var stmt_apply_tag = snowflake.createStatement({sqlText:query_apply_tag});
            var ResultSet_apply_tag = stmt_apply_tag.execute();             
           
            ResultSet_tag.next();  
            
        }

        var query_update_result ="";
        query_update_result="update trial_123.public.RulesforTagApplication set lastrunstatus=true,lastrundatetime=CURRENT_TIMESTAMP(),";
        query_update_result +=" lastrunerror='' where uniqueid ="+uniqueid;        
        var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
        var result_update = stmt_result_update.execute();
        
        ResultSet_MetaData.next();  
    }
    
    return "success";
    
     
}    
catch(err)
{
    var errstr;
    errstr = "Failed: Code: " + err.code + "\\n  State: " + err.state;
    errstr += "\\n  Message: " + err.message;
    errstr += "\\nStack Trace:\\n" + err.stackTraceTxt;

    var query_update_result ="update trial_123.public.RulesforTagApplication set lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
    query_update_result =+" lastrunerror='"+errstr+"' where uniqueid="+uniqueid;
    var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
    var result_update = stmt_result_update.execute();
    
    return "failure";
}
$$;

----------------- Task to automatically call the Tag automation procedure ----------

create or replace task TagAutomation
    WAREHOUSE = COMPUTE_WH    
    SCHEDULE = 'USING CRON 0 6 * * * UTC' --everyday 6 am
    AS 
    call  trial_123.public.TagAutomation_test();

