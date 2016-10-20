CREATE OR REPLACE PACKAGE BODY CMS_DEV_BUILD_71.TA_PROCESS
AS

function GET_ACHIEVE_VAL(v_ta_parameter ta_parameters,V_TA_RULE_QUERY_ID NUMBER) return varchar2
  as
  TA_QUERY clob;
  cursor_name INTEGER;
  returnVarVal Varchar2(100);
  bindvar varchar2(20);
  rows_processed NUMBER;

BEGIN


     SELECT (t.TA_RULE_EXEC_QUERY) INTO  TA_QUERY 
     FROM TA_RULE_QUERY t WHERE t.TA_RULE_QUERY_ID = V_TA_RULE_QUERY_ID 
            and t.IS_ACTIVE_IND='Y' and t.IS_DELETED_IND='N';
    


      cursor_name := dbms_sql.open_cursor;
     DBMS_SQL.PARSE(cursor_name, TA_QUERY,DBMS_SQL.NATIVE);
    for x in ( select SEQ_NO,TA_RULE_QUERY_PARAM_NM,DATA_TYPE,TA_RULE_QUERY_PARAM_TYP from TA_RULE_QUERY_PARAM  where TA_RULE_QUERY_ID =V_TA_RULE_QUERY_ID   and IS_DELETED_IND='N' order by SEQ_NO,TA_RULE_QUERY_PARAM_TYP desc)
    loop
    
  

        if x.TA_RULE_QUERY_PARAM_TYP ='O' then
           DBMS_SQL.DEFINE_COLUMN(cursor_name, x.SEQ_NO, returnVarVal,100);
        elsif x.TA_RULE_QUERY_PARAM_TYP ='I' then


            bindvar :=':'||x.SEQ_NO;
            if(x.DATA_TYPE is not null and upper(x.DATA_TYPE)='NUMBER') then
                DBMS_SQL.BIND_VARIABLE(cursor_name, bindvar, TO_TA_NUMBER(v_ta_parameter(upper(x.TA_RULE_QUERY_PARAM_NM))));
                 

            elsif(x.DATA_TYPE is not null and upper(x.DATA_TYPE)='DATE') then

                DBMS_SQL.BIND_VARIABLE(cursor_name, bindvar, TO_TA_DATE(v_ta_parameter(upper(x.TA_RULE_QUERY_PARAM_NM))));
            elsif(x.DATA_TYPE is not null and upper(x.DATA_TYPE)='TIMESTAMP') then
                DBMS_SQL.BIND_VARIABLE(cursor_name, bindvar, TO_TA_TIMESTAMP(v_ta_parameter(upper(x.TA_RULE_QUERY_PARAM_NM))));
            else
                DBMS_SQL.BIND_VARIABLE(cursor_name, bindvar, v_ta_parameter(upper(x.TA_RULE_QUERY_PARAM_NM)));
            end if;
        end if;

    end loop;
                  
   

        rows_processed := DBMS_SQL.EXECUTE(cursor_name);
        rows_processed :=DBMS_SQL.FETCH_ROWS(cursor_name);
       IF rows_processed >0 THEN

        DBMS_SQL.COLUMN_VALUE(cursor_name, 1, returnVarVal);
              
       end if;
       DBMS_SQL.CLOSE_CURSOR(cursor_name);
       return returnVarVal;
--EXCEPTION
--    WHEN OTHERS THEN
--            IF( cursor_name IS NOT NULL) THEN
--                   DBMS_SQL.CLOSE_CURSOR(cursor_name);
--            END IF;
--            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM,true);
END;


FUNCTION TO_TA_DATE (V_PARAM VARCHAR) RETURN DATE
 as
BEGIN
        IF(V_PARAM IS NOT NULL) THEN
            RETURN TO_DATE(V_PARAM,'DD/MM/YYYY');
        ELSE
            RETURN NULL;
        END IF;
 END;
 FUNCTION TO_TA_NUMBER (V_PARAM VARCHAR) RETURN NUMBER
 as
BEGIN
        IF(V_PARAM IS NOT NULL) THEN
            RETURN TO_NUMBER(V_PARAM);
        ELSE
            RETURN null;
        END IF;
 END;
 FUNCTION TO_TA_TIMESTAMP (V_PARAM VARCHAR) RETURN TIMESTAMP
 as
BEGIN
        IF(V_PARAM IS NOT NULL) THEN
            RETURN TO_TIMESTAMP(V_PARAM,'DD/MM/YYYY HH:MI:SS');
        ELSE
            RETURN NULL;
        END IF;
 END;
 FUNCTION GET_HIERARCHY_ID (v_hierarchy_grp_id varchar2) RETURN varchar2
 as
 v_hierarchy_id varchar2(100);
BEGIN
    if(v_hierarchy_grp_id is not null) then
        select (hierarchy_id) into v_hierarchy_id from hierarchy_grp where hierarchy_grp_id=v_hierarchy_grp_id and is_deleted_ind='N';
    else
        v_hierarchy_id :=null;
    end if;
    return v_hierarchy_id;
 END;
 function GET_ACHIEVE_ID(v_array ta_parameters,v_hierarchy_id varchar2) return NUMBER
 as
 v_entity_typ varchar2(100);
 v_achieve_id number;
 BEGIN
        v_achieve_id :=0;
        v_entity_typ :=v_array(upper('entity_type'));
        if(v_entity_typ is not null and  lower(v_entity_typ)='geo') then
            select   (ta_achievement_id)  into v_achieve_id from TA_ACHIEVEMENT where target_type_id=1 and party_id=v_array(upper('human_Or_Geo_Party_Id')) and hierarchy_id=TO_TA_NUMBER(v_hierarchy_id) and ta_parameter_id=v_array(upper('ta_parameter_id')) and achievement_period=TO_TA_DATE(v_array(upper('startDate'))) ;
        elsif(v_entity_typ is not null and  lower(v_entity_typ)='human') then
            select  (ta_achievement_id)  into v_achieve_id from TA_ACHIEVEMENT where target_type_id=2 and party_id=v_array(upper('human_Or_Geo_Party_Id')) and Human_job_position_typ_id=v_array(upper('human_pos_type_id')) and org_party_id=v_array(upper('human_org_party_id')) and ta_parameter_id=v_array(upper('ta_parameter_id')) and achievement_period=TO_TA_DATE(v_array(upper('startDate')));
        end if;
        --dbms_output.put_line(v_achieve_id);
        return v_achieve_id;
     exception
     WHEN NO_DATA_FOUND THEN
         return 0;

     when others then
       -- v_achieve_id :=0;
       -- return v_achieve_id;
      raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM,true);
     --dbms_output.put_line(v_achieve_id);

 END;
 

 
 Procedure ACHIEVEMENT_CALC IS
 --P_CUR CURSOR_TYPE ;
 V_SQL CLOB ;
 v_insert_qry CLOB ;
 V_TST NUMBER ;
 v_uploadfor Varchar2(20) ;
 v_fin_year Varchar2(10) ;
 v_StartDt DATE;
 v_EndDt DATE;
 Q_CUR CURSOR_TYPE;
 Col_Nm CURSOR_TYPE;
 P_CUR1 CURSOR_TYPE ;
 
 v_array ta_parameters;
    v_cursor_id integer;
    v_col_cnt integer;
    v_columns dbms_sql.desc_tab;
    colVal varchar2(4000);
    rc number;
    v_rule_qry_id number;
    v_party_id number;
    v_process_log_id number;
   
    v_entity_typ varchar2(100);
    v_achieve_max_id number;
    v_achievement varchar2(4000);
    v_hierarchy_id varchar2(100);
     v_mssg varchar2(4000);
     v_log_param clob DEFAULT '';
     v_table_nm TA_RULE_QUERY.TMP_TABLE_NM%TYPE ;
     v_ach_colname Varchar2(100);
     v_tbl_colname Varchar2(100);
     v_qry clob;
     v_t NUMBER ;
     v_target_type_id TA_TARGET_DEF_HDR.TARGET_TYPE_ID%TYPE;
     v_ta_param_id TA_PARAMDEF_DET.TA_PARAMETER_ID%TYPE;
     v_human_pos_type TA_TARGET_DEF_DET.HUMAN_POS_TYPE_ID%TYPE ;
     v_hierarchy_grp_id TA_TARGET_DEF_DET.GEO_HIERARCHY_GRP_ID%TYPE;
     v_count NUMBER ;
     v_ticker_last_run_Date DATE ;
     v_current_date DATE;
     CUR_ACHV CURSOR_TYPE ;
    
    -- v_rule_qry_id NUMBER ;
    
 CURSOR P_CUR IS 
    select distinct th.TA_TARGET_DEF_HDR_ID, af.TA_FREQUENCY_NM, th.TARGET_START_MONTH from 
    TA_TARGET_DEF_HDR th inner join 
    TA_TARGET_DEF_DET td on td.TA_TARGET_DEF_HDR_ID = th.TA_TARGET_DEF_HDR_ID and td.IS_ACTIVE_IND='Y' and td.IS_DELETED_IND='N'
    inner join
    TA_PARAMDEF_HDR ph on ph.TA_TARGET_DEF_DET_ID = td.TA_TARGET_DEF_DET_ID and ph.IS_ACTIVE_IND='Y' and ph.IS_DELETED_IND='N'
    inner join
    TA_PARAMDEF_DET pd on pd.TA_PARAMDEF_HDR_ID = ph.TA_PARAMDEF_HDR_ID and pd.IS_ACTIVE_IND='Y' and pd.IS_DELETED_IND='N'
    inner join ADM_TA_FREQUENCY af ON af.TA_FREQUENCY_ID = th.REVIEW_FREQUENCY and af.IS_DELETED_IND='N'
    where th.IS_ACTIVE_IND='Y' and th.IS_DELETED_IND='N';-- and th.TA_TARGET_DEF_HDR_ID = 100610  ;
    
 BEGIN
 
 
 

    
    
 
    
    select to_date(to_char(LAST_EXPORT_TSTAMP,'mon-YYYY'),'mon-YYYY') , sysdate
    into v_ticker_last_run_Date,v_current_date from INTERFACE_CONTROL 
    where interface_cd='TACALC' and interface_type='TA' ; 
    
    -- Filter Queries Execution
    delete from temp_Ta_table ;
    insert into temp_TA_table
    select distinct 
    th.TA_TARGET_DEF_HDR_ID, tuh.target_type_id, pd.ta_parameter_id, td.human_pos_type_id, td.geo_hierarchy_grp_id ,
    ta_process.calc_end_date(tuh.TA_FREQUENCY_ID,th.target_start_month,tuh.uploadfor,tuh.fin_year) End_Date,
    ta_process.calc_start_date(tuh.TA_FREQUENCY_ID,th.target_start_month,tuh.uploadfor,tuh.fin_year) Start_Date,
    tuh.PARTY_ID,
    tuh.ORG_PARTY_ID
    from Target_Parameter_Upload_Det tud inner join Target_Parameter_Upload_Hdr tuh on tuh.target_param_upload_hdr_id=tud.target_param_upload_hdr_id     
    inner join ta_target_def_det td on td.ta_target_def_det_id=tuh.ta_target_def_det_id and td.is_active_ind='Y' and td.is_deleted_ind='N'   
    inner join ta_target_def_hdr th on th.ta_target_def_hdr_id=td.ta_target_def_hdr_id and th.is_active_ind='Y' and th.is_deleted_ind='N'   
    inner join ta_paramdef_hdr ph on ph.ta_target_def_det_id=td.ta_target_def_det_id and ph.is_active_ind='Y' and ph.is_deleted_ind='N'    
    inner join ta_paramdef_det pd on pd.ta_paramdef_hdr_id =ph.ta_paramdef_hdr_id and pd.is_active_ind='Y' and pd.is_deleted_ind='N'    
    inner join adm_ta_frequency af on af.ta_frequency_id=th.review_frequency and af.is_deleted_ind='N'   
    inner join adm_ta_type tt on tt.target_type_id=tuh.target_type_id where tud.is_deleted_ind='N'
    and ta_process.calc_end_date(tuh.TA_FREQUENCY_ID,th.target_start_month,tuh.uploadfor,tuh.fin_year) >=
     v_ticker_last_run_Date ;


    --- Target Upload Query -----
    
    ------------------------------
      
    FOR I IN P_CUR LOOP
    
    
    insert into tmp_test(txt,LOGV,DATETIME) values ('1',v_ticker_last_run_Date || ',' || v_current_date || ',' || I.TARGET_START_MONTH || ',' || I.TA_FREQUENCY_NM , systimestamp);
    commit;
              
    CUR_ACHV := TA_PROCESS.GETTICKLERDATES(v_ticker_last_run_Date, v_current_date, I.TARGET_START_MONTH , I.TA_FREQUENCY_NM) ;
        
        LOOP
        FETCH CUR_ACHV into v_Startdt, v_enddt ;
        EXIT WHEN CUR_ACHV%NOTFOUND ;   
        
            insert into tmp_test(txt,logv,DATETIME) values('2','Start Date : --'||v_startdt || '--EndDate:--' || v_Enddt, systimestamp) ;
            commit;
        
             FOR reg IN (select  UPPER(TMP_TABLE_NM) as table_name
                from TA_FILTER_QUERY union select UPPER(TMP_TABLE_NM) from TA_RULE_QUERY)
                LOOP
                
                SELECT COUNT(*) INTO v_t FROM user_tables WHERE table_name = reg.table_name ; 
                  
                  IF v_t = 1 THEN 
                  EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || reg.table_name;
                 END IF ;
                  
                END LOOP;
    
            OPEN  P_CUR1 FOR 
                select QRY_TXT, TMP_TABLE_NM  from TA_FILTER_QUERY 
                Where TA_TARGET_DEF_HDR_ID = I.TA_TARGET_DEF_HDR_ID
                order by  QRY_ORDER ASC ;
            
            LOOP
             FETCH P_CUR1 into v_qry, v_table_nm ;
             EXIT WHEN P_CUR1%NOTFOUND;
             
                
                
                V_SQL:= REPLACE(v_qry , '^^StartDate^^','''' || v_startdt || '''' );
                V_SQL:= REPLACE(V_SQL , '^^startDate^^','''' || v_startdt || '''' );
                V_SQL:= REPLACE(V_SQL , '<<FromDate>>','''' || v_startdt || '''' );
                
                V_SQL:= REPLACE(V_SQL , '^^EndDate^^','''' || v_Enddt || '''' );
                V_SQL:= REPLACE(V_SQL , '^^endDate^^','''' || v_Enddt || '''' );
                V_SQL:= REPLACE(V_SQL , '<<ToDate>>','''' || v_Enddt || '''' );
                
                
                SELECT COUNT(*) INTO v_t FROM user_tables WHERE table_name = UPPER(v_table_nm) ; 
              
                IF v_t = 0 THEN
                
                    v_qry := 'Create Table ' || v_table_nm || ' as select * from (' || V_SQL || ' ) where 1<>1 ' ;
                    EXECUTE IMMEDIATE v_qry ; 
                    v_qry := NULL ;
                
                End If;
                
                
                
                V_SQL := 'Insert into ' || v_table_nm || ' ' || V_SQL ;
                
               -- insert into tmp_test(txt,LOGV,datetime) values ('1',V_SQL,systimestamp) ;
               -- commit;
                
                 
                
                EXECUTE IMMEDIATE V_SQL ;
                commit;
            END LOOP ;
            
        
                OPEN Q_CUR FOR
                        select tq.TA_RULE_QUERY, tq.TMP_TABLE_NM, tq.TA_RULE_QUERY_ID, ta_hdr.TARGET_TYPE_ID, td.TA_PARAMETER_ID,
                        ta_det.HUMAN_POS_TYPE_ID, ta_det.GEO_HIERARCHY_GRP_ID
                        from
                        ta_paramdef_hdr th,
                        TA_PARAMDEF_DET td,
                        TA_RULE_QUERY tq,
                        TA_TARGET_DEF_DET ta_det,
                        TA_TARGET_DEF_HDR ta_hdr
                        where th.TA_PARAMDEF_HDR_ID = td.TA_PARAMDEF_HDR_ID
                        and th.IS_ACTIVE_IND='Y' and td.IS_ACTIVE_IND='Y' and
                        tq.TA_RULE_QUERY_ID = td.TA_RULE_QUERY_ID and
                        ta_det.TA_TARGET_DEF_DET_ID = th.TA_TARGET_DEF_DET_ID and
                        ta_hdr.TA_TARGET_DEF_HDR_ID = ta_det.TA_TARGET_DEF_HDR_ID and
                        ta_hdr.TA_TARGET_DEF_HDR_ID = I.TA_TARGET_DEF_HDR_ID and 
                        tq.IS_ACTIVE_IND='Y' and tq.IS_DELETED_IND='N' and
                        tq.TMP_TABLE_NM is not null
                        order by ta_hdr.TA_TARGET_DEF_HDR_ID, td.SEQ_NO ASC ;
                        
                LOOP
                    FETCH Q_CUR into v_sql,  v_table_nm , v_rule_qry_id, v_target_type_id, v_ta_param_id, v_human_pos_type, v_hierarchy_grp_id ;
                    EXIT WHEN Q_CUR%NOTFOUND;   
                    
                   -- insert into tmp_test(txt,LOGV,datetime) values ( '2',v_table_nm||','||I.TA_TARGET_DEF_HDR_ID,systimestamp);
                    
                   -- insert into tmp_test(txt,LOGV,datetime) values ('3',v_sql,systimestamp);
                   -- commit;   
                    
                    V_SQL:= REPLACE(v_sql , '^^StartDate^^','''' || v_startdt || '''' );
                    V_SQL:= REPLACE(V_SQL , '^^startDate^^','''' || v_startdt || '''' );
                    V_SQL:= REPLACE(V_SQL , '<<FromDate>>','''' || v_startdt || '''' );
                    
                    V_SQL:= REPLACE(V_SQL , '^^EndDate^^','''' || v_Enddt || '''' );
                    V_SQL:= REPLACE(V_SQL , '^^endDate^^','''' || v_Enddt || '''' );
                    V_SQL:= REPLACE(V_SQL , '<<ToDate>>','''' || v_Enddt || '''' );
                    
                    SELECT COUNT(*) INTO v_t FROM user_tables WHERE table_name = UPPER(v_table_nm) ; 
                  
                    IF v_t = 0 THEN
                    
                        v_qry := 'Create Table ' || v_table_nm || ' as select * from (' || V_SQL || ' ) where 1<>1 ' ;
                        
                       -- insert into tmp_test(txt,LOGV,datetime) values ('3.1',v_qry,systimestamp);
                      --  commit; 
                        EXECUTE IMMEDIATE v_qry ; 
                        v_qry := NULL ;
                    
                    End If;
                    
                    v_insert_qry := 'insert into '  || v_table_nm || '  ' || v_sql ;
                    EXECUTE IMMEDIATE v_insert_qry ;
                            
                                
                    OPEN COL_NM FOR        
                            select  ach.TA_RULE_QUERY_PARAM_NM, Tb.COLUMN_NAME from 
                            ALL_TAB_COLUMNS Tb 
                            left join TA_RULE_QUERY_PARAM ach on ach.ACHV_COL_NAME = Tb.COLUMN_NAME 
                            and TA_RULE_QUERY_ID = v_rule_qry_id
                            where Tb.TABLE_NAME='TA_ACHIEVEMENT' ;
                            
                    LOOP
                    
                    FETCH COL_NM into v_ach_colname, v_tbl_colname ;
                    EXIT WHEN COL_NM%NOTFOUND;
                     
                     IF  v_ach_colname is not null then
                     v_array(v_tbl_colname) :=   v_ach_colname ;
                     Else
                     v_array(v_tbl_colname) := 'NULL' ;
                     ENd If; 
                     
                     v_log_param := v_log_param|| v_tbl_colname ||':' ||v_ach_colname ||'~';
                     
                    END LOOP ;   
                    
                    --insert into tmp_test(txt,LOGV,datetime) values ('4',v_log_param,systimestamp);
                   -- commit; 
                    
                      
                    
                    
                    select count(1) into v_count from temp_ta_table where TA_TARGET_DEF_HDR_ID = I.TA_TARGET_DEF_HDR_ID and
                    TARGET_TYPE_ID = v_target_type_id and TA_PARAMETER_ID = v_ta_param_id 
                    and END_DATE = v_EndDt and  START_DATE =v_startdt ;
                    
                   -- insert into tmp_test(txt,LOGV,datetime) values ('567567',v_count||','||I.TA_TARGET_DEF_HDR_ID||','||v_target_type_id||','||v_ta_param_id||','||v_EndDt||','||v_startdt,systimestamp);
                   -- commit; 
                  
                    v_insert_qry := 'insert into TA_ACHIEVEMENT(ta_achievement_id,
                                                       target_type_id,ta_parameter_id,party_id,achievement,
                                                       achievement_period,lst_updated_dtm,lst_updt_userid,is_deleted_ind,
                                                       hierarchy_id,HUMAN_JOB_POSITION_TYP_ID, org_party_id,ACHIEVEMENT_PERIOD_END) ' ;
                  
                    IF v_count = 0 Then
                    
                        IF v_target_type_id = 1 or v_target_type_id = 3 Then
                        
                        v_hierarchy_id :=GET_HIERARCHY_ID(v_hierarchy_grp_id) ;
                        
                        
                        -- Delete Geo Achievement 
                         v_qry := 'delete from ta_achievement where   ' ||
                          '  achievement_period= ''' || v_startdt ||  ''' and  TA_PARAMETER_ID = ' || v_ta_param_id ||
                          --' and  HIERARCHY_ID = ' || v_hierarchy_id ||
                          ' and target_type_id= ' || v_target_type_id ||  ' and PARTY_ID IN ( ' ||
                          ' select ' ||  v_Array('PARTY_ID') 
                                    || ' FROM ' || v_table_nm || ')' ;
                         
                        -- insert into tmp_test(txt,LOGV,datetime) values ('5',v_qry,systimestamp);
                        -- commit; 
                         EXECUTE IMMEDIATE v_qry ;
                         
                        -- Delete Human Achivement
                        Elsif v_target_type_id = 2 Then
                         v_qry := 'delete from ta_achievement where   ' ||
                          '  achievement_period= ''' || v_startdt ||  ''' and  TA_PARAMETER_ID = ' || v_ta_param_id ||
                          ' and HUMAN_JOB_POSITION_TYP_ID = ' || v_human_pos_type ||
                          ' and target_type_id= 2 and PARTY_ID IN ( ' || 
                          ' select ' ||  v_Array('PARTY_ID')  || ' FROM ' || v_table_nm || ')' ;
                   
                         
                         --insert into tmp_test(txt,LOGV,datetime) values ('6',v_qry,systimestamp);
                         --commit; 
                         EXECUTE IMMEDIATE v_qry ;
                                   
                        End If;
                      
                    If v_hierarchy_id is NULL THen
                        v_hierarchy_id := 'NULL' ;
                    End IF;  
                    
                    IF v_human_pos_type is NULL Then
                        v_human_pos_type := '0' ;
                    End If ;
                    
                    v_qry :=  v_insert_qry || ' select SEQ_TA_ACHIEVEMENT.nextval, ' || v_target_type_id || ',' ||
                    v_ta_param_id || ',' || v_Array('PARTY_ID') || ',NVL(' ||  v_array('ACHIEVEMENT') || ',0) as ACHVIEMENT,''' ||
                    v_startdt || ''',sysdate,' || '''Admin''' ||','||
                    '''N'''  || ',' || v_hierarchy_id ||  ',' || v_human_pos_type 
                    ||  ',' || v_array('ORG_PARTY_ID') || ',''' ||
                     v_EndDt || '''' || ' FROM ' || v_table_nm ;
                    
                    ELSE
                    
                        IF v_target_type_id = 1 Then
                        
                        v_hierarchy_id :=GET_HIERARCHY_ID(v_hierarchy_grp_id) ;
                        
                        
                        -- Delete Geo Achievement 
                         v_qry := 'delete from ta_achievement where   ' ||
                          '  achievement_period= ''' || v_startdt ||  ''' and  TA_PARAMETER_ID = ' || v_ta_param_id ||
                         -- ' and  HIERARCHY_ID = ' || v_hierarchy_id ||
                          ' and target_type_id= 1 and PARTY_ID IN ( ' ||
                          ' select PARTY_ID from temp_ta_table where TA_TARGET_DEF_HDR_ID = ' || I.TA_TARGET_DEF_HDR_ID || ' and
                            TARGET_TYPE_ID = ' || v_target_type_id || ' and TA_PARAMETER_ID = ' || v_ta_param_id ||
                           ' and END_DATE = ''' || v_EndDt || ''' and  START_DATE = ''' ||v_startdt || ''')' ;
                         
                        -- insert into tmp_test(txt,LOGV,datetime) values ('5',v_qry,systimestamp);
                         --commit; 
                         EXECUTE IMMEDIATE v_qry ;
                         
                        -- Delete Human Achivement
                        Elsif v_target_type_id = 2 Then
                         v_qry := 'delete from ta_achievement where   ' ||
                          '  achievement_period= ''' || v_startdt ||  ''' and  TA_PARAMETER_ID = ' || v_ta_param_id ||
                          ' and HUMAN_JOB_POSITION_TYP_ID = ' || v_human_pos_type ||
                          ' and target_type_id= 2 and PARTY_ID IN ( ' || 
                          ' select PARTY_ID from temp_ta_table where TA_TARGET_DEF_HDR_ID = ' || I.TA_TARGET_DEF_HDR_ID || ' and
                            TARGET_TYPE_ID = ' || v_target_type_id || ' and TA_PARAMETER_ID = ' || v_ta_param_id ||
                           ' and END_DATE = ''' || v_EndDt || ''' and  START_DATE = ''' ||v_startdt || ''')' ;
                   
                         
                        -- insert into tmp_test(txt,LOGV,datetime) values ('6',v_qry,systimestamp);
                         --commit; 
                         EXECUTE IMMEDIATE v_qry ;
                                   
                        End If;
                    
                    
                    v_qry :=  v_insert_qry || ' select SEQ_TA_ACHIEVEMENT.nextval,T1.TARGET_TYPE_ID,T1.TA_PARAMETER_ID, T1.PARTY_ID, NVL(T2.' ||  v_array('ACHIEVEMENT') || 
                    ',0) as ACHIEVEMENT,''' ||  v_startdt || ''',sysdate,''Admin'',''N'', T1.GEO_HIERARCHY_GRP_ID, T1.HUMAN_POS_TYPE_ID, T1.ORG_PARTY_ID,''' ||v_EndDt || ''' FROM
                    temp_ta_table T1 LEFT JOIN ' || v_table_nm || ' T2 ON T2.' ||   v_Array('PARTY_ID') || '=  T1.PARTY_ID ' ;
                  
                    
                    v_qry :=  v_qry || ' where T1.TA_TARGET_DEF_HDR_ID = ' || I.TA_TARGET_DEF_HDR_ID || ' and
                    T1.TARGET_TYPE_ID = ' || v_target_type_id || ' and T1.TA_PARAMETER_ID = ' || v_ta_param_id || 
                    ' and T1.END_DATE = ''' || v_EndDt || ''' and  T1.START_DATE =''' || v_startdt || '''';   
                     
                     
                    END IF ;
                     
                     
                    --insert into tmp_test(txt,LOGV,datetime) values ('7',v_qry,systimestamp);
                   -- commit;
                    
                    EXECUTE IMMEDIATE v_qry ;                                        
                    
                    
                  
                END LOOP;            
        
            
        END LOOP ;
    
    END LOOP ;
    
   
     
            
    
   
  
 END ACHIEVEMENT_CALC;
 
 function getUploadFor(DateVal DATE, p_TA_FREQUENCY_NM adm_ta_frequency.TA_FREQUENCY_NM%type,p_mon ta_target_def_hdr.target_start_month%type
 ) return Varchar2 As
 V_return Varchar2(20) ;
 
 Begin
        
     If   UPPER(p_TA_FREQUENCY_NM)  = 'MONTHLY' Then
     
         select to_char(DateVal,'mon') into  V_return from dual;
      
     elsif UPPER(p_TA_FREQUENCY_NM)  = 'QUARTERLY' Then
        
      select case when DateVal <= add_months(to_date('01-'||p_mon||to_char(DateVal,'yyyy'),'dd/mm/yyyy'),3) Then 'Q1' 
         when DateVal <= add_months(to_date('01-'||p_mon||to_char(DateVal,'yyyy'),'dd/mm/yyyy'),6) Then 'Q2'
         when DateVal <= add_months(to_date('01-'||p_mon||to_char(DateVal,'yyyy'),'dd/mm/yyyy'),9) Then 'Q3'
         else'Q4' End
         into v_return
         from dual ;
        
      
      
        
     Elsif UPPER(p_TA_FREQUENCY_NM)  = 'HALFYEARLY' or  UPPER(p_TA_FREQUENCY_NM)  like 'HALF%' Then
       select case when DateVal <= add_months(to_date('01-'||p_mon||to_char(DateVal,'yyyy'),'dd/mm/yyyy'),6) 
            Then 'HalfYearly1' 
         Else 'HalfYearly2'
       end into  V_return from dual  ; 
     Else
         V_return:= 'Yearly' ;
     End If;
     return  v_return ;
 
 End ;
 
 function getUploadPeriod( v_date DATE, v_uploadType Varchar2, v_target_start_month Varchar2) return varchar2 AS
 v_ta_month number;
 v_cnt number ;
 v_return varchar2(10) ;
 Begin
 
 select to_char(to_Date('01-' || v_target_Start_month || '-2016'),'mm') into v_ta_month from dual ;
 
 IF UPPER(v_uploadType) = 'QUARTERLY' Then
 
     v_cnt := v_ta_month + 3 ;
     if v_cnt > 12 Then
       v_cnt := v_cnt - 12 ;
     ENd If;
     
     If to_number(to_char(v_Date,'mm')) <= case when (v_ta_month + 2) > 12 Then ( (v_ta_month + 2) - 12 ) Else v_ta_month + 2 End
     Then v_return := 'Q1' ;
     Elsif to_number(to_char(v_Date,'mm')) <= case when (v_ta_month +5) > 12 Then ( (v_ta_month + 5) - 12 ) Else v_ta_month + 5 End
     Then v_return := 'Q2' ;
     Elsif to_number(to_char(v_Date,'mm')) <= case when (v_ta_month + 8) > 12 Then ( (v_ta_month + 8) - 12 ) Else v_ta_month + 8 End
     Then v_return := 'Q3' ;
     Elsif to_number(to_char(v_Date,'mm')) <= case when (v_ta_month + 11) > 12 Then ( (v_ta_month + 11) - 12 ) Else v_ta_month + 11 End
     Then v_return := 'Q4' ;
     End If;
 
 End If ;
 
 return v_return ; 
 End;
 
 function getTicklerDates(p_last_run_date DATE, p_current_date DATE, p_target_Start_month Varchar2, p_frequency Varchar2) return cursor_type
 As
 p_cur CURSOR_TYPE;
 v_target_Start_dt DATE ;
 v_qry CLOB ;
 v_no NUMBER ;
 Begin
 
 select to_Date('01-'|| p_target_Start_month|| '-' || to_char(p_last_run_date,'YYYY'), 'dd/mm/yyyy') into v_target_start_dt from dual;
 
 If UPPER(p_frequency) = 'QUARTERLY' Then
 
 v_no := to_number(to_char(v_target_Start_Dt, 'mm')) ;
 If v_no = 1 Then
    v_no := 0 ;
 Else
    v_no:= 13 - v_no ;
 End If ;    
 
 Open p_cur for
 SELECT  ADD_MONTHS( ADD_MONTHS(TRUNC(ADD_MONTHS(PARAM.start_date,v_no), 'Q'),-v_no), 3*(LEVEL-1) )   AS StartDate
    ,   ADD_MONTHS( ADD_MONTHS(TRUNC(ADD_MONTHS(PARAM.start_date,v_no), 'Q'),-v_no), 3*(LEVEL) ) -1  AS EndDate
    FROM    (   SELECT  p_last_run_date  AS start_date
                ,       p_current_date   AS end_date
            FROM    DUAL
        ) PARAM
    CONNECT BY ADD_MONTHS( ADD_MONTHS(TRUNC(ADD_MONTHS(PARAM.start_date,v_no), 'Q'),-v_no), 3*(LEVEL-1) ) 
        <= PARAM.end_date ; 
        
 Elsif UPPER(p_frequency) = 'HALFYEARLY' OR UPPER(p_frequency) like '%HALF%' Then
 
 Open p_cur for
     select * from (
      select 
      ADD_MONTHS(v_target_Start_Dt,6*(LEVEL-1)) StartDate,
      ADD_MONTHS(v_target_Start_Dt,6*(LEVEL))-1  EndDate
       from
      (   SELECT  p_last_run_date  AS start_date
                    ,  p_current_date   AS end_date
                FROM    DUAL
            ) PARAM
            connect by ADD_MONTHS(v_target_Start_Dt,6*(LEVEL-1)) < PARAM.end_Date
            ) t where t.EndDate >=p_last_run_date ; 
            
 Elsif UPPER(p_frequency) = 'MONTHLY' THEN
 
  Open p_cur for 
        select ADD_MONTHS(param.start_date,(LEVEL-1)) StartDate,
        ADD_MONTHS(param.start_date,LEVEL)-1 as ENDDATE from           
        (select p_last_run_date as start_Date,
        p_current_date as end_Date from dual) PARAM
        connect by   ADD_MONTHS(param.start_date,(LEVEL-1)) <= PARAM.end_date  ;            
                
 Elsif UPPER(p_Frequency) = 'YEARLY' THEN
 
 Open p_cur for
     select * from (
      select 
      ADD_MONTHS(v_target_Start_Dt,12*(LEVEL-1)) StartDate,
      ADD_MONTHS(v_target_Start_Dt,12*(LEVEL))-1  EndDate
       from
      (   SELECT  p_last_run_date  AS start_date
                    ,  p_current_date   AS end_date
                FROM    DUAL
            ) PARAM
            connect by ADD_MONTHS(v_target_Start_Dt,12*(LEVEL-1)) < PARAM.end_Date
            ) t where t.EndDate >=p_last_run_date ; 
 
 End If;    
 
 return p_cur ;
 End;
 
 function calc_start_date(
p_ta_frequency_id adm_ta_frequency.ta_frequency_id%type,
p_target_start_month ta_target_def_hdr.target_start_month%type,
p_uploadfor target_parameter_upload_hdr.uploadfor%type,
p_fin_year target_parameter_upload_hdr.fin_year%type) 
return date
as
start_date date;
v_tmp varchar2(10);
begin
    
    if(p_ta_frequency_id = 4 ) then
        start_date := to_date(p_uploadfor||'-'||p_fin_year , 'mon-yyyy');
        return start_date;
    elsif(p_ta_frequency_id = 3 ) then
       -- start_date := add_months(to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy'), (to_number(substr(p_uploadfor,2))-1)*3);
       If UPPER(p_uploadfor) = 'Q1' THEN
            start_date :=to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy');
        Elsif  UPPER(p_uploadfor) = 'Q2' THEN
           start_date := add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),3);
        Elsif  UPPER(p_uploadfor) = 'Q3' THEN
            start_date := add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),6);
        Elsif  UPPER(p_uploadfor) = 'Q4' THEN
            start_date := add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),9);
        End If;
        return start_date;
    elsif(p_ta_frequency_id = 2 ) then -- Half Yearly
       -- start_date := add_months(to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy'), (to_number(substr(p_uploadfor,12))-1)*6);
        If UPPER(p_uploadfor) = 'HALFYEARLY1' or  UPPER(p_uploadfor) like 'HALF%1' Then
        start_date := to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy');
        Else
         start_date := add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),6);
        End IF; 
        return start_date;
    elsif(p_ta_frequency_id = 1 ) then
        start_date :=  to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy');
        return start_date;
     end if;
    

end;
function calc_end_date(
p_ta_frequency_id adm_ta_frequency.ta_frequency_id%type,
p_target_start_month ta_target_def_hdr.target_start_month%type,
p_uploadfor target_parameter_upload_hdr.uploadfor%type,
p_fin_year target_parameter_upload_hdr.fin_year%type) 
return date
as
end_date date;
v_tmp varchar2(10);
v_month Varchar2(20);
begin
    
    if(p_ta_frequency_id =1 ) then -- Yearly
       end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),11));
        return end_date;
    elsif(p_ta_frequency_id = 3 ) then -- Quarterly
        If UPPER(p_uploadfor) = 'Q1' THEN
            end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),3));
        Elsif  UPPER(p_uploadfor) = 'Q2' THEN
            end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),6));
        Elsif  UPPER(p_uploadfor) = 'Q3' THEN
            end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),9));
        Elsif  UPPER(p_uploadfor) = 'Q4' THEN
            end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),11));
        End If;
        return end_date;
    elsif(p_ta_frequency_id = 2 ) then  -- Half Yearly
       If UPPER(p_uploadfor) = 'HALFYEARLY1' or  UPPER(p_uploadfor) like 'HALF%1' Then
        end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),6));
       Else
         end_date := last_day(add_months((to_date(p_target_start_month||'-'||p_fin_year , 'mon-yyyy')),11));
       End IF;  
        return end_date;
    elsif(p_ta_frequency_id = 4 ) then -- Monthly
       -- select to_char(sysdate,'month') into v_month from dual ;
        end_date :=  last_day(to_date(p_uploadfor||'-'||p_fin_year , 'mon-yyyy'));
       
        return end_date;
     end if;
end;  

 
 
-- PROCEDURE ACHIEVEMENT_CALC
-- is
--    v_array ta_parameters;
--    v_sql clob;
--    v_cursor_id integer;
--    v_col_cnt integer;
--    v_columns dbms_sql.desc_tab;
--    colVal varchar2(4000);
--    rc number;
--    v_rule_qry_id number;
--    v_party_id number;
--    v_process_log_id number;
--    v_start varchar2(100);
--    v_enddate varchar2(100);
--    v_entity_typ varchar2(100);
--    v_achieve_max_id number;
--    v_achievement varchar2(4000);
--    v_hierarchy_id varchar2(100);
--     v_mssg varchar2(4000);
--     v_log_param clob DEFAULT '';
--     P_CUR CURSOR_TYPE ;
--    begin
--   -- select (PROP_VALUE) into v_sql from ta_property where is_deleted_ind='N' and prop_nm='TA_QUERY';
--   
-- 
--   
----  delete from tmp_ci_trans ;
----   
----  insert into tmp_ci_trans
----select * from CI_PMS_PREMIUM_TRANS where channel_type = 'AG' and CASHIERED_DATE between 
----to_Date('01/01/2015','dd/mm/yyyy') and to_Date('01/01/2016','dd/mm/rrrr') ;
---- commit;
---- 
----  delete from  tmp_ci2 ;
----  
----insert into tmp_ci2 
----select sum(SUM_ASSURED) NB, cms_code from  tmp_ci_trans
----group by   cms_code;
----commit;
--   
--    v_sql := 'select distinct pid.PARTY_ID human_Or_Geo_Party_Id,
--''Quarterly'' as review_freq,''JAN'' target_start_month,
--''2015'' fin_year,''Q1''  as UploadFor,
--1 as query_id,
--''3'' as ta_parameter_id,''3'' as target_type_id,''IA''  as entity_type, identifier_value as cms_code
--from Tmp_CI_Trans pi 
--inner join party_identifier pid on pid.identifier_value= pi.cms_code and pid.IDENTIFIER_TYP_ID=1   ' ;

--    v_cursor_id := dbms_sql.open_cursor;
--    dbms_sql.parse(v_cursor_id, v_sql, dbms_sql.native);
--    dbms_sql.describe_columns(v_cursor_id, v_col_cnt, v_columns);

--    if(v_columns.count >1) then
--    for i in 1 .. v_columns.count loop
--         DBMS_SQL.define_column( v_cursor_id, i, colVal, 4000 );
--    end loop;
--    rc := DBMS_SQL.execute_and_fetch(v_cursor_id );

--    if(rc >0) then
--    loop
--    v_log_param :='';
--        for j in 1..v_col_cnt loop
--            DBMS_SQL.column_value( v_cursor_id, j, colVal );
--            v_array(v_columns(j).col_name):=colVal;
--            v_log_param := v_log_param|| v_columns(j).col_name ||':' ||colVal ||'~';
--        end loop;
--        v_rule_qry_id :=v_array(upper('QUERY_ID'));
--        v_entity_typ:= v_array(upper('entity_type'));
--        v_party_id :=v_array(upper('human_Or_Geo_Party_Id'));


--        select to_char(to_date(v_array(upper('target_start_month'))||'-'||v_array(upper('fin_year')), 'mon-yyyy'),'dd/MM/rrrr') into v_start from dual ;
--        v_enddate :=to_char(last_day(to_date(v_array(upper('target_start_month'))||'-'||v_array(upper('fin_year')), 'mon-yyyy')),'dd/MM/rrrr');
--        v_array(upper('startDate')):=v_start;
--        v_array(upper('endDate')):=v_enddate;

--        
--   
--        v_achievement :=  GET_ACHIEVE_VAL(v_array,v_rule_qry_id);
--          
--        
--       
--        v_array(upper('achievement')):=v_achievement;
--        v_achieve_max_id :=GET_ACHIEVE_ID(v_array,v_hierarchy_id);

--        
--            v_achieve_max_id:=SEQ_TA_ACHIEVEMENT.nextval;
--            insert into TA_ACHIEVEMENT(ta_achievement_id,target_type_id,ta_parameter_id,party_id,achievement,achievement_period,lst_updated_dtm,lst_updt_userid,is_deleted_ind,hierarchy_id) values(v_achieve_max_id, v_array(upper('target_type_id')),v_array(upper('ta_parameter_id')),v_party_id,v_achievement,TO_TA_DATE(v_start),sysdate,'admin','N',TO_TA_NUMBER(v_hierarchy_id));
--            commit;        
--            
--        exit when DBMS_SQL.fetch_rows(  v_cursor_id ) = 0;
--        v_array.delete();
--       
--   
--    end loop;
--    end if;
--    end if;
--    dbms_sql.close_cursor(v_cursor_id);
--    commit;
----exception when others then
----    if(v_cursor_id is not null) then
----        dbms_sql.close_cursor(v_cursor_id);
----    end if;
----    rollback;
----    v_process_log_id :=TA_PROCESS_LOG_SEQ.nextval;
----    v_mssg :=SUBSTR(SQLERRM, 1, 200);
----   insert into TA_PROCESS_LOG(TA_PROCESS_LOG_ID,MESSAGE,TA_RULE_QUERY_ID,CASE_ID,IS_DELETED_IND,LST_UPDT_USRID,LST_UPDT_DTM,PARAM) values (v_process_log_id,v_mssg,v_rule_qry_id,v_party_id,'N','Admin',sysdate, v_log_param );
----   commit;
----    raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM,true);
--end;
END TA_PROCESS;
/
