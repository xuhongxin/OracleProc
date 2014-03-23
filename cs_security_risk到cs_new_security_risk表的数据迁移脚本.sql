-- 创建临时表
create table tmp_cs_new_security_risk as select * from cs_new_security_risk;

-- 执行数据迁移
declare 
        type cur_type is ref Cursor;
        cur_csr cur_type;
        sqlStr varchar2(500); 
        rec_info cs_security_risk%rowtype;
		rec_num_count number(11):=0;
Begin 
      sqlStr:='select user_id,security_risk_type,has_modify_pwd from cs_security_risk';
      open cur_csr for sqlStr;
      loop
          fetch  cur_csr into rec_info.user_id,rec_info.security_risk_type,rec_info.has_modify_pwd;
          exit when cur_csr%notfound;
         
          if((rec_info.security_risk_type='ST' or rec_info.security_risk_type='SU')and rec_info.has_modify_pwd=0) then
                insert into cs_new_security_risk
                       (user_id,risk_flag,gmt_create,gmt_modified)
                values
                      (rec_info.user_id,'ST',sysdate,sysdate);
				rec_num_count:=rec_num_count+1;
          end if;
		  
		  if(mod(rec_num_count,200)=0) then 
			commit;
		  end if;
		  
      end loop;
close cur_csr;             
end;

-- 从临时表回滚数据
insert into cs_new_security_risk select * from tmp_cs_new_security_risk;















declare 
        type cur_type is ref Cursor;
        cur_csr cur_type;
        sqlStr varchar2(500); 
        rec_info cs_security_risk%rowtype;
Begin 
      sqlStr:='select user_id,security_risk_type from cs_security_risk';
      open cur_csr for sqlStr;
      loop
          fetch  cur_csr into rec_info.user_id,rec_info.security_risk_type;
          exit when cur_csr%notfound;
          
          Dbms_Output.put_line(rec_info.security_risk_type||','||rec_info.user_id);
      end loop;
close cur_csr;             
      
      
end;






















declare 
        type cur_type is ref Cursor;
        cur_csr cur_type;
        sqlStr varchar2(500); 
        rec_info cs_security_risk%rowtype;
Begin 
      sqlStr:='select user_id,security_risk_type,has_modify_pwd from cs_security_risk';
      open cur_csr for sqlStr;
      loop
          fetch  cur_csr into rec_info.user_id,rec_info.security_risk_type,rec_info.has_modify_pwd;
          exit when cur_csr%notfound;
         
                insert into cs_new_security_risk
                       (user_id,risk_flag,gmt_create,gmt_modified)
                values
                      (rec_info.user_id,'ST',sysdate,sysdate);
      end loop;
close cur_csr;             
end;























declare 
        type cur_type is ref Cursor;
        cur_csr cur_type;
        sqlStr varchar2(500); 
        rec_info cs_security_risk%rowtype;
Begin 
      sqlStr:='select user_id,security_risk_type,has_modify_pwd from cs_security_risk';
      open cur_csr for sqlStr;
      loop
          fetch  cur_csr into rec_info.user_id,rec_info.security_risk_type,rec_info.has_modify_pwd;
          exit when cur_csr%notfound;
         
          if((rec_info.security_risk_type='ST' or rec_info.security_risk_type='SU')and rec_info.has_modify_pwd=0) then
                insert into cs_new_security_risk
                       (user_id,risk_flag,gmt_create,gmt_modified)
                values
                      (rec_info.user_id,'ST',sysdate,sysdate);
          end if;
      end loop;
close cur_csr;             
end;