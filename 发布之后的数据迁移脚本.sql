-- �����û��ع�����ʱ��
create table tmp_sec_risk_rollback
(
	user_id varchar2(16),
	action varchar2(16),
	gmt_create timestamp,
	gmt_modified timestamp
);
alter table tmp_sec_risk_rollback add constraint tmp_sec_risk_rollback_pk primary key (user_id) using index;

-- ִ�ж�������Ǩ�ƵĴ洢����
create or replace procedure PC_SEC_RISK_DATA_MIG(begintime in varchar2,endtime in varchar2) is
 type csr_type is ref Cursor; -- �����αꡣ
   cur_csr csr_type;
   sqlStr varchar2(500); -- ���嶯̬sql��
   old_rec_info cs_security_risk%rowtype; -- �����ݱ��еļ�¼��
   new_rec_info cs_new_security_risk%rowtype; -- �����ݱ��еļ�¼��
   rec_count int;
      
   Begin
	   sqlStr:='select user_id,security_risk_type,has_modify_pwd from cs_security_risk where gmt_modified > '||CHR(39)||to_timestamp(begintime,'yyyy-mm-dd hh24:mi:ss.ff9')||CHR(39)||' and gmt_modified < '||CHR(39)||to_timestamp(endtime,'yyyy-mm-dd hh24:mi:ss.ff9')||CHR(39);
	   open cur_csr for sqlStr;
	   loop
		fetch  cur_csr into old_rec_info.user_id,old_rec_info.security_risk_type,old_rec_info.has_modify_pwd;
		exit when cur_csr%notfound;
		
		-- �ж��±����Ƿ����ָ��user_id�ļ�¼
    select count(*) into rec_count from cs_new_security_risk where user_id = old_rec_info.user_id;
    -- ����±��в�����ָ���ļ�¼
    if rec_count = 0 then
    		-- �ж��ϱ��е������Ƿ��������������
				-- 1. security_risk_type�ֶ�SU����ST��
				-- 2. has_modify_pwd�ֶ�Ϊ0��
				if (old_rec_info.security_risk_type = 'ST' or old_rec_info.security_risk_type = 'SU') and old_rec_info.has_modify_pwd = '0' then
					-- �������������д���±���
					insert into cs_new_security_risk
						(user_id,risk_flag,gmt_create,gmt_modified)
					values
						(old_rec_info.user_id,'ST',sysdate,sysdate);
					-- ������д��tmp_sec_risk_rollback����(����ADD�����ڻع�ʱֱ��ɾ������˲��ü�¼ʱ��)
					insert into tmp_sec_risk_rollback
						(user_id,action)
					values
						(old_rec_info.user_id,'ADD');
				end if;
  
        else
      		select user_id,gmt_create,gmt_modified into new_rec_info.user_id,new_rec_info.gmt_create,new_rec_info.gmt_modified from cs_new_security_risk where user_id = old_rec_info.user_id;
      		if new_rec_info.user_id = old_rec_info.user_id then
      			begin
      				-- �ж��ϱ��е�has_modify_pwd�ֶ��Ƿ�Ϊ1
      				if old_rec_info.has_modify_pwd = '1' then
      					-- ���Ϊ1�������ݴ��±���ɾ��
      					delete from cs_new_security_risk where user_id = old_rec_info.user_id;
      					-- ������д��tmp_sec_risk_rollback����
      					insert into tmp_sec_risk_rollback
      						(user_id,action,gmt_create,gmt_modified)
      					values
      						(old_rec_info.user_id,'DEL',new_rec_info.gmt_create,new_rec_info.gmt_modified);
      				end if;
      			end;
      		end if;
        end if;
    end loop;
    close cur_csr;
   End PC_SEC_RISK_DATA_MIG;
   
   
-- �����ʱ��Ļع��ű�
declare 
        type cur_type is ref Cursor;
        cur_csr cur_type;
        sqlStr varchar2(500); 
        rec_info tmp_sec_risk_rollback%rowtype;
		rec_num_count number(11):=0;
Begin 
      sqlStr:='select user_id,action,gmt_create,gmt_modified from tmp_sec_risk_rollback';
      open cur_csr for sqlStr;
      loop
          fetch  cur_csr into rec_info.user_id,rec_info.action,rec_info.gmt_create,rec_info.gmt_modified;
          exit when cur_csr%notfound;
         
          if(rec_info.action='DEL') then
                insert into cs_new_security_risk
                       (user_id,risk_flag,gmt_create,gmt_modified)
                values
                      (rec_info.user_id,'ST',rec_info.gmt_create,rec_info.gmt_modified);
		  elsif(rec_info.action='ADD') then
				delete from cs_new_security_risk where user_id = rec_info.user_id;
          end if;
      end loop;
close cur_csr;             
end;
	
	