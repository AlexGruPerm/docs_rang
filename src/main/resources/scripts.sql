create user npa_docs
  identified by npa_docs
  default tablespace USERS
  temporary tablespace TEMP
  profile DEFAULT
  quota unlimited on users;
grant connect to npa_docs;
grant exp_full_database to npa_docs;
grant imp_full_database to npa_docs;
grant resource to npa_docs;
grant create any directory to npa_docs;
grant create any procedure to npa_docs;
grant create any synonym to npa_docs;
grant create any table to npa_docs;
grant create procedure to npa_docs;
grant create sequence to npa_docs;
grant create table to npa_docs;
grant create trigger to npa_docs;
grant delete any table to npa_docs;
grant drop any directory to npa_docs;
grant execute any procedure to npa_docs;
grant execute any type to npa_docs;
grant flashback any table to npa_docs;
grant flashback archive administer to npa_docs;
grant insert any table to npa_docs;
grant select any dictionary to npa_docs;
grant select any table to npa_docs with admin option;
grant unlimited tablespace to npa_docs;
grant update any table to npa_docs;
alter user npa_docs
  default role connect, resource;


clear;
drop table docs;
drop table dict_dimension;
drop table doc_dim_count;
drop table dep; //департамент

create table dep(
id      integer,
s_name  varchar2(256)
);

begin
  insert into dep values(1,'Деп Культ');
  insert into dep values(2,'Деп Здрав');
  commit;
end;
/

create table docs(
id      integer,
content clob,
id_dep  integer --распределение от пользователя, исп. для обучения.
);

create table dict_dimension(
id_dim  integer not null,
word    varchar2(64) not null
);

-- ID = 1 Всегда зарезервирован для департамента, задается пользователем.
begin
  insert into dict_dimension values(2,'товар');
  insert into dict_dimension values(2,'товары');
  insert into dict_dimension values(2,'товаров');
  insert into dict_dimension values(2,'товарами');

  insert into dict_dimension values(3,'закон');
  insert into dict_dimension values(3,'законы');
  insert into dict_dimension values(3,'законов');
  insert into dict_dimension values(3,'законом');

  commit;
end;
/

create table doc_dim_count(
  id_doc integer not null,
  id_dim integer not null,
  cnt    integer not null
);