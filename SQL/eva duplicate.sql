--��������� ��� �������� �� � ������� � ������� ������ �� Case

select t.d from (
	select ROW_NUMBER ( ) OVER ( PARTITION BY c.UsrMemberIdPL order by id)   as d
	from Contact as c
	where not exists (select * from [Case] as c1 where c1.ContactId = c.id) 
) as t
where t.d > 1
--��������� ������ 1 
delete t from (
	select ROW_NUMBER ( ) OVER ( PARTITION BY c.UsrMemberIdPL order by id)   as d
	from Contact as c
	where not exists (select * from [Case] as c1 where c1.ContactId = c.id) 
) as t
where t.d > 1
--��������� ������ 2 
with a as(
	select t.d from (
		select ROW_NUMBER ( ) OVER ( PARTITION BY c.UsrMemberIdPL order by id)   as d
		from Contact as c
		where not exists (select * from [Case] as c1 where c1.ContactId = c.id) 
	) as t
	where t.d > 1
)
delete from a 
--��������� ��� �������� � ���� ���� ������ �� ���������
select c.Name, c.UsrMemberIdPL
from Contact as c
where not exists (select * from [Case] as c1 where c1.ContactId = c.id) 
--���������
with a as(
	select c.Name, c.UsrMemberIdPL
	from Contact as c
	where not exists (select * from [Case] as c1 where c1.ContactId = c.id)
) 
delete from a 

