select UsrPaySystemId from Lead
select UsrPaySystemId from Contact
select UsrPaySystemId from Opportunity

select count(*) from Lead where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')
select count(*) from Contact where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')
select count(*) from Opportunity where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')

update Lead set UsrPaySystemId = '53504eb6-2729-4156-8fde-3eee5fdae3e5' where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')
update Contact set UsrPaySystemId = '53504eb6-2729-4156-8fde-3eee5fdae3e5' where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')
update Opportunity set UsrPaySystemId = '53504eb6-2729-4156-8fde-3eee5fdae3e5' where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')
update UsrPaymentInContact set UsrPaySystemId = '53504eb6-2729-4156-8fde-3eee5fdae3e5' where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')
update UsrLeadImport set UsrPaySystemId = '53504eb6-2729-4156-8fde-3eee5fdae3e5' where UsrPaySystemId  IN (select Id from UsrPaySystem where Name like '%CB%')

select Id from UsrPaySystem where Name like '%CB%'
select * from UsrPaySystem where Name = 'CB'
delete from UsrPaySystem  where Name like '%CB%' and  Name != 'CB'
