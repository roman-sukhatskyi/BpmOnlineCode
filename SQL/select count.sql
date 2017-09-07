DECLARE @ErrorMessage nvarchar(max)	
DECLARE @c bigint;
BEGIN TRY
	BEGIN TRAN
  	BEGIN
  		select @c = count(*) from (
			select t.d, t.Id from (
					select ROW_NUMBER ( ) OVER ( PARTITION BY c.UsrMemberIdPL order by id)   as d, c.id
					from Contact as c
					where not exists (select * from [Case] as c1 where c1.ContactId = c.id)  and 
				not exists (select * from SysAdminUnit as s where s.ContactId = c.id)
				) as t
				where t.d > 1
		) as tab;
		
		--delete from Contact where id in (select id from a)
		--delete top(1) from a 
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertContactFromFtp', @c)
	END
	COMMIT
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK
	SELECT ERROR_MESSAGE() AS ErrorMessage;
	SET @ErrorMessage = ERROR_MESSAGE()
	INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertContactFromFtp', @ErrorMessage)
END CATCH