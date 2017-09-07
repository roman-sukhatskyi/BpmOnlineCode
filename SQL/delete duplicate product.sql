DECLARE @ErrorMessage nvarchar(max)	
DECLARE @c bigint;
BEGIN TRY
	BEGIN TRAN
  	BEGIN
		delete from Product where id in (
			select t.Id from (  
					select ROW_NUMBER ( ) OVER ( PARTITION BY p.Code order by id)  as d, p.id
					from Product as p
					where not exists (select * from [OrderProduct] as o where o.ProductId = p.id)  
				
			) as t
			where t.d > 1
		)
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertContactFromFtp', 'ok')
	END
	COMMIT
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK
	SELECT ERROR_MESSAGE() AS ErrorMessage;
	SET @ErrorMessage = ERROR_MESSAGE()
	INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertContactFromFtp', @ErrorMessage)
END CATCH

