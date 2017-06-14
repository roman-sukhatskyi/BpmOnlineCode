IF OBJECT_ID('tsp_UploadFromFtp', 'P') IS NOT NULL
	DROP PROCEDURE [dbo].[tsp_UploadFromFtp]
GO

CREATE PROCEDURE [dbo].[tsp_UploadFromFtp]
@PathToBayersFile VARCHAR(250),
@PathInActivCardsFile VARCHAR(250),
@PathOursFile VARCHAR(250),
@PathSheduleFile VARCHAR(250),
@PathProdsFile VARCHAR(250)
--@ReturnValue int OUTPUT,
--@Error nvarchar(max) OUTPUT
AS 
BEGIN
	DECLARE @charset NVARCHAR(250) = 'Cyrillic_General_CI_AI'
	TRUNCATE TABLE TempBuyersCsv
	TRUNCATE TABLE TempInActiveCardsCsv
	TRUNCATE TABLE TempOursCsv
	TRUNCATE TABLE TempScheduleCsv
	TRUNCATE TABLE TempProductCsv
BEGIN TRY
	--_________________________________________ INSERT Bayers from FTP _________________________________________
	IF NOT EXISTS(
		SELECT * FROM sys.objects WHERE Name = 'TempBuyersCsv'
	)
	BEGIN
		DECLARE @createTableBuyers nvarchar(2000) = N'
			CREATE TABLE TempBuyersCsv 
			(
				MemberIdPL NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Surname NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				GivenName NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', MiddleName  NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				Gender NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Birthday  NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				Phone NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Email NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				City NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Street NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				House NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Corps NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				Apartment NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', NumberActingCard NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				StatusWriteoffBonuses NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', StatusCard NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				DateRegistrationCard NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', CardType NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				BalanceTrade NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', AcceptanceEmailPl NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				AcceptanceSMSPl NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', SymptomParticipationClub NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				AcceptanceSMSKm NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', AcceptanceEmailKm NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', 
				WalletBalance NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +'
			)'
		EXEC (@createTableBuyers)
	END     

			DECLARE @ErrorMessage nvarchar(max)	

	BEGIN TRY
	  BEGIN TRAN
  		BEGIN
  			declare @sqlTextBayers nvarchar(1000) = N'
	  			BULK INSERT TempBuyersCsv
				FROM ' + ''''  + @PathToBayersFile + '''' +  '
				WITH
				(
					CODEPAGE = ''ACP'',
					FIRSTROW = 2,
					FIELDTERMINATOR = '';'',  
					ROWTERMINATOR = ''\n'',
					TABLOCK
				)
			';
			exec(@sqlTextBayers)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertContactFromFtp', @ErrorMessage)
	END CATCH
    
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO City (Id, Name)
			SELECT NEWID() AS Id, tmp.City AS Name
			FROM TempBuyersCsv AS tmp
			WHERE not tmp.City is null and not exists (SELECT 1 FROM City AS c WHERE c.Name = tmp.City)
			GROUP BY tmp.City
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationContactCityFromFtp', @ErrorMessage)
	END CATCH
	
	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert City', 'Success')
	END

	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			UPDATE c
				SET c.UsrMemberIdPl = myTable.MemberIdPL
			FROM Contact c
			INNER JOIN (SELECT t.MemberIdPL, t.ContactId FROM (
					SELECT c.Id AS ContactId, t.MemberIdPL AS MemberIdPL, ROW_NUMBER ( )  OVER (PARTITION BY t.MemberIdPL ORDER BY c.Id ) AS Pos 
					FROM 
						(
							SELECT c.id AS id, cc.SearchNumber AS SearchNumber
							FROM  Contact  AS c
							INNER JOIN ContactCommunication AS cc ON cc.ContactId = c.Id AND cc.SearchNumber != '' AND cc.CommunicationTypeId = 'D4A2DC80-30CA-DF11-9B2A-001D60E938C6'
							WHERE c.UsrMemberIdPL = ''
						) AS c
						LEFT JOIN 
						(
							SELECT  tmp.MemberIdPL AS MemberIdPL, [dbo].[fn_GetPhoneNumberSearchForm](tmp.Phone) AS ReversePhone
							FROM TempBuyersCsv AS tmp with(nolock)
							WHERE NOT tmp.MemberIdPL IN (SELECT UsrMemberIdPL FROM Contact)
						) AS t ON t.ReversePhone = c.SearchNumber
					WHERE NOT t.MemberIdPL IS NULL
				) t
				WHERE t.pos = 1
			) AS myTable ON myTable.ContactId = c.Id
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('UpdateContactDuplicateMobilePhone', @ErrorMessage)
	END CATCH
	
	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Update Contact duplicate MobilePhone', 'Success')
	END
	
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			UPDATE c SET c.Surname = IsNULL(TRY_CONVERT([nvarchar](250), tmp.Surname), ''),
				 c.GivenName = IsNULL(TRY_CONVERT([nvarchar](250), tmp.GivenName), ''),
				 c.MiddleName = IsNULL(TRY_CONVERT([nvarchar](250), tmp.MiddleName), ''),
				 c.Name = IsNULL(TRY_CONVERT([nvarchar](250), tmp.Surname + ' ' +  tmp.GivenName + ' ' + tmp.MiddleName), ''),
				 c.GenderId = g.Id,
				 c.BirthDate = IsNULL(TRY_CONVERT([datetime2], tmp.Birthday), null),
				 c.MobilePhone = IsNULL(TRY_CONVERT([nvarchar](250), tmp.Phone), ''),
				 c.Email = IsNULL(TRY_CONVERT([nvarchar](250), tmp.Email), ''),
				 c.CityId = ci.Id,
				 c.Address = IsNULL(TRY_CONVERT([nvarchar](500), tmp.Street), ''),
				 c.UsrHouse = IsNULL(TRY_CONVERT([nvarchar](50), tmp.House), ''),
				 c.UsrCorps = IsNULL(TRY_CONVERT([nvarchar](50), tmp.Corps), ''),
				 c.UsrApartment = IsNULL(TRY_CONVERT([nvarchar](50), tmp.Apartment), ''),
				 c.UsrNumberActiveCard = IsNULL(TRY_CONVERT([nvarchar](250), tmp.NumberActingCard), ''),
				 c.UsrStatusWriteoffBonusesId = swb.Id,
				 c.UsrStatusCardId = sc.Id,
				 c.UsrDateRegistrationCard = IsNULL(TRY_CONVERT([datetime2], tmp.DateRegistrationCard), null),
				 c.UsrCardTypeId = ct.Id,
				 c.UsrBalanceTrade =   IsNULL(TRY_CONVERT([decimal](20, 2), tmp.BalanceTrade), 0.00),
				 c.UsrAcceptanceEmailPlId = ase.Id,
				 c.UsrAcceptanceSMSPlId = ass.Id,
				 c.UsrSymptomParticipationClub = IsNULL(TRY_CONVERT([bit], tmp.SymptomParticipationClub), 0),
				 c.UsrAcceptanceSMSKmId =  aek.Id,
				 c.UsrAcceptanceEmailKmId = ask.Id,
				 c.UsrWalletBalance =  IsNULL(TRY_CONVERT([decimal](20, 2), tmp.WalletBalance), 0.00)
			FROM Contact AS c
				inner join TempBuyersCsv AS tmp ON c.UsrMemberIdPL = TRY_CONVERT([nvarchar], tmp.MemberIdPL)
				left join Gender AS g with (nolock) ON g.UsrCode = TRY_CONVERT([int], tmp.Gender) 
				left join City AS ci with (nolock) ON ci.Name = TRY_CONVERT([nvarchar], tmp.City)
				left join UsrStatusWriteoffBonuses AS swb with (nolock) ON swb.UsrCodeId = TRY_CONVERT([int], tmp.StatusWriteoffBonuses)
				left join UsrStatusCard AS sc with (nolock) ON sc.UsrCodeId = TRY_CONVERT([int], tmp.StatusCard)
				left join UsrCardType AS ct with (nolock) ON ct.UsrCodeId =  TRY_CONVERT([int], tmp.CardType)
				left join UsrAcceptanceSendingEmail AS ase with (nolock) ON ase.UsrCodeId =  TRY_CONVERT([int], tmp.AcceptanceEmailPl)
				left join UsrAcceptanceSendingSMS AS ass with (nolock) ON ass.UsrCodeId =  TRY_CONVERT([int], tmp.AcceptanceSMSPl)
				left join UsrAcceptanceSendingEmail AS ask with (nolock) ON ask.UsrCodeId = TRY_CONVERT([int], tmp.AcceptanceSMSKm)
				left join UsrAcceptanceSendingSMS AS aek with (nolock) ON aek.UsrCodeId = TRY_CONVERT([int], tmp.AcceptanceEmailKm)
			WHERE c.UsrMemberIdPL != '' AND
				  (c.Surname != IsNULL(TRY_CONVERT([nvarchar](250), tmp.Surname), '') OR
				  c.GivenName != IsNULL(TRY_CONVERT([nvarchar](250),tmp.GivenName), '') OR 
				  c.MiddleName != IsNULL(TRY_CONVERT([nvarchar](250), tmp.MiddleName), '') OR
				  c.GenderId != g.Id OR
				  c.BirthDate != IsNULL(TRY_CONVERT([datetime2], tmp.Birthday), null) OR
				  c.MobilePhone != IsNULL(TRY_CONVERT([nvarchar](250), tmp.Phone), '') OR
				  c.Email != IsNULL(TRY_CONVERT([nvarchar](250), tmp.Email), '') OR
				  c.CityId != ci.Id OR 
				  c.Address != IsNULL(TRY_CONVERT([nvarchar](500),tmp.Street), '') OR
				  c.UsrHouse != IsNULL(TRY_CONVERT([nvarchar](50), tmp.House), '') OR
				  c.UsrCorps != IsNULL(TRY_CONVERT([nvarchar](50), tmp.Corps), '') OR
				  c.UsrApartment !=  IsNULL(TRY_CONVERT([nvarchar](50),tmp.Apartment), '') OR
				  c.UsrNumberActiveCard !=  IsNULL(TRY_CONVERT([nvarchar](250),tmp.NumberActingCard), '') OR
				  c.UsrStatusWriteoffBonusesId != swb.Id OR 
				  c.UsrStatusCardId != sc.Id OR
				  c.UsrDateRegistrationCard != IsNULL(TRY_CONVERT([datetime2], tmp.DateRegistrationCard), null) OR 
				  c.UsrCardTypeId != ct.Id OR 
				  c.UsrBalanceTrade != IsNULL(TRY_CONVERT([decimal](20, 2), tmp.BalanceTrade), 0.00) OR
				  c.UsrAcceptanceEmailPlId != ase.Id OR
				  c.UsrAcceptanceSMSPlId != ass.Id OR 
				  c.UsrSymptomParticipationClub != IsNULL(TRY_CONVERT([bit], tmp.SymptomParticipationClub), 0) OR
				  c.UsrAcceptanceSMSKmId != aek.Id OR
				  c.UsrAcceptanceEmailKmId != ask.Id OR
				  c.UsrWalletBalance != IsNULL(TRY_CONVERT([decimal](20, 2), tmp.WalletBalance), 0.00))
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationUpdateContactFromFtp', @ErrorMessage)
	END CATCH
	
	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Update Contact', 'Success')
	END

	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO Contact (UsrMemberIdPL,
						Surname,
						GivenName,
						MiddleName,
						Name,
						GenderId,
						BirthDate,
						MobilePhone,
						Email,
						CityId,
						Address,
						UsrHouse,
						UsrCorps,
						UsrApartment,
						UsrNumberActiveCard,
						UsrStatusWriteoffBonusesId,
						UsrStatusCardId,
						UsrDateRegistrationCard,
						UsrCardTypeId,
						UsrBalanceTrade,
						UsrAcceptanceEmailPlId,
						UsrAcceptanceSMSPlId,
						UsrSymptomParticipationClub,
						UsrAcceptanceSMSKmId,
						UsrAcceptanceEmailKmId,
						UsrWalletBalance,
						CreatedById,
						TypeId)
			SELECT  
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.MemberIdPL), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.Surname), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.GivenName), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.MiddleName), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.Surname + ' ' +  tmp.GivenName + ' ' + tmp.MiddleName), ''),
				 g.Id,
				 IsNULL(TRY_CONVERT([datetime2], tmp.Birthday), null),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.Phone), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.Email), ''),
				 ci.Id,
				 IsNULL(TRY_CONVERT([nvarchar](500), tmp.Street), ''),
				 IsNULL(TRY_CONVERT([nvarchar](50), tmp.House), ''),
				 IsNULL(TRY_CONVERT([nvarchar](50), tmp.Corps), ''),
				 IsNULL(TRY_CONVERT([nvarchar](50), tmp.Apartment), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmp.NumberActingCard), ''),
				 swb.Id,
				 sc.Id,
				 IsNULL(TRY_CONVERT([datetime2], tmp.DateRegistrationCard), null),
				 ct.Id,
				 IsNULL(TRY_CONVERT([decimal](20, 2), tmp.BalanceTrade), 0.00),
				 ase.Id,
				 ass.Id, 
				 IsNULL(TRY_CONVERT([bit], tmp.SymptomParticipationClub), 0),
				 aek.Id,
				 ask.Id,
				 IsNULL(TRY_CONVERT([decimal](20, 2), tmp.WalletBalance), 0.00),
				 '410006E1-CA4E-4502-A9EC-E54D922D2C00',
				 '00783EF6-F36B-1410-A883-16D83CAB0980'
			FROM TempBuyersCsv AS tmp
				left join Gender AS g with (nolock) ON g.UsrCode = TRY_CONVERT([int], tmp.Gender) 
				left join City AS ci with (nolock) ON ci.Name = TRY_CONVERT([nvarchar], tmp.City)
				left join UsrStatusWriteoffBonuses AS swb with (nolock) ON swb.UsrCodeId = TRY_CONVERT([int], tmp.StatusWriteoffBonuses)
				left join UsrStatusCard AS sc with (nolock) ON sc.UsrCodeId = TRY_CONVERT([int], tmp.StatusCard)
				left join UsrCardType AS ct with (nolock) ON ct.UsrCodeId =  TRY_CONVERT([int], tmp.CardType)
				left join UsrAcceptanceSendingEmail AS ase with (nolock) ON ase.UsrCodeId =  TRY_CONVERT([int], tmp.AcceptanceEmailPl)
				left join UsrAcceptanceSendingSMS AS ass with (nolock) ON ass.UsrCodeId =  TRY_CONVERT([int], tmp.AcceptanceSMSPl)
				left join UsrAcceptanceSendingEmail AS ask with (nolock) ON ask.UsrCodeId = TRY_CONVERT([int], tmp.AcceptanceSMSKm)
				left join UsrAcceptanceSendingSMS AS aek with (nolock) ON aek.UsrCodeId = TRY_CONVERT([int], tmp.AcceptanceEmailKm)
			WHERE  ISNUMERIC(tmp.MemberIdPL) = 1 AND not exists (select 1 from Contact as c where c.UsrMemberIdPL = TRY_CONVERT([nvarchar], tmp.MemberIdPL))
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationInsertContactFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert Contact', 'Success')
	END

	BEGIN TRY
	  BEGIN TRAN
		BEGIN	
			INSERT INTO ContactCommunication (Number, CommunicationTypeId, ContactId)
			SELECT c.MobilePhone AS Number, 'D4A2DC80-30CA-DF11-9B2A-001D60E938C6' AS TypeId, c.Id AS ContactId 
			FROM Contact c
			WHERE not exists (SELECT TOP 1 Id FROM ContactCommunication cc WHERE cc.ContactId = c.Id and cc.Number = c.MobilePhone) and c.MobilePhone != '' 
			UNION ALL
			SELECT c.Email AS Number, 'EE1C85C3-CFCB-DF11-9B2A-001D60E938C6' AS TypeId, c.Id AS ContactId 
			FROM Contact c
			WHERE not exists (SELECT TOP 1 Id FROM ContactCommunication cc WHERE cc.ContactId = c.Id and cc.Number = c.Email) and c.Email != '' 
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationContactCommunicationFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert ContactCommunication', 'Success')
	END

	BEGIN	
		TRUNCATE TABLE TempBuyersCsv 
	END

	--_________________________________________ INSERT InActiveCards from FTP _________________________________________
	IF NOT EXISTS(
		SELECT * FROM sys.objects WHERE Name = 'TempInActiveCardsCsv'
	)
	BEGIN
		DECLARE @createTableInActivCards nvarchar(2000) = N'
		CREATE TABLE TempInActiveCardsCsv 
			(
				MemberIdPL NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', CardNumber NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +' 
			)'
		EXEC (@createTableInActivCards)
	END
	
	BEGIN TRY
	  BEGIN TRAN
  		BEGIN
			declare @sqlTextInActivCards nvarchar(1000) = N'
	  			BULK INSERT TempInActiveCardsCsv
				FROM ' + ''''  + @PathInActivCardsFile + '''' +  '
				WITH
				(
					CODEPAGE = ''ACP'',
					FIRSTROW = 2,
					FIELDTERMINATOR = '';'',  
					ROWTERMINATOR = ''\n'',
					TABLOCK
				)
			';
			exec(@sqlTextInActivCards)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulkInsertInActiveCardsFromFtp', @ErrorMessage)
	END CATCH
	-- Insert in temporary table #TempInActiveCardsCsv
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO UsrContactInactiveCard (UsrContactId, UsrCardNumber, UsrStatusCardId)
			SELECT 
				c.Id,
				IsNULL(TRY_CONVERT([nvarchar](250), tmpac.CardNumber), ''),
				'B2A488E9-83FC-4FE3-9FDA-7D9EB0E9F6DA'
			FROM TempInActiveCardsCsv AS tmpac
			INNER JOIN Contact AS c ON c.UsrMemberIdPL = TRY_CONVERT([nvarchar], tmpac.MemberIdPL)
			WHERE tmpac.MemberIdPL != 0  AND 
			NOT EXISTS(select top 1 * from UsrContactInactiveCard as ci where ci.UsrContactId = c.id AND ci.UsrCardNumber =  TRY_CONVERT([nvarchar], tmpac.CardNumber))
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationContactInactiveCardFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert UsrContactInactiveCard', 'Success')
	END

	BEGIN	
		TRUNCATE TABLE TempInActiveCardsCsv 
	END
	
	--_________________________________________ INSERT Ours from FTP _________________________________________
	IF NOT EXISTS(
		SELECT * FROM sys.objects WHERE Name = 'TempOursCsv'
	)
	BEGIN
		DECLARE @createTableOurs nvarchar(2000) = N'
			CREATE TABLE TempOursCsv 
			(
				Code NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Name NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				City NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Address NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				Address2 NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Phone NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				FIO NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Comment1 NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				Comment2 NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Region NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				Filial NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +'
			)'
		EXEC (@createTableOurs)
	END

	BEGIN TRY
	  BEGIN TRAN
  		BEGIN
  			declare @sqlTextOurs nvarchar(1000) = N'
	  			BULK INSERT TempOursCsv
				FROM ' + ''''  + @PathOursFile + '''' +  '
				WITH
				(
					CODEPAGE = ''ACP'',
					FIRSTROW = 2,
					FIELDTERMINATOR = '';'',  
					ROWTERMINATOR = ''\n'',
					TABLOCK
				)
			';
			exec(@sqlTextOurs)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulkInsertOursFromFtp', @ErrorMessage)
	END CATCH

	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO City (Id, Name)
			SELECT NEWID() AS Id, tmpo.City AS Name
			FROM TempOursCsv AS tmpo
			WHERE not tmpo.City is null and not exists (SELECT 1 FROM City AS c WHERE c.Name = tmpo.City)
			GROUP BY tmpo.City
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationOursCityFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert City Ours', 'Success')
	END
	-- Update in Account from templorary table 
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			UPDATE a SET a.Code = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Code), ''), 
				a.Name = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Name), ''), 
				a.CityId = ci.Id, 
				a.Address = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Address), ''), 
				a.UsrAddress2 = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Address2), ''), 
				a.Phone = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Phone), ''), 
				a.UsrComment1 = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Comment1), ''), 
				a.UsrComment2 = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Comment2), ''), 
				a.UsrRegion = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Region), ''), 
				a.UsrFilial = IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Filial), '')
			FROM Account AS a
				inner join TempOursCsv AS tmpo ON a.Code = TRY_CONVERT([nvarchar], tmpo.Code)
				left join City AS ci with (nolock) ON ci.Name = TRY_CONVERT([nvarchar], tmpo.City)
			WHERE  tmpo.Code != '' AND
				(a.Name != tmpo.Name OR
				a.CityId != ci.Id OR
				a.Address != tmpo.Address OR
				a.UsrAddress2 != tmpo.Address2 OR
				a.Phone != tmpo.Phone OR
				a.UsrComment1 != tmpo.Comment1 OR
				a.UsrComment2 != tmpo.Comment2 OR
				a.UsrRegion != tmpo.Region OR
				a.UsrFilial != tmpo.Filial)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationUpdateAccountFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Update Account', 'Success')
	END
	-- Insert in Account from templorary table 
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO Account (Code, Name, CityId, Address, UsrAddress2, Phone, UsrComment1, UsrComment2, UsrRegion, UsrFilial)
			SELECT  
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Code), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Name), ''),
				 ci.Id,
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Address), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Address2), ''),	
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Phone), ''),	
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Comment1), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Comment2), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Region), ''),
				 IsNULL(TRY_CONVERT([nvarchar](250), tmpo.Filial), '')
			FROM TempOursCsv AS tmpo
				left join City AS ci with (nolock) ON ci.Name = TRY_CONVERT([nvarchar], tmpo.City)
			WHERE not exists (select 1 from Account as a where a.Code = TRY_CONVERT([nvarchar], tmpo.Code))
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationInsertAccountFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert Account', 'Success')
	END
		
	BEGIN 
		TRUNCATE TABLE TempOursCsv
	END
	--_________________________________________ INSERT Schedule from FTP _________________________________________
	IF NOT EXISTS(
		SELECT * FROM sys.objects WHERE Name = 'TempScheduleCsv'
	)
	BEGIN
		DECLARE @createTableSchedule nvarchar(2000) = N'
			CREATE TABLE TempScheduleCsv 
			(
				Code NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', WorkingHours NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				DayOfWeek NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', TBreak NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +'
			)'
		EXEC (@createTableSchedule)
	END
	
	BEGIN TRY
	  BEGIN TRAN
  		BEGIN
			declare @sqlTextShedule nvarchar(1000) = N'
	  			BULK INSERT TempScheduleCsv
				FROM ' + ''''  + @PathSheduleFile + '''' +  '
				WITH
				(
					CODEPAGE = ''ACP'',
					FIRSTROW = 2,
					FIELDTERMINATOR = '';'',  
					ROWTERMINATOR = ''\n'',
					TABLOCK
				)
			';
			exec(@sqlTextShedule)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertSheduleFromFtp', @ErrorMessage)
	END CATCH
	
	-- Update from temporary table #TempScheduleCsv
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			UPDATE accs SET accs.UsrAccountId = a.Id,
				accs.UsrWorkingHours = IsNULL(TRY_CONVERT([nvarchar](250), tmps.WorkingHours), ''),
				accs.UsrDayOfWeekId = dw.Id,
				accs.UsrBreak = IsNULL(TRY_CONVERT([nvarchar](250), tmps.TBreak), '')
			FROM UsrAccountSchedule AS accs
				INNER JOIN Account as a with (nolock) ON a.Id = accs.UsrAccountId
				INNER JOIN TempScheduleCsv as tmps ON tmps.Code = a.Code
				INNER JOIN DayOfWeek as dw with (nolock) ON dw.UsrCode = TRY_CONVERT([nvarchar], tmps.DayOfWeek)
			WHERE tmps.Code != '' 
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationInsertAccountSheduleFromFtp', @ErrorMessage)
	END CATCH
	
	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Update UsrAccountSchedule', 'Success')
	END 
	-- Insert from temporary table #TempScheduleCsv
	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO UsrAccountSchedule (UsrAccountId, UsrWorkingHours, UsrDayOfWeekId, UsrBreak)
			SELECT 
				a.Id,
				IsNULL(TRY_CONVERT([nvarchar](250), ts.WorkingHours), ''),
				dw.Id,
				IsNULL(TRY_CONVERT([nvarchar](250), ts.TBreak), '')
			FROM TempScheduleCsv as ts
				INNER JOIN Account as a ON a.Code = TRY_CONVERT([nvarchar], ts.Code)
				INNER JOIN DayOfWeek as dw ON dw.UsrCode = TRY_CONVERT([nvarchar], ts.DayOfWeek)
			WHERE dw.UsrCode != '' AND 
			NOT EXISTS(select top 1 * from UsrAccountSchedule as accs where accs.UsrAccountId = a.Id)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationInsertAccountSheduleFromFtp', @ErrorMessage)
	END CATCH
	
	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert UsrAccountSchedule', 'Success')
	END 

	BEGIN	
		TRUNCATE TABLE TempScheduleCsv 
	END
	
	--_________________________________________ INSERT Product from FTP _________________________________________
	IF NOT EXISTS(
		SELECT * FROM sys.objects WHERE Name = 'TempProductCsv'
	)
	BEGIN
		DECLARE @createTableProduct nvarchar(2000) = N'
			CREATE TABLE TempProductCsv 
			(
				Code NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +', Name NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +',
				Barcode NVARCHAR(250) COLLATE ' + ''+ @charset+ '' +'
			)'
		EXEC (@createTableProduct)
	END

	BEGIN TRY
	  BEGIN TRAN
  		BEGIN
			declare @sqlTextProds nvarchar(1000) = N'
	  			BULK INSERT TempProductCsv
				FROM ' + ''''  + @PathProdsFile + '''' +  '
				WITH
				(
					CODEPAGE = ''ACP'',
					FIRSTROW = 2,
					FIELDTERMINATOR = '';'',  
					ROWTERMINATOR = ''\n'',
					TABLOCK
				)
			';
			exec(@sqlTextProds)
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationBulcInsertProductFromFtp', @ErrorMessage)
	END CATCH

	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO UsrBarcode (Id, UsrCode, UsrProductCode)
			SELECT NEWID() AS Id, tp.Barcode, tp.Code AS ProductCode
			FROM TempProductCsv AS tp
			WHERE not tp.Barcode is null and not exists (SELECT 1 FROM UsrBarcode AS b WHERE b.UsrCode = tp.Barcode)
			GROUP BY tp.Barcode, tp.Code
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationInsertUsrBarcodeFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert UsrBarcode', 'Success')
	END

	BEGIN TRY
	  BEGIN TRAN
		BEGIN
			INSERT INTO Product (Name, Code)
			SELECT  IsNULL(TRY_CONVERT([nvarchar](250), tp.Name), ''),
					IsNULL(TRY_CONVERT([nvarchar](250), tp.Code), '')
			FROM TempProductCsv as tp
			WHERE tp.Code != '' AND not exists (select 1 from Product as p where p.Code = TRY_CONVERT([nvarchar], tp.Code))
		END
	  COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationInsertProductFromFtp', @ErrorMessage)
	END CATCH

	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('Insert Product', 'Success')
	END

	BEGIN	
		TRUNCATE TABLE TempProductCsv 
	END

	--SET @ReturnValue = 1
	BEGIN
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrDescription) VALUES ('IntegrationFromFtp', 'No errors. Import succeeded.')
	END
	END TRY
	BEGIN CATCH
		TRUNCATE TABLE TempBuyersCsv
		TRUNCATE TABLE TempInActiveCardsCsv
		TRUNCATE TABLE TempOursCsv
		TRUNCATE TABLE TempScheduleCsv
		TRUNCATE TABLE TempProductCsv

		--SET @ReturnValue = 0
		--SET @Error = ERROR_MESSAGE()
		
		IF @@TRANCOUNT > 0 ROLLBACK
		SELECT ERROR_MESSAGE() AS ErrorMessage;
		SET @ErrorMessage = ERROR_MESSAGE()
		INSERT INTO UsrIntegrationLogFtp (UsrName, UsrErrorDescription) VALUES ('IntegrationFromFtp', @ErrorMessage)
	END CATCH

END
GO 