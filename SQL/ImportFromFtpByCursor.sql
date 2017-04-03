--*********************************************************Перевірка створена тимчасової таблиці *********************************************************
IF NOT EXISTS(
	SELECT * FROM sys.objects WHERE Name = 'TempTableCsv'
)
-- *********************************************************Створення тимчасовоъ балички*********************************************************
BEGIN
	CREATE TABLE TempTableCsv 
	(
		N0 NVARCHAR(500), N1 NVARCHAR(500), N2 NVARCHAR(500), N3 NVARCHAR(500), 
		N4 NVARCHAR(500), N5 NVARCHAR(500), N6 NVARCHAR(500), N7 NVARCHAR(500), 
		N8 NVARCHAR(500), N9 NVARCHAR(500), N10 NVARCHAR(500), N11 NVARCHAR(500), 
		N12 NVARCHAR(500), N13 NVARCHAR(500), N14 NVARCHAR(500), N15 NVARCHAR(500), 
		N16 NVARCHAR(500), N17 NVARCHAR(500), N18 NVARCHAR(500), N19 NVARCHAR(500), 
		N20 NVARCHAR(500), N21 NVARCHAR(500), N22 NVARCHAR(500), N23 NVARCHAR(500), 
		N24 NVARCHAR(500)
	)
END
--*********************************************************Запуск імпорту даних в тимчасову таблицю*********************************************************
BEGIN
	BULK INSERT TempTableCsv
    FROM 'C:\RUSH_Buyers_2017_03_06.csv'
    WITH
    (
		CODEPAGE = 'ACP',
		FIRSTROW = 2,
		FIELDTERMINATOR = ';',  
		ROWTERMINATOR = '\n',
		TABLOCK
    )
END

DECLARE @ErrorMessage nvarchar(max)
BEGIN TRY
	BEGIN
		DECLARE 
		@MemberIdPL NVARCHAR(500),
		@Surname NVARCHAR(500),               --string
		@GivenName  NVARCHAR(500),            --string                
		@MiddleName  NVARCHAR(500),           --string
		@Gender NVARCHAR(500),                --справочник   (Gender)
		@Birthday NVARCHAR(500),              --dateTime
		@Phone NVARCHAR(500),                 --string
		@Email NVARCHAR(500),                 --string
		@City NVARCHAR(500),                  --справочник   (ContactAddress)
		@Street NVARCHAR(500),                --string       (ContactAddress)
		@House NVARCHAR(500),                 --string       (ContactAddress) 
		@Corps NVARCHAR(500),                 --string       (ContactAddress)
		@Apartment NVARCHAR(500),             --string       (ContactAddress) 
		@NumberActingCard NVARCHAR(500),      --int
		@StatusWriteoffBonuses NVARCHAR(500), --справочник   (UsrStatusWriteoffBonuses) 
		@StatusCard NVARCHAR(500),            --справочник   (UsrStatusCard)
		@DateRegistrationCard NVARCHAR(500),  --dateTime
		@CardType NVARCHAR(500),              --справочник   (UsrCardType)
		@BalanceTrade NVARCHAR(500),          --decimal
		@AcceptanceEmailPl NVARCHAR(500),     --справочник   (UsrAcceptanceEmailPl)
		@AcceptanceSMSPl NVARCHAR(500),       --справочник   (UsrAcceptanceSMSPl)
		@SymptomParticipationClub NVARCHAR(500), --bool
		@AcceptanceSMSKm NVARCHAR(500),       --справочник   (UsrAcceptanceSMSKm)
		@AcceptanceEmailKm NVARCHAR(500),     --справочник   (UsrAcceptanceEmailKm)
		@WalletBalance NVARCHAR(500)          --decimal
		-- *********************************************************СТВОРЕННЯ CURSOR*****************************************************************
		DECLARE Contact_Сursor CURSOR  
		FOR SELECT N0, N1, N2, N3, N4, N5, N6, N7, N8, N9, N10, N11, N12, N13, N14,
		N15, N16, N17, N18, N19, N20, N21, N22, N23, N24 FROM TempTableCsv  
		OPEN Contact_Сursor  
		FETCH NEXT FROM Contact_Сursor 
		INTO @MemberIdPL, @Surname, @GivenName, @MiddleName, @Gender, @Birthday, @Phone, @Email, @City, @Street, @House, @Corps, 
		@Apartment, @NumberActingCard, @StatusWriteoffBonuses, @StatusCard, @DateRegistrationCard, 
		@CardType, @BalanceTrade, @AcceptanceEmailPl, @AcceptanceSMSPl, @SymptomParticipationClub, 
		@AcceptanceSMSKm, @AcceptanceEmailKm, @WalletBalance
		-- *********************************************************ОТРИМАННЯ ДАНИХ*****************************************************************
		WHILE @@FETCH_STATUS = 0   
		BEGIN   
			FETCH NEXT FROM Contact_Сursor
			INTO @MemberIdPL, @Surname, @GivenName, @MiddleName, @Gender, @Birthday, @Phone, @Email, @City, @Street, @House, @Corps, 
		@Apartment, @NumberActingCard, @StatusWriteoffBonuses, @StatusCard, @DateRegistrationCard, 
		@CardType, @BalanceTrade, @AcceptanceEmailPl, @AcceptanceSMSPl, @SymptomParticipationClub, 
		@AcceptanceSMSKm, @AcceptanceEmailKm, @WalletBalance
		-- *********************************************************ПЕРЕВІРКИ***************************************************************** 
		DECLARE @phoneCount NVARCHAR(500),
				@cityId [uniqueidentifier],	
				@genderId [uniqueidentifier],
				@statusWriteoffBonusesId [uniqueidentifier],
				@statusCardId [uniqueidentifier],
				@cardTypeId [uniqueidentifier],
				@acceptanceSendingEmailId [uniqueidentifier],
				@acceptanceSendingSMSId [uniqueidentifier],
				@contactId [uniqueidentifier],
				@contactName NVARCHAR(500),
				@contactTypeId [uniqueidentifier]
		-- ***********************************************ПЕРЕВІРКИ ПОЛІВ НА ПУСТОТУ***********************************************
		IF exists(select 1 where @MemberIdPL is null)
			BEGIN
				SET @MemberIdPL = ''
			END
		IF exists(select 1 where @Surname is null)
			BEGIN
				SET @Surname = ''
			END
		IF exists(select 1 where @GivenName is null)
			BEGIN
				SET @GivenName = ''
			END
		IF exists(select 1 where @MiddleName is null)
			BEGIN
				SET @MiddleName = ''
			END
		IF exists(select 1 where @Email is null)
			BEGIN
				SET @Email = ''
			END
		IF exists(select 1 where @Phone is null)
			BEGIN
				SET @Phone = ''
			END
		IF exists(select 1 where @Street is null)
			BEGIN
				SET @Street = ''
			END
		IF exists(select 1 where @House is null)
			BEGIN
				SET @House = ''
			END
		IF exists(select 1 where @Corps is null)
			BEGIN
				SET @Corps = ''
			END
		IF exists(select 1 where @Apartment is null)
			BEGIN
				SET @Apartment = ''
			END
		IF @Birthday = '0000-00-00'
			BEGIN
				SET @Birthday = NULL
			END
		-- ***********************************************ПЕРЕВІРКИ ПОЛІВ НА ПУСТОТУ***********************************************	
		SET @contactName = @Surname + ' ' + @GivenName + ' ' + @MiddleName
		SET @contactTypeId = '00783EF6-F36B-1410-A883-16D83CAB0980'
		SET @cityId = (SELECT TOP(1) Id FROM City WHERE Name = @City)
		IF exists(select 1 where @cityId is null)
			BEGIN
				IF not exists(select 1 where @City is null)
				BEGIN
					INSERT INTO City (Name, CountryId, RegionId) values (@City, null, null)
				END
				
			END
			SET @cityId = (SELECT TOP(1) Id FROM City WHERE Name = @City)
		SET @genderId =
			CASE
				WHEN EXISTS(SELECT * FROM Gender where UsrCode = 2) THEN 'EEAC42EE-65B6-DF11-831A-001D60E938C6'
				WHEN EXISTS(SELECT * FROM Gender where UsrCode = 1) THEN 'FC2483F8-65B6-DF11-831A-001D60E938C6'
			END;
		SET @statusWriteoffBonusesId =
			CASE
				WHEN EXISTS(SELECT * FROM UsrStatusWriteoffBonuses where UsrCodeId = 0) THEN 'CD5225E2-82B3-4DA9-81FD-4CB69E3251EA'
				WHEN EXISTS(SELECT * FROM UsrStatusWriteoffBonuses where UsrCodeId = 1) THEN 'E4075BA5-A3B6-43CC-94C4-A8D7AEDB4311'
			END;
		SET @statusCardId =
			CASE
				WHEN EXISTS(SELECT * FROM UsrStatusCard where UsrCodeId = 1) THEN 'B2A488E9-83FC-4FE3-9FDA-7D9EB0E9F6DA'
				WHEN EXISTS(SELECT * FROM UsrStatusCard where UsrCodeId = 2) THEN '50160227-8D86-41C4-BB5D-8E0CE034635A'
				WHEN EXISTS(SELECT * FROM UsrStatusCard where UsrCodeId = 3) THEN 'C5F7E61B-B0E7-4FCF-8454-463EBFC3E84F'
				WHEN EXISTS(SELECT * FROM UsrStatusCard where UsrCodeId = 4) THEN 'CA5E800E-F9B9-41DB-B0E4-FE3DB3E946A4'
			END;
		SET @cardTypeId = 
			CASE
				WHEN EXISTS(SELECT * FROM UsrCardType where UsrCodeId = 0) THEN '0FE4D077-B894-4023-B73A-0CCCF4CD13DB'
				WHEN EXISTS(SELECT * FROM UsrCardType where UsrCodeId = 1) THEN 'F49818B6-9AD3-4741-816A-E2B2CC8D772A'
			END;
		SET @acceptanceSendingEmailId = 
			CASE
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingEmail where UsrCodeId = 0) THEN '118C70EF-F56D-43F7-A338-E0B0DCFF8862'
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingEmail where UsrCodeId = 1) THEN 'F12FC0BA-ACE7-4E19-B9E6-05CBFC9110B5'
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingEmail where UsrCodeId = 2) THEN '72EFF0BC-C8A2-4B43-969E-7FD65586C736'
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingEmail where UsrCodeId = 3) THEN '1111C6C1-735D-46BC-8204-754E264CA925'
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingEmail where UsrCodeId = 7) THEN 'C5A12366-2C1E-4091-9513-58BA0D877F1B'
			END;
		SET @acceptanceSendingSMSId = 
			CASE
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingSMS where UsrCodeId = 0) THEN '264F72E6-11AC-4179-9D9B-E26754E4276C'
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingSMS where UsrCodeId = 1) THEN '9515730A-6FAD-476A-A1F8-726F43CB8216'
				WHEN EXISTS(SELECT * FROM UsrAcceptanceSendingSMS where UsrCodeId = 3) THEN '2518C068-8266-471F-ADDE-04784DAF9326'
			END;
		BEGIN TRY
		--Перевірка на дублі по телефону
			IF EXISTS(SELECT * FROM Contact WHERE UsrMemberIdPL = @MemberIdPL)
				BEGIN 
				UPDATE Contact SET UsrMemberIdPL = @MemberIdPL,
								   TypeId = @contactTypeId,
								   Surname = @Surname, 
								   GivenName = @GivenName,
								   MiddleName = @MiddleName,
								   Name = @contactName,
								   GenderId = @genderId,
								   BirthDate =  CAST(@Birthday AS [datetime2]), 
								   MobilePhone = @Phone,
								   Email = @Email, 
								   CityId = @cityId,
								   UsrNumberActiveCard = CAST(@NumberActingCard AS [decimal](18, 2)),
								   UsrStatusWriteoffBonusesId = @statusWriteoffBonusesId,
								   UsrStatusCardId = @statusCardId,
								   UsrDateRegistrationCard = CAST(@DateRegistrationCard AS [datetime2]),
								   UsrCardTypeId =  @cardTypeId,
								   UsrBalanceTrade = CAST(@BalanceTrade AS DECIMAL),
								   UsrAcceptanceEmailPlId = @acceptanceSendingEmailId,
								   UsrAcceptanceSMSPlId = @acceptanceSendingSMSId,
								   UsrSymptomParticipationClub =  CAST(@SymptomParticipationClub AS [bit]),
								   UsrAcceptanceSMSKmId = @acceptanceSendingSMSId,
								   UsrAcceptanceEmailKmId = @acceptanceSendingEmailId,
								   UsrWalletBalance = CAST(@WalletBalance AS DECIMAL)
								WHERE Id = @contactId
				 END
			ELSE
				BEGIN
				SET @contactId = NEWID()
				INSERT INTO Contact (Id,
									 UsrMemberIdPL,
									 TypeId,
									 Surname,   --+
									 GivenName, --+
									 MiddleName, --+
									 Name,
									 GenderId,   --+
									 BirthDate, --+ 
									 MobilePhone,     --+
									 Email,      --+
									 CityId,     --+
									 UsrNumberActiveCard, --+
									 UsrStatusWriteoffBonusesId, --+  
									 UsrStatusCardId, --+
									 UsrDateRegistrationCard, --+
									 UsrCardTypeId,  --+
									 UsrBalanceTrade, --+
									 UsrAcceptanceEmailPlId, --+
									 UsrAcceptanceSMSPlId, --+
									 UsrSymptomParticipationClub, --+
									 UsrAcceptanceSMSKmId, --+
									 UsrAcceptanceEmailKmId, --+
									 UsrWalletBalance --+
										 ) VALUES (@contactId,
												   @MemberIdPL,
												   @contactTypeId,
												   @Surname,
												   @GivenName,
												   @MiddleName,
												   @contactName,
												   @genderId,
												   CAST(@Birthday AS [datetime2]),
												   @Phone,
												   @Email,
												   @cityId,
												   --@Street,
												   --@House,
												   --@Corps,
												   --@Apartment,
												   CAST(@NumberActingCard AS  [decimal](18, 2)),
												   @statusWriteoffBonusesId,
												   @statusCardId,
												   CAST(@DateRegistrationCard AS [datetime2]),
												   @cardTypeId,
												   CAST(@BalanceTrade AS [decimal](20, 2)),
												   @acceptanceSendingEmailId,
												   @acceptanceSendingSMSId,
												   CAST(@SymptomParticipationClub AS [bit]),
												   @acceptanceSendingSMSId,
												   @acceptanceSendingEmailId,
												   CAST(@WalletBalance AS [decimal](20, 2))
										 )
				END
			END TRY  
			BEGIN CATCH 	
				SELECT ERROR_MESSAGE() AS ErrorMessage;
				SET @ErrorMessage = ERROR_MESSAGE()
				insert into UsrIntegrationLogFtp (UsrName, UsrErrorDescription) values ('IntegrationContactFromFtp', @ErrorMessage)
			END CATCH;  
			BEGIN TRY  
			DECLARE @currentCityId [uniqueidentifier]
			SET @currentCityId = @cityId
			IF EXISTS(SELECT * FROM ContactAddress WHERE ContactId = @contactId)
				BEGIN
					UPDATE ContactAddress SET  AddressTypeId = '780BF68C-4B6E-DF11-B988-001D60E938C6', CountryId = 'A470B005-E8BB-DF11-B00F-001D60E938C6', 
											   CityId = @currentCityId, Address = @Street, UsrHouse = @House, UsrCorps = @Corps, UsrApartment = @Apartment
				END
			ELSE
				BEGIN
					INSERT INTO ContactAddress (AddressTypeId, CountryId, CityId, ContactId, Address, UsrHouse, UsrCorps, UsrApartment) 
					VALUES ('780BF68C-4B6E-DF11-B988-001D60E938C6', 'A470B005-E8BB-DF11-B00F-001D60E938C6', @currentCityId, @contactId, @Street, @House, @Corps, @Apartment)
				END
			END TRY  
			BEGIN CATCH 	
				SELECT ERROR_MESSAGE() AS ErrorMessage;
				SET @ErrorMessage = ERROR_MESSAGE()
				insert into UsrIntegrationLogFtp (UsrName, UsrErrorDescription) values ('IntegrationContactFromFtp', @ErrorMessage)
			END CATCH;
	END   
	CLOSE Contact_Сursor   
		DEALLOCATE Contact_Сursor
	END
END TRY  
BEGIN CATCH 	
	SELECT ERROR_MESSAGE() AS ErrorMessage;
	SET @ErrorMessage = ERROR_MESSAGE()
	insert into UsrIntegrationLogFtp (UsrName, UsrErrorDescription) values ('IntegrationContactFromFtp', @ErrorMessage)
END CATCH;  
GO  	
	
	
--Після всіх операці портібно видалити тестову таблицю "CSVTestNew"
BEGIN 
	DROP TABLE TempTableCsv
END

SELECT TOP(10) * FROM TempTableCsv