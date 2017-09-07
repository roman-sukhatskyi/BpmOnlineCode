DECLARE @data xml = (
	replace(
		(
			select  top 1 
				convert(varchar(max), CONVERT(varbinary(max), Data))
			FROM [File]
			WHERE Id = 'CF7018A8-5F67-4DAC-AD7C-0EE9A59D14C1'
		), 
		'&#x03;', 
		''
	)
);

BEGIN
	CREATE TABLE TempTableCsv 
	(
		MemberIdPL NVARCHAR(500), Surname NVARCHAR(500), 
		GivenName NVARCHAR(500), MiddleName  NVARCHAR(500), 
		Gender NVARCHAR(500), Birthday  NVARCHAR(500), 
		Phone NVARCHAR(250), Email NVARCHAR(250), 
		City NVARCHAR(500), Street NVARCHAR(500), 
		House NVARCHAR(500), Corps NVARCHAR(500), 
		Apartment NVARCHAR(500), NumberActingCard NVARCHAR(500), 
		StatusWriteoffBonuses NVARCHAR(500), StatusCard NVARCHAR(500), 
		DateRegistrationCard NVARCHAR(500), CardType NVARCHAR(500), 
		BalanceTrade NVARCHAR(500), AcceptanceEmailPl NVARCHAR(500), 
		AcceptanceSMSPl NVARCHAR(500), SymptomParticipationClub NVARCHAR(500), 
		AcceptanceSMSKm NVARCHAR(500), AcceptanceEmailKm NVARCHAR(500), 
		WalletBalance NVARCHAR(500)
	)
END  

SELECT 
	row.value('./BuyerID[1]', 'nvarchar(250)') as MemberIdPL,   
	row.value('./lastName[1]', 'nvarchar(250)') as Surname,
	row.value('./firstName[1]', 'nvarchar(250)') as GivenName,
	row.value('./middleName[1]', 'nvarchar(250)') as MiddleName,
	row.value('./sex[1]', 'nvarchar(250)') as Gender,
    row.value('./birthDate[1]', 'nvarchar(250)') as Birthday,
	row.value('./cellphone[1]', 'nvarchar(250)') as Phone,
	row.value('./email[1]', 'nvarchar(250)') as Email,
	row.value('./addrCity[1]', 'nvarchar(250)') as City,
	row.value('./addrStreet[1]', 'nvarchar(250)') as Street,
	row.value('./addrBuilding[1]', 'nvarchar(250)') as House,
	row.value('./addrHousing[1]', 'nvarchar(250)') as Corps,
	row.value('./addrFlat[1]', 'nvarchar(250)') as Apartment,
	row.value('./cardNum[1]', 'nvarchar(250)') as NumberActingCard,
	row.value('./spendAllowed[1]', 'nvarchar(250)') as StatusWriteoffBonuses,
	row.value('./state[1]', 'nvarchar(250)') as StatusCard,
	row.value('./dateRegistered[1]', 'nvarchar(250)') as DateRegistrationCard,
	row.value('./CardType[1]', 'nvarchar(250)') as CardType,
	row.value('./activeBalance[1]', 'nvarchar(250)') as BalanceTrade,
	row.value('./sendEmail[1]', 'nvarchar(250)') as AcceptanceEmailPl,
	row.value('./sendSMS[1]', 'nvarchar(250)') as AcceptanceSMSPl,
	row.value('./KM[1]', 'nvarchar(250)') as SymptomParticipationClub,
	row.value('./sendEmailKM[1]', 'nvarchar(250)') as AcceptanceSMSKm,
	row.value('./sendSMSlKM[1]', 'nvarchar(250)') as AcceptanceEmailKm,
	row.value('./BalansPurse[1]', 'nvarchar(250)') as WalletBalance
FROM @data.nodes('./Buyers/row') col(row) 




select City from (
	SELECT 
		row.value('./addrCity[1]', 'nvarchar(250)') as City
	FROM @data.nodes('./Buyers/row') col(row) 
) as rt
where not City is null AND not exists (SELECT 1 FROM City AS c WHERE c.Name = rt.City)
GROUP by City



	BEGIN
		DECLARE @data2 xml = (
			replace(
				(
					select  top 1 
						convert(varchar(max), CONVERT(varbinary(max), Data))
					FROM [File]
					WHERE Id = 'CF7018A8-5F67-4DAC-AD7C-0EE9A59D14C1'
				), 
				'&#x03;', 
				''
			)
		);


		--INSERT INTO City (Id, Name)
		SELECT NEWID() AS Id, row.value('./addrCity[1]', 'nvarchar(250)') AS Name
		FROM @data2.nodes('./Buyers/row') col(row) 
		WHERE not tmp.City is null AND not exists (SELECT 1 FROM City AS c WHERE c.Name = tmp.City)
		GROUP BY tmp.City
	END
