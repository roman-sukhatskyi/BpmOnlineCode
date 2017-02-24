UPDATE [SysPackage] SET [IsChanged] = 1, [IsLocked] = 1, [InstallType] = 0 WHERE [Name] = 'WebitelSMS';

UPDATE [SysSchema] SET [IsChanged] = 1, [IsLocked] = 1 WHERE [syspackageid] = (SELECT [id] FROM [syspackage] WHERE [name] = 'WebitelSMS')

UPDATE [syspackageschemadata] SET [IsChanged] = 1, [IsLocked] = 1 WHERE [syspackageid] = (SELECT [id] FROM [syspackage] WHERE [name] = 'WebitelSMS')


--Для разблокировки пакета необходимо изменить первый скрипт на:
UPDATE [SysPackage]
SET
[IsChanged] = 1,
[IsLocked] = 1,
[InstallType] = 0,
[Maintainer] = (select [TextValue]
from [SysSettingsValue]
join [SysSettings]
on [SysSettingsValue].[SysSettingsId] = [SysSettings].[Id]
WHERE [Code] = 'Maintainer')
WHERE [Name] = 'WebitelCore'
