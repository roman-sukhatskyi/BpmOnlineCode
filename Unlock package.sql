UPDATE [SysPackage] SET [IsChanged] = 1, [IsLocked] = 1, [InstallType] = 0 WHERE [Name] = 'WebitelSMS';

UPDATE [SysSchema] SET [IsChanged] = 1, [IsLocked] = 1 WHERE [syspackageid] = (SELECT [id] FROM [syspackage] WHERE [name] = 'WebitelSMS')

UPDATE [syspackageschemadata] SET [IsChanged] = 1, [IsLocked] = 1 WHERE [syspackageid] = (SELECT [id] FROM [syspackage] WHERE [name] = 'WebitelSMS')