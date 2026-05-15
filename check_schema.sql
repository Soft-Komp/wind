SELECT s.name+CHAR(46)+t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE CHARINDEX('skw_',t.name)=1 ORDER BY 1
