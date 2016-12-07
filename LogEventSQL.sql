use dollogevent
GO

declare @AppID as int

if (select count(*) from buApplication where AppName='vsdStolenVehMsgService')=0
	begin
		insert into buApplication
		select 'vsdStolenVehMsgService'
	
		set @AppID = @@IDENTITY
	end
else
	begin
		set @AppID = (select AppID from buApplication where AppName='vsdStolenVehMsgService')
	end

insert into buApplicationEmail
select @AppID, 'VSAPPSUP@DOL.WA.GOV'

