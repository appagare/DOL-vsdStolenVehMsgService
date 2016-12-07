use vsdStolenVehicle
go

update utAppConfig
set KeyValue = 'DOLUTOLYQSVR'
where ProgramName = 'vsdStolenVehMsgService' and KeyName = 'QueueServer'
