'Preliminary code changes complete for WACIC
'TODO: remove OldOCA stuff (OldOCANumber parameter and OCAModified ProcessStatus value
'TODO: double check Modify -- ParseModifiedFields and SetModifiedField

Imports System.ServiceProcess
Imports System.Threading
Imports WA.DOL.LogEvent.LogEvent
Imports WA.DOL.VSD.WSPQueue

Public Class vsdStolenVehMsgServ
    Inherits System.ServiceProcess.ServiceBase

    Private QueueRxName As String = "PRIVATE$\SV"  'name of the stolen vehicle input queue
    Private QueueServer As String = "" ' queue server path name
    Private QueueSleepWhenEmpty As Integer = 300000  'number of milliseconds to wait when the queue is empty before checking again - default to 5 min.
    Private DebugMode As Byte = 0 '0 = don't log raw messages; 1 = log all messages.
    Private LogEventObject As New WA.DOL.LogEvent.LogEvent()
    Private QueueRXObject As WA.DOL.VSD.WSPQueue.QueueObject
    Private ConnectString As String = "" 'connect string key for database.
    Private EmailFrequency As Integer = 3600 'number of seconds between error e-mails
    Private LastEmailSent As Date = Now.AddDays(-1) 'initialize it to an "old" day
    Private MessageFieldMap As New DataTable() 'datatable containing all of the mapping of

    'constants
    Private Const STOLEN_VEHICLE_TABLE As String = "buStolenVehMessages"
    Private Const SP_INSERT_RECORD As String = "insStolenVehMessage"
    Private Const SP_SELECT_CONFIG As String = "selAppConfig"
    Private Const SP_SELECT_MESSAGE_FIELDS As String = "selMessageFieldCodes"

    'enumeration for message process status
    Private Enum ProcessStatus
        Fail = -1
        Unprocessed = 0
        BadData = 2
        Skip = 3
    End Enum

    'enumeration for state of the service
    Private Enum ServiceStates
        Shutdown = 0
        Paused = 1
        Running = 2
    End Enum

    'enumeration of types of messages to identify
    Private Enum SVMessageTypes
        Enter = 0
        Modify = 1
        Cancel = 2
        Clear = 3
        Unknown = 4
    End Enum

    'enumeratioon of field positions for EVS (Enter)
    Private Enum EVSPositions
        MessageKey = 0
        ORI = 1
        WACIC = 2
        LicencePlate = 3
        LicenseState = 4
        LicenseYearExpire = 5
        LicensePlateType = 6
        VIN = 7
        VehYear = 8
        VehMake = 9
        VehModel = 10
        VehStyle = 11
        VehColor = 12
        DateOfTheft = 13
        OCA = 14
        Misc = 15
        OAN = 16
    End Enum


    'enumeratioon of field positions for MVS (Modify)
    Private Enum MVSPositions
        MessageKey = 0
        ORI = 1
        WACIC = 2
        RecordIdentifier = 3
        OCA = 4
    End Enum
    'enumeratioon of field positions for CVS (Clear)
    Private Enum CVSPositions
        MessageKey = 0
        ORI = 1
        WACIC = 2
        RecordIdentifier = 3
        OCA = 4
        DateOfClear = 5
        RecoveringAgencyID = 6
        RecoveringAgencyCaseNumber = 7
    End Enum

    'enumeratioon of field positions for XVS (Cancel)
    Private Enum XVSPositions
        MessageKey = 0
        ORI = 1
        WACIC = 2
        RecordIdentifier = 3
        OCA = 4
        DateOfCancel = 5
    End Enum

    Private ServiceState As ServiceStates = ServiceStates.Paused

#Region " Component Designer generated code "

    Public Sub New()
        MyBase.New()

        ' This call is required by the Component Designer.
        InitializeComponent()

        ' Add any initialization after the InitializeComponent() call

    End Sub

    'UserService overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ' The main entry point for the process
    <MTAThread()> _
    Shared Sub Main()
        Dim ServicesToRun() As System.ServiceProcess.ServiceBase

        ' More than one NT Service may run within the same process. To add
        ' another service to this process, change the following line to
        ' create a second service object. For example,
        '
        '   ServicesToRun = New System.ServiceProcess.ServiceBase () {New Service1, New MySecondUserService}
        '
        ServicesToRun = New System.ServiceProcess.ServiceBase() {New vsdStolenVehMsgServ}

        System.ServiceProcess.ServiceBase.Run(ServicesToRun)
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    ' NOTE: The following procedure is required by the Component Designer
    ' It can be modified using the Component Designer.  
    ' Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        '
        'vsdStolenVehMsgServ
        '
        Me.ServiceName = "vsdStolenVehMsgService"

    End Sub

#End Region

    Protected Overrides Sub OnStart(ByVal args() As String)
        ' Add code here to start your service. This method should set things
        ' in motion so your service can do its work.

        Try

            'set the connect string in machine.config from the token in app.config
            Dim AppSettingsReader As New System.Configuration.AppSettingsReader
            Dim strReturnValue As String = ""
            ConnectString = AppSettingsReader.GetValue("ConnectString", GetType(System.String))
            AppSettingsReader = Nothing

            'read the app settings from the db
            ReadAppSettings()

            'validate settings
            If Not ValidSettings() Then
                'we have read all of the values but do not
                'have valid parameters and thus are unable
                'to continue. Throw error to drop into exception catch
                Throw New Exception("Invalid settings at start up. Unable to proceed.")
            End If

            'create input queue
            QueueRXObject = New WA.DOL.VSD.WSPQueue.QueueObject(QueueServer, QueueRxName)

        Catch ex As Exception
            'LogEvent, Send E-mail, and quit
            Dim strMessage As String = "Service is unable to proceed.  Shutting down. " & ex.Message
            'Log the error
            LogEvent("Service_OnStart", strMessage, MessageType.Error, LogType.Standard)
            OnStop()
            Exit Sub
        End Try

        'set our status to run mode
        ServiceState = ServiceStates.Running

        'make note that we started
        LogEvent("OnStart", "Service Started", MessageType.Start, LogType.Standard)

        'start an endless loop for processing the queue
        ThreadPool.QueueUserWorkItem(AddressOf ProcessQueue)
    End Sub

    Protected Overrides Sub OnShutdown()
        'calls the Windows service OnStop method
        OnStop()
    End Sub

    Protected Overrides Sub OnStop()
        ' Add code here to perform any tear-down necessary to stop your service.
        ServiceState = ServiceStates.Shutdown

        'add a delay to complete any inprocess messages
        'give thread 8 seconds to wrap things up (this should be more than enough time)
        Dim EndWait As Date = Now.AddSeconds(8)
        While Now <= EndWait
            'do nothing
        End While

        'log event that service is stopping
        LogEvent("OnStop", "Service Stopped", MessageType.Finish, LogType.Standard)
    End Sub

    Private Function ConvertYYYYMMDDToDate(ByVal YYYYMMDD As String) As Date
        'Purpose:   Convert a YYYYMMDD string to a date. Throws exception if it fails.
        'Inputs:    YYYYMMDD string date.
        'Output:    Date.

        If Not IsNumeric(YYYYMMDD) Or Len(YYYYMMDD) <> 8 Then
            'throw an error if we can tell its not going to convert
            Throw New Exception("Error converting YYYYMMDD to Date (" & YYYYMMDD & ").")
        Else
            Try
                'try to convert the string to a date
                Return New Date(CType(Left(YYYYMMDD, 4), Integer), _
                    CType(Mid(YYYYMMDD, 5, 2), Integer), _
                    CType(Mid(YYYYMMDD, 7, 2), Integer))
            Catch ex As Exception
                'throw an error if the conversion failed.
                Throw New Exception("Error converting YYYYMMDD to Date (" & YYYYMMDD & ") " & ex.Message)
            End Try
        End If
    End Function

    Private Function CreateParameterCollection() As Collection
        'Purpose:   Create the necessary parameters for the insert stored proc.
        'Inputs:    None.
        'Output:    Collection of SqlParameter containing all of the fields 

        Dim colParameters As New Collection

        colParameters.Add(New SqlClient.SqlParameter("@MessageKey", ""), "@MessageKey")
        colParameters.Add(New SqlClient.SqlParameter("@ORIAgency", ""), "@ORIAgency")
        colParameters.Add(New SqlClient.SqlParameter("@ORIWorkstation", ""), "@ORIWorkstation")
        colParameters.Add(New SqlClient.SqlParameter("@OCANumber", ""), "@OCANumber")
        colParameters.Add(New SqlClient.SqlParameter("@LicensePlate", ""), "@LicensePlate")
        colParameters.Add(New SqlClient.SqlParameter("@LicenseState", ""), "@LicenseState")
        colParameters.Add(New SqlClient.SqlParameter("@LicenseYearExp", ""), "@LicenseYearExp")
        colParameters.Add(New SqlClient.SqlParameter("@LicensePlateType", ""), "@LicensePlateType")
        colParameters.Add(New SqlClient.SqlParameter("@VIN", ""), "@VIN")
        colParameters.Add(New SqlClient.SqlParameter("@VehicleYear", ""), "@VehicleYear")
        colParameters.Add(New SqlClient.SqlParameter("@VehicleMake", ""), "@VehicleMake")
        colParameters.Add(New SqlClient.SqlParameter("@VehicleModel", ""), "@VehicleModel")
        colParameters.Add(New SqlClient.SqlParameter("@VehicleColor", ""), "@VehicleColor")
        colParameters.Add(New SqlClient.SqlParameter("@VehicleStyle", ""), "@VehicleStyle")
        colParameters.Add(New SqlClient.SqlParameter("@DateOfTheft", vbNull), "@DateOfTheft")
        colParameters.Add(New SqlClient.SqlParameter("@Miscellaneous", ""), "@Miscellaneous")
        colParameters.Add(New SqlClient.SqlParameter("@OANNumber", ""), "@OANNumber")
        colParameters.Add(New SqlClient.SqlParameter("@WACICNumber", ""), "@WACICNumber")
        colParameters.Add(New SqlClient.SqlParameter("@NCICNumber", ""), "@NCICNumber")
        colParameters.Add(New SqlClient.SqlParameter("@RecovAgencyID", ""), "@RecovAgencyID")
        colParameters.Add(New SqlClient.SqlParameter("@RecovAgencyCaseNo", ""), "@RecovAgencyCaseNo")
        colParameters.Add(New SqlClient.SqlParameter("@DateTimeOfClear", vbNull), "@DateTimeOfClear")
        colParameters.Add(New SqlClient.SqlParameter("@DateTimeOfCancel", vbNull), "@DateTimeOfCancel")
        colParameters.Add(New SqlClient.SqlParameter("@CorrectVIN", ""), "@CorrectVIN")
        colParameters.Add(New SqlClient.SqlParameter("@CorrectPlate", ""), "@CorrectPlate")
        colParameters.Add(New SqlClient.SqlParameter("@CorrectModelYr", ""), "@CorrectModelYr")
        colParameters.Add(New SqlClient.SqlParameter("@CorrectOAN", ""), "@CorrectOAN")
        colParameters.Add(New SqlClient.SqlParameter("@ProcessedStatus", ProcessStatus.Unprocessed), "@ProcessedStatus")
        'colParameters.Add(New SqlClient.SqlParameter("@OldOCANumber", ""), "@OldOCANumber")

        'return the collection
        Return colParameters
    End Function

    Private Function DetermineMessageType(ByVal Body As String) As SVMessageTypes
        'Purpose:   Determine which type of message is being processed by 
        '           parsing the MessageKey from the Body.
        'Inputs:    Body = string containing the message body. Note - the Body is 
        '           the WSPMessage.Body which is the everything to the right of the 
        '           MSS Header (which is typically the first 16 characters of the message)
        'Output:    Returns the enumerated message type.

        Select Case UCase(Left(Body, 3))
            Case "EVS"
                Return SVMessageTypes.Enter
            Case "MVS"
                Return SVMessageTypes.Modify
            Case "CVS"
                Return SVMessageTypes.Clear
            Case "XVS"
                Return SVMessageTypes.Cancel
            Case Else
                Return SVMessageTypes.Unknown
        End Select
    End Function

    Private Function GetFieldValue(ByVal Message As String, _
        ByVal Position As Integer, _
        Optional ByVal MessageFieldCode As String = "") As String
        'Purpose:   Get a period delimited, positional field value from the input string.
        'Inputs:    Message is the string to search.
        '           Position is the zero based field position in the string to fetch.
        '           MessageFieldCode is an optional string value that will be removed
        '           from the return value if the the return value begins with the MessageFieldCode.
        '           ex. - OCA/04-12345 will return 04-12345 when the MessageFieldCode = "OCA/"
        'Output:    Returns a string in location of Position.

        Dim PositionStart As Integer = 0
        Dim PositionEnd As Integer = 0
        Dim CurrentField As Integer = 0
        Dim NextField As Integer = 1
        Dim CurrentPosition As Integer = 0
        Const Delimiter As String = "."

        Do
            'iterate through the message until we get to the field in question
            CurrentField = InStr(NextField, Message, Delimiter)
            NextField = CurrentField + 1
            If CurrentField > 0 Then
                'special case if we are looking for the first position
                If Position = 0 Then
                    PositionEnd = CurrentField
                    PositionStart = 1
                    Exit Do
                End If
                'we found a field so increment the position
                CurrentPosition += 1
            Else
                'we ran out of characters before we reached our "position"
                Exit Do
            End If

            If CurrentPosition = Position Then
                'we've found the field we were looking for so remember our start position
                PositionStart = CurrentField
                'get the end position
                PositionEnd = InStr(NextField, Message, Delimiter)
                If PositionEnd < PositionStart Then
                    'if here it's the last field, so we'll get everything after the starting position
                    PositionEnd = Len(Message) + 1
                End If
                'we should have both the start and end pointers so bail out of the loop
                Exit Do
            End If
        Loop Until CurrentField < 1

        If PositionEnd > PositionStart Then
            'if we have our starting and ending points, get the data in between (and strip any delimiters)
            Dim ReturnValue As String = Replace(Mid(Message, PositionStart, PositionEnd - PositionStart), Delimiter, "")

            If MessageFieldCode <> "" Then
                'if we were passed a MessageFieldCode value to remove, strip it from the return value 
                'if it matches the starting of the return value.
                'ex. - OCA/04-12345 will return 04-12345 when the MessageFieldCode = "OCA/"
                If Left(ReturnValue, Len(MessageFieldCode)) = MessageFieldCode Then
                    ReturnValue = Right(ReturnValue, Len(ReturnValue) - Len(MessageFieldCode))
                End If
            End If

            'return the result
            Return ReturnValue
        Else
            'not found
            Return ""
        End If
    End Function

    Private Sub LogEvent(ByVal pstrSource As String, _
        ByVal pstrMessage As String, _
        ByVal MessageType As MessageType, _
        ByVal LogType As LogType)
        'Purpose:       Write an event to the event log database of the specified type.
        'Input:          pstrSource = source procedure reporting the event.
        '                   pstrMessage = event message.
        '                   MessageType = LogEvent object indicator specifying whether the 
        '                   message is error, informational, start, finish, or debug
        '                   LogType = LogEvent object indicator
        'Returns:       None
        'Note:          When a LogType is error, an e-mail is automatically sent.
        '               E-mails are sent once every EmailFrequency seconds 

        'log message
        LogEventObject.LogEvent(Me.ServiceName, pstrSource, pstrMessage, MessageType, LogType)

        'if message type is an error, also log an e-mail event if we haven't sent one in awhile
        If MessageType = MessageType.Error AndAlso Now >= LastEmailSent.AddSeconds(EmailFrequency) Then

            'send the e-mail
            LogEventObject.LogEvent(Me.ServiceName, pstrSource, pstrMessage, MessageType, LogType.Email)

            'update the last email sent time
            LastEmailSent = Now
        End If
    End Sub

    Private Function ParseMessageCancel(ByVal SVMessage As WSPMessage, ByRef Parameters As Collection) As ProcessStatus
        'Purpose:   Parse out an XVS message and save it to the database
        'Inputs:    SVMessage is the message object being checked.
        '           Parameters is the collection of SqlParameters passed ByRef so we can set the corrected values if necessary.
        'Output:    Unprocessed if OK, BadData if parseable but fails validation, Fail if parsing fails.

        'Expected format from Vehicle File.doc
        '=====================
        'Message Key	XVS
        'Originating Agency Identifier	WA0340500
        'WACIC 
        'Record Identifier	LIC/123XYZ
        'Record Identifier	OCA/99-12345
        'Date of Cancel	20000228
        'XVS.WA0320000.04V0046950.LIC/749GRF.OCA/040101558^M^J

        'parse the message and populate the parameters with the data
        Try
            Parameters("@MessageKey").Value = Left(GetFieldValue(SVMessage.Body, XVSPositions.MessageKey), 3)
            Dim TestValue As String = GetFieldValue(SVMessage.Body, XVSPositions.ORI, "ORI/")
            Parameters("@ORIAgency").Value = Left(TestValue, 7)
            Parameters("@ORIWorkstation").Value = IIf(Len(TestValue) = 9, Right(TestValue, 2), "")
            Parameters("@WACICNumber").Value = GetFieldValue(SVMessage.Body, XVSPositions.WACIC)

            'get the first record identifier
            TestValue = GetFieldValue(SVMessage.Body, XVSPositions.RecordIdentifier)
            Select Case Left(TestValue, 4)
                Case "LIC/"
                    Parameters("@LicensePlate").Value = Right(TestValue, Len(TestValue) - 4)
                Case "VIN/"
                    Parameters("@VIN").Value = Right(TestValue, Len(TestValue) - 4)
                    'Case "WAC/"
                    '4-5-04  - we now grab WACIC from its fixed position so ignore it if repeated
                    'Parameters("@WACICNumber").Value = Right(TestValue, Len(TestValue) - 4)

                    'Case Else
                    '    'must be LIC, VIN, or WAC 
                    '    Return ProcessStatus.BadData
            End Select

            'get the OCA
            TestValue = GetFieldValue(SVMessage.Body, XVSPositions.OCA)
            If Left(TestValue, 4) <> "OCA/" Then
                'if its not an OCA/ log an event - the validation below will set the return value
                LogEvent("ParseMessageCancel", "Record Identifier #2 not an OCA (" & TestValue & ")", MessageType.Information, LogType.Standard)
            Else
                'ok to proceed
                Parameters("@OCANumber").Value = Right(TestValue, Len(TestValue) - 4)
            End If

            'get the DateOfCancel
            TestValue = GetFieldValue(SVMessage.Body, XVSPositions.DateOfCancel, "DOC/")
            Try
                Parameters("@DateTimeOfCancel").Value = ConvertYYYYMMDDToDate(TestValue)
            Catch ex As Exception
                'use the current date
                Parameters("@DateTimeOfCancel").Value = FormatDateTime(Now, DateFormat.ShortDate)
            End Try

            'validate input
            If Parameters("@WACICNumber").Value = "" Then
                '4-5-2004 - we only require the WACIC number now

                'fail parsing due to missing data in required fields
                LogEvent("ParseMessageCancel", "Error Parsing XVS. Missing required fields." & vbCrLf & _
                    ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)

                Return ProcessStatus.BadData
            Else
                'no errors, return ok
                Return ProcessStatus.Unprocessed
            End If
        Catch ex As Exception
            'error parsing Cancel
            'log event
            LogEvent("ParseMessageCancel", "Error Parsing XVS. Error=" & ex.Message & vbCrLf & ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)
            'return fail
            Return ProcessStatus.Fail
        End Try
    End Function

    Private Function ParseMessageClear(ByVal SVMessage As WSPMessage, ByRef Parameters As Collection) As ProcessStatus
        'Purpose:   Parse out a CVS message and save it to the database.
        'Inputs:    SVMessage is the message object being checked.
        '           Parameters is the collection of SqlParameters passed ByRef so we can set the corrected values if necessary.
        'Output:    Unprocessed if OK, BadData if parseable but fails validation, Fail if parsing fails.

        'Expected format from Vehicle File.doc
        '=====================
        'Message Key	CVS
        'Originating Agency Identifier	WA0340500
        'WACIC 
        'Record Identifier	LIC/123XYZ
        'Record Identifier	OCA/99-12345
        'Date of Clear	20000228
        'Locating Agency's ORI	WA0340100
        'Locating Agency's Case Number	00-54321
        'CVS.WA0270000.02V0159516.LIC/217NBI.OCA/022670825.20040331^M^J

        Try
            Parameters("@MessageKey").Value = Left(GetFieldValue(SVMessage.Body, CVSPositions.MessageKey), 3)
            Dim TestValue As String = GetFieldValue(SVMessage.Body, CVSPositions.ORI, "ORI/")
            Parameters("@ORIAgency").Value = Left(TestValue, 7)
            Parameters("@ORIWorkstation").Value = IIf(Len(TestValue) = 9, Right(TestValue, 2), "")
            Parameters("@WACICNumber").Value = GetFieldValue(SVMessage.Body, CVSPositions.WACIC)

            'get the first record identifier
            TestValue = GetFieldValue(SVMessage.Body, CVSPositions.RecordIdentifier)
            Select Case Left(TestValue, 4)
                Case "LIC/"
                    Parameters("@LicensePlate").Value = Right(TestValue, Len(TestValue) - 4)
                Case "VIN/"
                    Parameters("@VIN").Value = Right(TestValue, Len(TestValue) - 4)

                    'Case "WAC/"
                    '4-5-04  - we now grab WACIC from its fixed position so ignore it if repeated
                    'Parameters("@WACICNumber").Value = Right(TestValue, Len(TestValue) - 4)

                    'Case Else
                    '    'must be LIC, VIN, or WAC (or OCA if the two fields are swapped?)
                    '    Return ProcessStatus.BadData
            End Select

            'get the OCA 
            TestValue = GetFieldValue(SVMessage.Body, CVSPositions.OCA)
            If Left(TestValue, 4) <> "OCA/" Then
                'if its not an OCA/ log an event - the validation below will set the return value
                LogEvent("ParseMessageClear", "Record Identifier #2 not an OCA (" & TestValue & ")", MessageType.Information, LogType.Standard)
            Else
                Parameters("@OCANumber").Value = Right(TestValue, Len(TestValue) - 4)
            End If

            'get the DateOfClear
            TestValue = GetFieldValue(SVMessage.Body, CVSPositions.DateOfClear, "DCL/")
            Try
                Parameters("@DateTimeOfClear").Value = ConvertYYYYMMDDToDate(TestValue)
            Catch ex As Exception
                'use the current date
                Parameters("@DateTimeOfClear").Value = FormatDateTime(Now, DateFormat.ShortDate)
            End Try
            'get the optional RRI
            Parameters("@RecovAgencyID").Value = GetFieldValue(SVMessage.Body, CVSPositions.RecoveringAgencyID, "RRI/")
            'get the optional RCA
            Parameters("@RecovAgencyCaseNo").Value = GetFieldValue(SVMessage.Body, CVSPositions.RecoveringAgencyCaseNumber, "RCA/")

            'validate input
            If Parameters("@WACICNumber").Value = "" Then
                '4-5-2004 - we only require the WACIC number now

                'fail parsing due to missing data in required fields
                LogEvent("ParseMessageClear", "Error Parsing CVS. Missing required fields." & vbCrLf & _
                    ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)
                Return ProcessStatus.BadData
            Else
                'no errors, return unprocessed status
                Return ProcessStatus.Unprocessed
            End If
        Catch ex As Exception
            'error parsing clear

            'log event
            LogEvent("ParseMessageClear", "Error Parsing CVS. Error=" & ex.Message & vbCrLf & ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)

            'return fail
            Return ProcessStatus.Fail
        End Try
    End Function

    Private Function ParseMessageEnter(ByVal SVMessage As WSPMessage, ByRef Parameters As Collection) As ProcessStatus
        'Purpose:   Parse out an EVS message and save it to the database
        'Inputs:    SVMessage is the message object being checked.
        '           Parameters is the collection of SqlParameters passed ByRef so we can set the corrected values if necessary.
        'Output:    Unprocessed if OK, BadData if parseable but fails validation, Fail if parsing fails.

        'parse the message and populate the parameters with the data
        'EVS.WA031023N.04V0046945.898LNU.WA.2004.PC.JHMBB1176RC000227.1994.HOND.PRE.2D.BLK.20040331.20040579.425 774 3583 SNOCOM  IMP OK^M^J
        'EVS.WA0310000.04V0046948.....M00170X011278.1990.DEER.FE.TF.GRN.20040331.04-7599.NO IMP/V 4253883523, RO CLL 360 631 1449^M^J

        Try
            Parameters("@MessageKey").Value = Left(GetFieldValue(SVMessage.Body, EVSPositions.MessageKey), 3)
            Dim TestValue As String = GetFieldValue(SVMessage.Body, EVSPositions.ORI)
            Parameters("@ORIAgency").Value = Left(TestValue, 7)
            Parameters("@ORIWorkstation").Value = IIf(Len(TestValue) = 9, Right(TestValue, 2), "")
            Parameters("@WACICNumber").Value = GetFieldValue(SVMessage.Body, EVSPositions.WACIC)
            Parameters("@LicensePlate").Value = GetFieldValue(SVMessage.Body, EVSPositions.LicencePlate)
            Parameters("@LicenseState").Value = GetFieldValue(SVMessage.Body, EVSPositions.LicenseState)
            Parameters("@LicenseYearExp").Value = GetFieldValue(SVMessage.Body, EVSPositions.LicenseYearExpire)
            Parameters("@LicensePlateType").Value = GetFieldValue(SVMessage.Body, EVSPositions.LicensePlateType)
            Parameters("@VIN").Value = GetFieldValue(SVMessage.Body, EVSPositions.VIN)
            Parameters("@VehicleYear").Value = GetFieldValue(SVMessage.Body, EVSPositions.VehYear)
            Parameters("@VehicleMake").Value = GetFieldValue(SVMessage.Body, EVSPositions.VehMake)
            Parameters("@VehicleModel").Value = GetFieldValue(SVMessage.Body, EVSPositions.VehModel)
            Parameters("@VehicleStyle").Value = GetFieldValue(SVMessage.Body, EVSPositions.VehStyle)
            Parameters("@VehicleColor").Value = GetFieldValue(SVMessage.Body, EVSPositions.VehColor)
            Parameters("@DateOfTheft").Value = ConvertYYYYMMDDToDate(GetFieldValue(SVMessage.Body, EVSPositions.DateOfTheft))
            Parameters("@OCANumber").Value = GetFieldValue(SVMessage.Body, EVSPositions.OCA)
            Parameters("@Miscellaneous").Value = GetFieldValue(SVMessage.Body, EVSPositions.Misc)
            Parameters("@OANNumber").Value = GetFieldValue(SVMessage.Body, EVSPositions.OAN)

            'validate input
            If Parameters("@WACICNumber").Value = "" Then
                'WACIC should enforce required fields but we check the bits we may need to associate records
                'fail parsing due to missing data in required fields
                LogEvent("ParseMessageEntry", "Error Parsing EVS. Missing required fields." & vbCrLf & _
                    ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)
                'return didn't pass muster
                Return ProcessStatus.BadData
            Else
                'Parameters("@ProcessedStatus").Value = ProcessStatus.Unprocessed
                'no errors, return ok
                Return ProcessStatus.Unprocessed
            End If
        Catch ex As Exception
            'error parsing Entry
            'log event
            LogEvent("ParseMessageEntry", "Error Parsing EVS. Error=" & ex.Message & vbCrLf & ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)
            'return fail
            Return ProcessStatus.Fail
        End Try
    End Function

    Private Function ParseMessageModify(ByVal SVMessage As WSPMessage, ByRef Parameters As Collection) As ProcessStatus
        'Purpose:   Parse out a MVS message and save it to the database
        'Inputs:    SVMessage is the message object being checked.
        '           Parameters is the collection of SqlParameters passed ByRef so we can set the corrected values if necessary.
        'Output:    Unprocessed if OK, BadData if parseable but fails validation, Fail if parsing fails.

        'Expected format from Vehicle File.doc
        '=====================
        'Message Key	MVS
        'Originating Agency Identifier	WA0340500
        'WACIC 
        'Record Identifier	LIC/123XYZ
        'Record Identifier	OCA/99-12345
        'Modified Field(s)	VCO/BLU.VST/4T
        'MVS.WASPD0000.04V0045420.LIC/002SEY.OCA/04-124949.MKE/EVSA.MIS/CAUTION-SUSPECT ARMED W/GUN^M^J

        Try
            Parameters("@MessageKey").Value = Left(GetFieldValue(SVMessage.Body, MVSPositions.MessageKey), 3)
            Dim TestValue As String = GetFieldValue(SVMessage.Body, MVSPositions.ORI, "ORI/")
            Parameters("@ORIAgency").Value = Left(TestValue, 7)
            Parameters("@ORIWorkstation").Value = IIf(Len(TestValue) = 9, Right(TestValue, 2), "")
            Parameters("@WACICNumber").Value = GetFieldValue(SVMessage.Body, MVSPositions.WACIC)

            'get the first record identifier
            TestValue = GetFieldValue(SVMessage.Body, MVSPositions.RecordIdentifier)
            Select Case Left(TestValue, 4)
                Case "LIC/"
                    Parameters("@LicensePlate").Value = Right(TestValue, Len(TestValue) - 4)
                Case "VIN/"
                    Parameters("@VIN").Value = Right(TestValue, Len(TestValue) - 4)
                    '4-5-04  - we now grab WACIC from its fixed position so ignore it if repeated
                    'Case "WAC/"
                    '    Parameters("@WACICNumber").Value = Right(TestValue, Len(TestValue) - 4)
                Case "OAN/"
                    Parameters("@OANNumber").Value = Right(TestValue, Len(TestValue) - 4)
            End Select

            'get the OCA
            TestValue = GetFieldValue(SVMessage.Body, MVSPositions.OCA)
            If Left(TestValue, 4) <> "OCA/" Then
                'if its not an OCA/ log an event - the validation below will set the return value
                LogEvent("ParseMessageModify", "Record Identifier #2 not an OCA (" & TestValue & ")", MessageType.Information, LogType.Standard)
            Else
                Parameters("@OCANumber").Value = Right(TestValue, Len(TestValue) - 4)
            End If

            'determine if this is a modify we care about
            Parameters("@ProcessedStatus").Value = ParseModifiedFields(SVMessage, Parameters)

            'validate input ONLY IF we care about this record
            If Parameters("@ProcessedStatus").Value = ProcessStatus.Unprocessed AndAlso _
                Parameters("@WACICNumber").Value = "" Then
                'must have ORI, OCA, and either LIC, WAC, or VIN

                'fail parsing due to missing data in required fields
                LogEvent("ParseMessageModify", "Error Parsing MVS. Missing required fields." & vbCrLf & _
                    ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)
                Return ProcessStatus.BadData
            Else
                'if here, its either Unprocessed AND it passed validation or its a Skip 
                'no errors, return value determined by ParseModifiedFields
                Return Parameters("@ProcessedStatus").Value
            End If
        Catch ex As Exception
            'error parsing Modify
            'log event
            LogEvent("ParseMessageModify", "Error Parsing MVS. Error=" & ex.Message & vbCrLf & ReconstructRawMessage(SVMessage), MessageType.Error, LogType.Standard)
            'return fail
            Return ProcessStatus.Fail
        End Try
    End Function

    Private Function ParseModifiedFields(ByVal SVMessage As WSPMessage, ByRef Parameters As Collection) As ProcessStatus

        'Purpose:   Look at all of the remaining fields to determine if it contains a modified LIC, VIN, or VYR. If so,
        '           extract the corrected fields and return a status of unprocessed. If not, return a status of Skip.
        'Inputs:    SVMessage is the message object being checked.
        '           Parameters is the collection of SqlParameters passed ByRef so we can set the corrected values if necessary.
        'Output:    Returns an enumerated status value of zero (Unprocessed) or three (Skip).

        'find the starting position by counting field delimiters (.) 
        'we are looking for the fourth position
        Dim CurrentPosition As Integer = 0
        Dim NextPosition As Integer = 1
        Dim CurrentCount As Integer = 0
        Do
            CurrentPosition = InStr(NextPosition, SVMessage.Body, ".")
            NextPosition = CurrentPosition + 1
            If CurrentPosition > 0 Then
                CurrentCount += 1
            End If
        Loop Until CurrentPosition < 1 Or CurrentCount = 4

        If CurrentPosition > 0 Then
            'we have our location to start search. Get the modified fields 
            Dim ModifiedFields As String = Mid(SVMessage.Body, CurrentPosition + 1)

            'parse the modified fields into their SqlParameters
            'set the initial starting point
            CurrentPosition = 1
            Do
                'SetModifiedField gets the current value then returns the next starting point
                CurrentPosition = SetModifiedField(CurrentPosition, ModifiedFields, Parameters)
                'when the last field is reached, SetModifiedField will return zero
            Loop While CurrentPosition <> 0

            'check to see if the modified fields section contained a field of interest
            If InStr(ModifiedFields, "VIN/") > 0 Or InStr(ModifiedFields, "LIC/") > 0 _
                Or InStr(ModifiedFields, "VYR/") > 0 Or InStr(ModifiedFields, "OAN/") > 0 Then
                'we care - get the corrected values, set the processed status
                Return ProcessStatus.Unprocessed
            Else

                'its nothing we care about
                Return ProcessStatus.Skip
            End If
        Else
            'nothing to modify??? not likely but handle it
            Return ProcessStatus.Skip
        End If
    End Function

    Private Sub ProcessQueue(ByVal State As Object)
        'Purpose: Main thread to monitor the queue for inquiries.

        'loop here while service is running
        While ServiceState = ServiceStates.Running

            Dim QueueMessage As New WA.DOL.VSD.WSPQueue.WSPMessage

            Try
                'if there is at lease one message in the queue 
                If QueueRXObject.CanRead = True Then
                    'read all the messages in the queue or until we've been shutdown
                    Do While QueueRXObject.CanRead And ServiceState = ServiceStates.Running
                        'fetch the queue message
                        QueueMessage.Body = ""
                        QueueMessage = QueueRXObject.ReadMessage

                        If DebugMode > 0 Then
                            'log the raw message
                            LogEvent("ProcessQueue", ReconstructRawMessage(QueueMessage), MessageType.Debug, LogType.Standard)
                        End If

                        'create the empty parameter collection
                        Dim colParameters As Collection = CreateParameterCollection()
                        Dim MessageStatus As ProcessStatus = ProcessStatus.Fail 'local param so I don't have to CType all of the ParseXXX return values

                        'handle goofy data from WSP
                        If Right(QueueMessage.Body, 4) = "^M^J" Then
                            QueueMessage.Body = Left(QueueMessage.Body, Len(QueueMessage.Body) - 4)
                        End If

                        'need code to evaluate each type of message then capture results from parsing
                        Select Case DetermineMessageType(QueueMessage.Body)
                            Case SVMessageTypes.Enter
                                MessageStatus = ParseMessageEnter(QueueMessage, colParameters)
                            Case SVMessageTypes.Modify
                                MessageStatus = ParseMessageModify(QueueMessage, colParameters)
                            Case SVMessageTypes.Clear
                                MessageStatus = ParseMessageClear(QueueMessage, colParameters)
                            Case SVMessageTypes.Cancel
                                MessageStatus = ParseMessageCancel(QueueMessage, colParameters)
                            Case Else
                                'unknown - result is default (Fail)
                                LogEvent("ProcessQueue", "Unknown message: " & ReconstructRawMessage(QueueMessage), MessageType.Error, LogType.Standard)
                        End Select

                        'set the processed status based on how the message was parsed
                        colParameters("@ProcessedStatus").Value = MessageStatus

                        'update this if business rules dictate. Until then, convert BadData to Unprocessed and let the update service deal with it
                        If MessageStatus = ProcessStatus.BadData Then
                            colParameters("@ProcessedStatus").Value = ProcessStatus.Unprocessed
                        End If

                        If MessageStatus <> ProcessStatus.Fail Then
                            'only try to save records that were successfully parsed (Unprocessed, BadData, or Skip)
                            Try
                                Call WriteRecord(colParameters)
                                'if everything worked, reset our last email sent value
                                LastEmailSent = Now.AddDays(-1)
                            Catch ex As Exception
                                'message was parsed but unable to be saved to the database
                                'log an error
                                LogEvent("ProcessQueue", "Database write error: " & ex.Message & " for message " & ReconstructRawMessage(QueueMessage), MessageType.Error, LogType.Standard)

                                'put back into queue 
                                If QueueMessage.Body <> "" Then
                                    'return the message to the queue if it exists
                                    ReturnMessage(QueueMessage)
                                End If

                                'sleep for awhile
                                Thread.Sleep(QueueSleepWhenEmpty)
                                'exit the Try/Catch and let control fall back into the loop after our nap
                                Exit Try
                            End Try
                        Else
                            'if here, we did NOT successfully parse the message!
                            'Do not put it back in the queue here since its a parsing error.
                            LogEvent("ProcessQueue", "Parse Failed for message: " & ReconstructRawMessage(QueueMessage), MessageType.Error, LogType.Standard)
                        End If
                    Loop
                Else
                    'queue is empty, take a nap
                    Thread.Sleep(QueueSleepWhenEmpty)
                End If
            Catch ex As Exception
                'some error occurred either reading the queue or somewhere in the Do/Loop prior to 
                'the Try/Catch that writes to the database

                'log message
                LogEvent("ProcessQueue", "Process Queue Exception: " & ex.Message & vbCrLf & ReconstructRawMessage(QueueMessage), MessageType.Error, LogType.Standard)

                'not likely to have a message but handle it just in case
                If QueueMessage.Body <> "" Then
                    'return the message to the queue if it exists
                    ReturnMessage(QueueMessage)
                End If

                'sleep for awhile
                Thread.Sleep(QueueSleepWhenEmpty)
            End Try
        End While
    End Sub

    Private Sub ReadAppSettings()
        'Purpose:   Read the necessary application settings

        On Error Resume Next
        Dim objDB As WA.DOL.Data.SqlHelper

        Dim Reader As SqlClient.SqlDataReader
        Reader = objDB.ExecuteReader(ConnectString, CommandType.StoredProcedure, SP_SELECT_CONFIG, New SqlClient.SqlParameter("@ProgramName", Me.ServiceName))
        While Reader.Read
            Select Case UCase(Reader("KeyName"))
                Case UCase("QueueServer")
                    'get the queue server
                    QueueServer = Trim(CType(Reader("KeyValue"), String))
                Case UCase("QueueRX")
                    'get the queue name
                    QueueRxName = Trim(CType(Reader("KeyValue"), String))
                Case UCase("QueueSleep")
                    'get the queue sleep interval
                    If IsNumeric(Reader("KeyValue")) AndAlso CType(Reader("KeyValue"), Integer) > 0 Then
                        'the db keeps the number in seconds but we need it in milliseconds, so fix it
                        QueueSleepWhenEmpty = CType(Reader("KeyValue"), Integer) * 1000
                    End If
                Case UCase("Debug")
                    'get the debug mode
                    If IsNumeric(Reader("KeyValue")) AndAlso CType(Reader("KeyValue"), Byte) >= 0 Then
                        'make sure the value can be converted to a byte
                        DebugMode = CType(Reader("KeyValue"), Byte)
                    End If
                Case UCase("EmailFrequency")
                    'get the queue sleep interval
                    If IsNumeric(Reader("KeyValue")) AndAlso CType(Reader("KeyValue"), Integer) > 0 Then
                        EmailFrequency = CType(Reader("KeyValue"), Integer)
                    End If
            End Select
        End While
        Reader.Close()
        Reader = Nothing

        'load the MessageFieldMap datatable
        MessageFieldMap = objDB.ExecuteDataset(ConnectString, CommandType.StoredProcedure, _
            SP_SELECT_MESSAGE_FIELDS, New SqlClient.SqlParameter("@pstrTableName", STOLEN_VEHICLE_TABLE)).Tables(0)

        objDB = Nothing
    End Sub

    Private Function ReconstructRawMessage(ByVal WSPMessage As WSPMessage) As String
        'Purpose:   Take the parsed WSP Message and reconstruct it to its string form
        'Inputs:    WSPMessage is a WSPMessage class.
        'Output:    Returns a string containing the various WSPMessage elements.
        Return WSPMessage.OriginatingID.PadRight(5, " ") & WSPMessage.Auxiliary.PadRight(4, " ") & _
            WSPMessage.Mnemonic.PadRight(5, " ") & WSPMessage.Delimiter & WSPMessage.Body
    End Function

    Private Sub ReturnMessage(ByVal QueueMessage As WSPMessage)
        'Purpose:   Used if we need to return a message to the queue
        Dim QueueRxObject As New WA.DOL.VSD.WSPQueue.QueueObject(QueueServer, QueueRxName)
        QueueRxObject.SendMessage(QueueMessage)
        QueueRxObject = Nothing
    End Sub

    Private Function SetModifiedField(ByVal StartingPosition As Integer, _
        ByVal ModifiedFields As String, _
        ByRef Parameters As Collection) As Integer

        'Purpose:   Parse all modified fields into their appropriate column
        'Inputs:    StartingPosition - starting position within the string to begin parsing
        '           ModifiedFields - string containing all of the modified fields
        '           Parameters - ByRef collection of parameters so we can populate the 
        '           appropriate parameter by the MFC.

        Dim NextPeriod As Integer = InStr(StartingPosition, ModifiedFields, ".")
        If NextPeriod = 0 Then
            'if no "next period" was found, this is the last field
            NextPeriod = Len(ModifiedFields) + 1
        End If

        'validation
        If StartingPosition > NextPeriod Or StartingPosition < 1 Then
            'shouldn't happen but validate the parsing pointers
            Throw New Exception("Bad starting position (" & CType(StartingPosition, String) & ")")
        End If

        Dim FullValue As String = Mid(ModifiedFields, StartingPosition, NextPeriod - StartingPosition)

        If Len(FullValue) > 3 AndAlso Mid(FullValue, 4, 1) = "/" Then
            'we have a value, find its MFC in our table
            MessageFieldMap.DefaultView.RowFilter = "MessageFieldCode='" & Left(FullValue, 3) & "'"
            If MessageFieldMap.DefaultView.Count = 1 Then
                'we have a match in our table

                'get the value w/out the MFC/
                Dim FieldValue As String = Right(FullValue, Len(FullValue) - 4)

                'see if it has a corrected column
                If Not IsDBNull(MessageFieldMap.DefaultView(0)("CorrectionColumn")) Then
                    'corrected column
                    CType(Parameters.Item("@" & MessageFieldMap.DefaultView(0)("CorrectionColumn")), SqlClient.SqlParameter).Value = FieldValue
                Else
                    'don't care column
                    If UCase(Left(MessageFieldMap.DefaultView(0)("ColumnName"), 4)) = "DATE" Then
                        'special case for date fields
                        Try
                            CType(Parameters.Item("@" & MessageFieldMap.DefaultView(0)("ColumnName")), SqlClient.SqlParameter).Value = ConvertYYYYMMDDToDate(FieldValue)
                        Catch ex As Exception
                            'modified date error - shouldn't happen but log it and continue
                            LogEvent("SetModifiedField", "Error converting " & MessageFieldMap.DefaultView(0)("ColumnName") & " field '" & FieldValue & "'. " & ex.Message, _
                                MessageType.Error, LogType.Standard)
                        End Try
                    ElseIf UCase(Left(FullValue, 3)) = "WAC" Then
                        'special case for modifying WACIC -- shouldn't happen (or if it does, the modified and old value should match).
                        If Parameters("@WACICNumber").Value <> FieldValue Then
                            'If the old and new value do not match, log an event but allow through
                            LogEvent("SetModifiedField", "Message attempted to modify the WACICNumber and the old and new values do not match. Old=" & Parameters("@WACICNumber").Value & ". New=" & FieldValue, MessageType.Error, LogType.Standard)
                        End If
                    ElseIf UCase(Left(FullValue, 3)) = "MKE" Then
                        'special case for MKE which we do NOT want to update since this can result in multiple EVS records
                        'do nothing here
                    ElseIf UCase(Left(FullValue, 3)) = "ORI" Then
                        'special case for ORI which doesn't 1:1 map MFC to column 
                        Parameters("@ORIAgency").Value = Left(FieldValue, 7)
                        Parameters("@ORIWorkstation").Value = IIf(Len(FieldValue) = 9, Right(FieldValue, 2), "")
                    Else
                        'some other don't care column
                        Try
                            CType(Parameters.Item("@" & MessageFieldMap.DefaultView(0)("ColumnName")), SqlClient.SqlParameter).Value = FieldValue
                        Catch ex As Exception
                            'if not found in our collection, an error will occur
                            'MFC doesn't match???? shouldn't happen but log this
                            LogEvent("SetModifiedField", "Unable to map MFC to collection parameter '" & Left(FullValue, 4) & "' to '" & _
                                MessageFieldMap.DefaultView(0)("ColumnName") & "'", MessageType.Error, LogType.Standard)
                        End Try
                    End If
                End If
            Else
                'MFC doesn't match???? shouldn't happen but log this
                LogEvent("SetModifiedField", "Unknown MFC code '" & Left(FullValue, 4) & _
                    "' found in string '" & ModifiedFields & "'", MessageType.Error, LogType.Standard)
            End If
            MessageFieldMap.DefaultView.RowFilter = ""
        Else
            'no MFC?? shouldn't happen but log this
            LogEvent("SetModifiedField", "Bad MFC code '" & Left(FullValue, 4) & "' found in string '" & _
                ModifiedFields & "'", MessageType.Error, LogType.Standard)
        End If

        'we return the next starting position which is zero if no more periods exist
        'or 1 + the next period
        If NextPeriod = Len(ModifiedFields) + 1 Then
            'if we set the NextPeriod to the length of the ModifiedFields + 1, then it means we
            'just parsed the last field, so we want to return zero.
            NextPeriod = 0
        Else
            'otherwise, return the new starting position as 1 + the next period
            NextPeriod += 1
        End If
        Return NextPeriod

    End Function

    Private Function ValidSettings() As Boolean
        'Purpose:   Verify we have valid settings
        If QueueRxName = "" Or QueueServer = "" Or ConnectString = "" Then
            Return False
        End If
        Return True
    End Function

    Private Function WriteRecord(ByVal Parameters As Collection) As Integer
        'Purpose:   Write the record to the database.
        'Inputs:    Parameters is a collection of SqlParameter objects
        'Output:    Returns the new record id.

        Dim objDB As WA.DOL.Data.SqlHelper

        'convert collection to array of SqlParameter objects
        Dim SqlParameters(Parameters.Count - 1) As SqlClient.SqlParameter
        Dim Index As Integer = 0
        For Index = 0 To Parameters.Count - 1
            SqlParameters(Index) = CType(Parameters.Item(Index + 1), SqlClient.SqlParameter)
        Next

        'save the record to the db
        Return objDB.ExecuteScalar(ConnectString, CommandType.StoredProcedure, SP_INSERT_RECORD, SqlParameters)

    End Function
End Class
