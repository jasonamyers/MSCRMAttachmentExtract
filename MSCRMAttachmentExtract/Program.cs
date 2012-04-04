using System;
using System.Globalization;
using System.Text;
using System.Data.SqlClient;
using System.IO;

namespace MSCRMAttachmentExtract
{
    class Program
    {
        /// <summary>
        /// Function to save byte array to a file
        /// </summary>
        /// <param name="fileName">File name to save byte array</param>
        /// <param name="byteArray">Byte array to save to external file</param>
        /// <returns>Return true if byte array save successfully, if not return false</returns>
        public static bool ByteArrayToFile(string fileName, byte[] byteArray)
        {
            try
            {
                // Open file for reading
                var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);

                // Writes a block of bytes to this stream using data from a byte array.
                fileStream.Write(byteArray, 0, byteArray.Length);
                //Console.WriteLine("Inside FIle Write");

                // close file stream
                fileStream.Close();

                return true;
            }
            catch (Exception exception)
            {
                // Error
                Console.WriteLine("Exception caught in process: {0}", exception);
            }

            // error occured, return false
            return false;
        }

        /// <summary>
        /// Pulls all the nonemail attachments.
        /// </summary>
        /// <param name="connString">The conn string.</param>
        /// <returns></returns>
        public static bool NonEmailAttachmentExtraction(string connString)
        {
            const string queryNonEmailAttachments = "SELECT DocumentBody, ObjectId, FileName, ObjectTypeCode, ObjectTypeCodeName, CONVERT(varchar(255), ObjectID) + '_' + CONVERT(varchar(255), Filename) AS SaveAsFileName FROM FilteredAnnotationUS WHERE IsDocument = 1 AND (objecttypecode = 1 or objecttypecode = 2 or objecttypecode = 3 or objecttypecode = 4 or objecttypecode = 4212 or objecttypecode = 4201 or objecttypecode = 4210) order by ObjectTypeCode";
            using (var connection = new SqlConnection(connString))
            {
                //Console.WriteLine("Prior to SQL");
                var command = new SqlCommand(queryNonEmailAttachments, connection);
                try
                {
                    connection.Open();
                    command.CommandTimeout = 300000;
                    SqlDataReader reader = command.ExecuteReader();
                    //Console.WriteLine("After reader");
                    var file = new StreamWriter(@"C:\\CRM\\NONEMAIL\\NonEmailMaster.csv", true);
                    file.WriteLine("\"ObjectId\",\"FileName\",\"ObjectTypeCode\",\"ObjectTypeCodeName\",\"SaveAsFileName\"");
                    int i = 0;
                    while (reader.Read())
                    {
                        string filePathAndName = "C:\\CRM\\NONEMAIL\\" + reader.GetString(5);
                        /* 
                         * Converts Latin-1 from the CRM database to UTF8 so we can use Base64 decoding to recreate the original file.
                         */
                        string fileDataString = Encoding.UTF8.GetString(Encoding.GetEncoding(1252).GetBytes(reader.GetString(0)));
                        byte[] fileBytes = Convert.FromBase64String(fileDataString);

                        /*
                         * Writes the original file to disk
                         */
                        bool fileWritten = ByteArrayToFile(filePathAndName, fileBytes);
                        if(fileWritten == false)
                            throw new Exception("File Could not be written.");

                        /*
                         * Adds the record to the CSV reference file.
                         */
                        string objectTypeName;
                        switch (reader.GetInt32(3))
                        {
                            case 1:
                                objectTypeName = "Account";
                                break;
                            case 2:
                                objectTypeName = "Contact";
                                break;
                            case 3:
                                objectTypeName = "Opportunity";
                                break;
                            case 4:
                                objectTypeName = "Lead";
                                break;
                            case 4212:
                                objectTypeName = "All Task";
                                break;
                            case 4201:
                                objectTypeName = "All Appointment";
                                break;
                            case 4210:
                                objectTypeName = "All Phone Call";
                                break;
                            default:
                                objectTypeName = "";
                                break;
                        }


                        //Console.WriteLine("Prior to CSV");
                        file.WriteLine("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\"", reader.GetValue(1), reader.GetValue(2), reader.GetValue(3), objectTypeName, reader.GetValue(5));
                        //Console.WriteLine("Post CSV");
                        i++;
                    }
                    Console.WriteLine("Processed {0} non email records.", i);
                    reader.Close();
                    file.Close();
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                return false;
            }
        }

        /// <summary>
        /// Pulls all the Email attachments.
        /// </summary>
        /// <param name="connString">The conn string.</param>
        /// <returns></returns>
        public static bool EmailAttachmentExtraction(string connString)
        {
            const string queryEmailAttachments = "SELECT EA.Body,  EA.ActivityID,  EA.AttachmentNumber, E.RegardingObjectId, E.RegardingObjectIdName, E.RegardingObjectTypeCode, CASE WHEN E.RegardingObjectTypeCode = 1 THEN 'Account' ELSE CASE WHEN E.RegardingObjectTypeCode = 2 THEN 'Contact' ELSE CASE WHEN E.RegardingObjectTypeCode = 3 THEN 'Opportunity' ELSE CASE WHEN E.RegardingObjectTypeCode = 4 THEN 'Lead' ELSE 'Unknown' END END END END AS RegardingObjectTypeCodeName, EA.FileName,CONVERT(varchar(255), E.regardingobjectid) + '_' + CONVERT(varchar(255), EA.AttachmentNumber) + '_' + CONVERT(varchar(255), EA.Filename) AS SaveAsFileName FROM  FilteredEmailUS E JOIN  ActivityMimeAttachment EA ON E.ActivityID = EA.ActivityID WHERE  E.regardingobjecttypecode = 1 or E.regardingobjecttypecode = 2 or E.regardingobjecttypecode = 3 or E.regardingobjecttypecode = 4";
            using (var connection = new SqlConnection(connString))
            {
                var command2 = new SqlCommand(queryEmailAttachments, connection);
                try
                {
                    connection.Open();
                    command2.CommandTimeout = 300000;
                    var reader = command2.ExecuteReader();
                    var file = new StreamWriter(@"C:\\CRM\\\\EMAIL\\EmailMaster.csv", true);
                    file.WriteLine("\"ActivityId\",\"AttachmentNumber\",\"RegardingObjectId\",\"RegardingObjectIdName\",\"RegardingObjectTypeCode\",\"RegardingObjectTypeCodeName\",\"FileName\",\"SaveAsFileName\"");
                    var i = 0;
                    while (reader.Read())
                    {

                        var filePathAndName = "C:\\CRM\\EMAIL\\" + reader.GetString(8);
                        var fileDataString = Encoding.UTF8.GetString(Encoding.GetEncoding(1252).GetBytes(reader.GetString(0)));
                        var fileBytes = Convert.FromBase64String(fileDataString);
                        var fileWritten = ByteArrayToFile(filePathAndName, fileBytes);
                        if (fileWritten == false)
                            throw new Exception("File Could not be written.");

                        file.WriteLine("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\"", reader.GetValue(1), reader.GetValue(2), reader.GetValue(3), reader.GetValue(4), reader.GetValue(5), reader.GetValue(6), reader.GetValue(7), reader.GetValue(8));
                        i++;
                    }
                    Console.WriteLine("Processed {0} email records.", i);
                    reader.Close();
                    file.Close();
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                return false;
            }
        }

        public static string GetAndWriteData(string connString, string queryString, string fileNameString, string headerString)
        {

            using (var connection = new SqlConnection(connString))
            {
                //Console.WriteLine("In SQL CONN");
                var command2 = new SqlCommand(queryString, connection);
                //Console.WriteLine("In SQL Cmd");
                try
                {
                    connection.Open();
                    //Console.WriteLine("Conn Open");
                    command2.CommandTimeout = 300000;
                    var reader = command2.ExecuteReader();
                    fileNameString = "C:\\CRM\\\\Exports\\" + fileNameString;
                    //Console.WriteLine(fileNameString);
                    var file = new StreamWriter(@fileNameString, true);
                    file.WriteLine(headerString);
                    var i = 0;
                    while (reader.Read())
                    {
                        //Console.WriteLine("InReader");
                        int i2;
                        string value;
                        for (i2 = 0; i2 < reader.FieldCount - 2; i2++)
                        {

                            value = reader.GetValue(i2).ToString().Replace(Environment.NewLine, ",").Replace("\"", "\\\"").Replace("\'", "\\'").Replace("\n", " ");
                            file.Write("\"{0}\",", value);
                        }

                        value = reader.GetValue(i2).ToString().Replace(Environment.NewLine, ",").Replace("\"", "\\\"").Replace("\'", "\\'").Replace("\n", " ");
                        file.WriteLine("\"{0}\"", value);
                        i++;
                    }
                    reader.Close();
                    file.Close();
                    return string.Format("{0}: Processed {1} records", fileNameString, i);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                return string.Format("Failed");
            }

        }

        static void Main()
        {
            const string connString = "Server=hostname;Database=DB_MSCRM;User Id=userName;password=passWord";

            #region NonEmailAttachments
            var nonEmailExecuted = NonEmailAttachmentExtraction(connString);
            if (nonEmailExecuted == false)
                Console.WriteLine("NonEmailAttachmentExtraction went bad!");
            #endregion

            #region EmailAttachments
            var emailExecuted = EmailAttachmentExtraction(connString);
            if (emailExecuted == false)
                Console.WriteLine("NonEmailAttachmentExtraction went bad!");
            #endregion

            #region AccountExtraction
            var accountQueries = new[,]
                {
                {"Accounts.csv",
                    "\"accountcategorycode\",\"accountcategorycodename\",\"accountid\",\"accountnumber\",\"accountratingcodename\",\"address1_city\",\"address1_line1\",\"address1_line2\",\"address1_name\",\"address1_postalcode\",\"address1_stateorprovince\",\"ccs_adminasstid\",\"ccs_adminasstiddsc\",\"ccs_adminasstidname\",\"ccs_contractexpdate\",\"ccs_contractexpdateutc\",\"ccs_contractrenewalsavailablenumberandlength\",\"ccs_contractriskparameters\",\"ccs_currentcontractonfile2\",\"ccs_currentcontractonfile2name\",\"ccs_hsaid\",\"ccs_hsaiddsc\",\"ccs_hsaidname\",\"ccs_inmatecount\",\"ccs_purchasingregistrationinformation\",\"ccs_staffingdetails\",\"ccs_vendorregistrationneededname\",\"ccs_vendorregistrationpasscode\",\"ccs_vendorregistrationusername\",\"ccs_vendorregistrationwebsite\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"customertypecode\",\"customertypecodename\",\"description\",\"fax\",\"industrycode\",\"industrycodename\",\"lbmc_currentproviderid\",\"lbmc_currentprovideriddsc\",\"lbmc_currentprovideridname\",\"lbmc_maximumexpirationdate\",\"lbmc_maximumexpirationdateutc\",\"marketcap\",\"marketcap_base\",\"masteraccountiddsc\",\"masteraccountidname\",\"masterid\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"name\",\"originatingleadid\",\"originatingleadiddsc\",\"ownerid\",\"owneridname\",\"owneridyominame\",\"owningteam\",\"owninguser\",\"parentaccountid\",\"parentaccountiddsc\",\"parentaccountidname\",\"primarycontactid\",\"primarycontactiddsc\",\"primarycontactidname\",\"primarycontactidyominame\",\"revenue\",\"revenue_base\",\"statecode\",\"statecodename\",\"telephone1\",\"telephone2\",\"territoryid\",\"territoryidname\",\"websiteurl\"",
                    "SELECT accountcategorycode, accountcategorycodename, accountid, accountnumber, accountratingcodename, address1_city, address1_line1, address1_line2, address1_name, address1_postalcode, address1_stateorprovince, ccs_adminasstid, ccs_adminasstiddsc, ccs_adminasstidname, ccs_contractexpdate, ccs_contractexpdateutc, ccs_contractrenewalsavailablenumberandlength, ccs_contractriskparameters, ccs_currentcontractonfile2, ccs_currentcontractonfile2name, ccs_hsaid, ccs_hsaiddsc, ccs_hsaidname, ccs_inmatecount, ccs_purchasingregistrationinformation, ccs_staffingdetails, ccs_vendorregistrationneededname, ccs_vendorregistrationpasscode, ccs_vendorregistrationusername, ccs_vendorregistrationwebsite, createdby, createdbyname, createdon, createdonutc, customertypecode, customertypecodename, description, fax, industrycode, industrycodename, lbmc_currentproviderid, lbmc_currentprovideriddsc, lbmc_currentprovideridname, lbmc_maximumexpirationdate, lbmc_maximumexpirationdateutc, marketcap, marketcap_base, masteraccountiddsc, masteraccountidname, masterid, modifiedby, modifiedbyname, modifiedon, modifiedonutc, name, originatingleadid, originatingleadiddsc, ownerid, owneridname, owneridyominame, owningteam, owninguser, parentaccountid, parentaccountiddsc, parentaccountidname, primarycontactid, primarycontactiddsc, primarycontactidname, primarycontactidyominame, revenue, revenue_base, statecode, statecodename, telephone1, telephone2, territoryid, territoryidname, websiteurl FROM FilteredAccountUS WHERE customertypecode <> 200000 AND customertypecode <> 200001 AND statecode = 0"},
                {"AccountNotesWithandWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbyname\",\"createdbyyominame\",\"createdon\",\"createdonutc\",\"filename\",\"filesize\",\"isdocument\",\"isdocumentname\",\"mimetype\",\"modifiedby\",\"modifiedbyname\",\"modifiedbyyominame\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"objecttypecode\",\"objecttypecodename\",\"ownerid\",\"owneridname\",\"owneridyominame\",\"owningbusinessunit\",\"owninguser\",\"subject\"",
                    "SELECT FAN.annotationid, FAN.createdby, FAN.createdbyname, FAN.createdbyyominame, FAN.createdon, FAN.createdonutc, FAN.filename, FAN.filesize, FAN.isdocument, FAN.isdocumentname, FAN.mimetype, FAN.modifiedby, FAN.modifiedbyname, FAN.modifiedbyyominame, FAN.modifiedon, FAN.modifiedonutc, FAN.notetext, FAN.objectid, FAN.objecttypecode, FAN.objecttypecodename, FAN.ownerid, FAN.owneridname, FAN.owneridyominame, FAN.owningbusinessunit, FAN.owninguser, FAN.subject FROM FilteredAnnotationUS FAN JOIN FilteredAccountUS FAC ON FAN.ObjectId = FAC.accountid WHERE FAN.objecttypecode = 1 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"},
                {"AccountTasks.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"createdby\",\"createdbydsc\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"percentcomplete\",\"regardingobjectid\",\"regardingobjectidname\",\"regardingobjecttypecode\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\"",
                    "SELECT FT.activityid, FT.actualdurationminutes, FT.actualend, FT.actualendutc, FT.actualstart, FT.actualstartutc, FT.createdby, FT.createdbydsc, FT.createdbyname, FT.createdon, FT.createdonutc, FT.description, FT.modifiedby, FT.modifiedbyname, FT.modifiedon, FT.modifiedonutc, FT.ownerid, FT.owneridname, FT.owninguser, FT.percentcomplete, FT.regardingobjectid, FT.regardingobjectidname, FT.regardingobjecttypecode, FT.scheduleddurationminutes, FT.scheduledend, FT.scheduledendutc, FT.scheduledstart, FT.scheduledstartutc, FT.statecode, FT.statecodename, FT.statuscode, FT.statuscodename, FT.subject FROM FilteredTaskUS FT JOIN FilteredAccountUS FA ON FT.regardingobjectid = FA.accountid WHERE FT.regardingobjecttypecode = 1 AND FA.customertypecode <> 200000 AND FA.customertypecode <> 200001 AND FA.statecode = 0"},
                {"AccountEmailsAndAllParties.CSV",
                    "\"activityid\",\"actualend\",\"actualendutc\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"directioncode\",\"directioncodename\",\"messageid\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owneridyominame\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"readreceiptrequested\",\"readreceiptrequestedname\",\"regardingobjectid\",\"regardingobjectidname\",\"sender\",\"statuscode\",\"statuscodename\",\"subject\",\"submittedby\",\"torecipients\",\"activitypartyid\",\"addressused\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyidname\",\"partyobjecttypecode\"",
                    "SELECT E.activityid, E.actualend, E.actualendutc, E.createdby, E.createdbyname, E.createdon, E.createdonutc, E.description, E.directioncode, E.directioncodename, E.messageid, E.modifiedby, E.modifiedbyname, E.modifiedon, E.modifiedonutc, E.ownerid, E.owneridname, E.owneridyominame, E.owninguser, E.prioritycode, E.prioritycodename, E.readreceiptrequested, E.readreceiptrequestedname, E.regardingobjectid, E.regardingobjectidname, E.sender, E.statuscode, E.statuscodename, E.subject, E.submittedby, E.torecipients, EP.activitypartyid, EP.addressused, EP.participationtypemask, EP.participationtypemaskname, EP.partyid, EP.partyidname, EP.partyobjecttypecode FROM FilteredEmailUS E JOIN FilteredActivityPartyUS EP ON E.ActivityId = EP.ActivityId JOIN FilteredAccountUS FA ON E.regardingobjectid = FA.accountid WHERE E.regardingobjecttypecode = 1 AND FA.customertypecode <> 200000 AND FA.customertypecode <> 200001 AND FA.statecode = 0"},
                {"AccountEmailAttachments.CSV",
                    "\"AttachmentNumber\",\"ActivityMimeAttachmentId\",\"ActivityId\",\"FileSize\",\"MimeType\",\"FileName\",\"VersionNumber\"",
                    "SELECT EA.AttachmentNumber, EA.ActivityMimeAttachmentId, EA.ActivityId, EA.FileSize, EA.MimeType, EA.FileName, EA.VersionNumber FROM FilteredEmailUS E JOIN ActivityMimeAttachment EA ON E.ActivityId = EA.ActivityId JOIN FilteredAccountUS FA ON E.regardingobjectid = FA.accountid WHERE E.regardingobjecttypecode = 1 AND FA.customertypecode <> 200000 AND FA.customertypecode <> 200001 AND FA.statecode = 0"},
                {"AccountAppointments.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"ccs_administrativeevent\",\"ccs_administrativeeventname\",\"ccs_clientevent\",\"ccs_clienteventname\",\"ccs_clinicalevent\",\"ccs_clinicaleventname\",\"ccs_humanresourcesevent\",\"ccs_humanresourceseventname\",\"ccs_sentinelevent\",\"ccs_sentineleventname\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"isalldayevent\",\"isalldayeventname\",\"location\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"regardingobjectidname\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\",\"activityid\",\"activitypartyid\",\"addressused\",\"effort\",\"exchangeentryid\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyidname\",\"partyobjecttypecode\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\"",
                    "SELECT A.activityid, A.actualdurationminutes, A.actualend, A.actualendutc, A.actualstart, A.actualstartutc, A.ccs_administrativeevent, A.ccs_administrativeeventname, A.ccs_clientevent, A.ccs_clienteventname, A.ccs_clinicalevent, A.ccs_clinicaleventname, A.ccs_humanresourcesevent, A.ccs_humanresourceseventname, A.ccs_sentinelevent, A.ccs_sentineleventname, A.createdby, A.createdbyname, A.createdon, A.createdonutc, A.description, A.isalldayevent, A.isalldayeventname, A.location, A.modifiedby, A.modifiedbyname, A.modifiedon, A.modifiedonutc, A.ownerid, A.owneridname, A.owninguser, A.prioritycode, A.prioritycodename, A.regardingobjectid, A.regardingobjectidname, A.scheduleddurationminutes, A.scheduledend, A.scheduledendutc, A.scheduledstart, A.scheduledstartutc, A.statecode, A.statecodename, A.statuscode, A.statuscodename, A.subject, AP.activityid, AP.activitypartyid, AP.addressused, AP.effort, AP.exchangeentryid, AP.participationtypemask, AP.participationtypemaskname, AP.partyid, AP.partyidname, AP.partyobjecttypecode, AP.scheduledend, AP.scheduledendutc, AP.scheduledstart, AP.scheduledstartutc FROM FilteredAppointmentUS A JOIN FilteredActivityPartyUS AP ON A.ActivityId = AP.ActivityId JOIN FilteredAccountUS FA ON A.regardingobjectid = FA.accountid WHERE A.regardingobjecttypecode = 1 AND FA.customertypecode <> 200000 AND FA.customertypecode <> 200001 AND FA.statecode = 0"},
                {"AccountPhoneCalls.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"directioncode\",\"directioncodename\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"phonenumber\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"regardingobjectidname\",\"regardingobjecttypecode\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"serviceid\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\"",
                    "SELECT FP.activityid, FP.actualdurationminutes, FP.actualend, FP.actualendutc, FP.actualstart, FP.actualstartutc, FP.createdby, FP.createdbyname, FP.createdon, FP.createdonutc, FP.description, FP.directioncode, FP.directioncodename, FP.modifiedby, FP.modifiedbyname, FP.modifiedon, FP.modifiedonutc, FP.ownerid, FP.owneridname, FP.owninguser, FP.phonenumber, FP.prioritycode, FP.prioritycodename, FP.regardingobjectid, FP.regardingobjectidname, FP.regardingobjecttypecode, FP.scheduleddurationminutes, FP.scheduledend, FP.scheduledendutc, FP.scheduledstart, FP.scheduledstartutc, FP.serviceid, FP.statecode, FP.statecodename, FP.statuscode, FP.statuscodename, FP.subject FROM FilteredPhoneCallUS FP JOIN FilteredAccountUS FA ON FP.regardingobjectid = FA.accountid WHERE FP.regardingobjecttypecode = 1 AND FA.customertypecode <> 200000 AND FA.customertypecode <> 200001 AND FA.statecode = 0"}
                };
            var accountResults = "Begin Accounts";
            for (var i = 0; i < accountQueries.GetLength(0); i++)
            //for (int i = 0; i < 1; i++)
            {
                var fileNameString = accountQueries[i, 0].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(fileNameString);
                var queryString = accountQueries[i, 2].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(queryString);
                var headerString = accountQueries[i, 1].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(headerString);
                accountResults = accountResults + "\n" + GetAndWriteData(connString, queryString, fileNameString, headerString);
                //Console.WriteLine(accountResults);
            }
            Console.WriteLine(accountResults + "\nAccounts Complete");
            //Console.ReadLine();
            #endregion
            #region ActivityNotesExtraction
            var activityNotesQueries = new[,]
                {
                {"TaskNotesWithAndWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"filename\",\"filesize\",\"isdocument\",\"isdocumentname\",\"mimetype\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"ownerid\",\"owneridname\",\"owninguser\",\"subject\"",
                    "SELECT annotationid, createdby, createdbyname, createdon, createdonutc, filename, filesize, isdocument, isdocumentname, mimetype, modifiedby, modifiedbyname, modifiedon, modifiedonutc, notetext, objectid, ownerid, owneridname, owninguser, subject FROM FilteredAnnotationUS WHERE objecttypecode = 4212"},
                {"AppointmentNotesWithAndWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"filename\",\"filesize\",\"isdocument\",\"isdocumentname\",\"mimetype\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"ownerid\",\"owneridname\",\"owninguser\",\"subject\"",
                    "SELECT annotationid, createdby, createdbyname, createdon, createdonutc, filename, filesize, isdocument, isdocumentname, mimetype, modifiedby, modifiedbyname, modifiedon, modifiedonutc, notetext, objectid, ownerid, owneridname, owninguser, subject FROM FilteredAnnotationUS WHERE objecttypecode = 4201"},
                {"PhoneCallNotesWithAndWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"filename\",\"filesize\",\"isdocument\",\"isdocumentname\",\"mimetype\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"ownerid\",\"owneridname\",\"owninguser\",\"subject\"",
                    "SELECT annotationid, createdby, createdbyname, createdon, createdonutc, filename, filesize, isdocument, isdocumentname, mimetype, modifiedby, modifiedbyname, modifiedon, modifiedonutc, notetext, objectid, ownerid, owneridname, owninguser, subject FROM FilteredAnnotationUS WHERE objecttypecode = 4210"}
                };
            var activityNotesResults = "Begin Acivity Notes";
            for (var i = 0; i < activityNotesQueries.GetLength(0); i++)
            //for (int i = 0; i < 1; i++)
            {
                var fileNameString = activityNotesQueries[i, 0].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(fileNameString);
                var queryString = activityNotesQueries[i, 2].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(queryString);
                var headerString = activityNotesQueries[i, 1].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(headerString);
                activityNotesResults = activityNotesResults + "\n" + GetAndWriteData(connString, queryString, fileNameString, headerString);
                //Console.WriteLine(accountResults);
            }
            Console.WriteLine(activityNotesResults + "\nActivity Notes Complete");
            //Console.ReadLine();
            #endregion
            #region ContactExtraction
            var contactQueries = new[,]
                {
                {"Contacts.csv",
                    "\"accountid\",\"accountidname\",\"accountrolecode\",\"accountrolecodename\",\"address1_addressid\",\"address1_city\",\"address1_county\",\"address1_line1\",\"address1_line2\",\"address1_line3\",\"address1_name\",\"address1_postalcode\",\"address1_stateorprovince\",\"address1_telephone1\",\"address2_addressid\",\"assistantname\",\"assistantphone\",\"ccs_affiliationsacasheriffsassociationetc\",\"ccs_interestshobbies\",\"ccs_metwithcontact\",\"ccs_metwithcontactname\",\"ccs_nextelectionyear\",\"ccs_numberofmeetings\",\"ccs_politicalaffiliation\",\"ccs_politicalaffiliationname\",\"childrensnames\",\"contactid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"customertypecode\",\"customertypecodename\",\"department\",\"description\",\"emailaddress1\",\"emailaddress2\",\"familystatuscode\",\"familystatuscodename\",\"fax\",\"firstname\",\"gendercode\",\"gendercodename\",\"jobtitle\",\"lastname\",\"managername\",\"managerphone\",\"middlename\",\"mobilephone\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"parentcustomerid\",\"parentcustomeriddsc\",\"parentcustomeridname\",\"parentcustomeridtype\",\"preferredappointmenttimecode\",\"preferredappointmenttimecodename\",\"salutation\",\"spousesname\",\"suffix\",\"telephone1\",\"telephone2\",\"telephone3\",\"websiteurl\"",
                    "SELECT FC.accountid, FC.accountidname, FC.accountrolecode, FC.accountrolecodename, FC.address1_addressid, FC.address1_city, FC.address1_county, FC.address1_line1, FC.address1_line2, FC.address1_line3, FC.address1_name, FC.address1_postalcode, FC.address1_stateorprovince, FC.address1_telephone1, FC.address2_addressid, FC.assistantname, FC.assistantphone, FC.ccs_affiliationsacasheriffsassociationetc, FC.ccs_interestshobbies, FC.ccs_metwithcontact, FC.ccs_metwithcontactname, FC.ccs_nextelectionyear, FC.ccs_numberofmeetings, FC.ccs_politicalaffiliation, FC.ccs_politicalaffiliationname, FC.childrensnames, FC.contactid, FC.createdby, FC.createdbyname, FC.createdon, FC.createdonutc, FC.customertypecode, FC.customertypecodename, FC.department, FC.description, FC.emailaddress1, FC.emailaddress2, FC.familystatuscode, FC.familystatuscodename, FC.fax, FC.firstname, FC.gendercode, FC.gendercodename, FC.jobtitle, FC.lastname, FC.managername, FC.managerphone, FC.middlename, FC.mobilephone, FC.modifiedby, FC.modifiedbyname, FC.modifiedon, FC.modifiedonutc, FC.ownerid, FC.owneridname, FC.owninguser, FC.parentcustomerid, FC.parentcustomeriddsc, FC.parentcustomeridname, FC.parentcustomeridtype, FC.preferredappointmenttimecode, FC.preferredappointmenttimecodename, FC.salutation, FC.spousesname, FC.suffix, FC.telephone1, FC.telephone2, FC.telephone3, FC.websiteurl FROM FilteredContactUS FC JOIN FilteredAccountUS FA ON FC.AccountId = FA.AccountId WHERE FC.statecode = 0 AND FA.customertypecode <> 200000 AND FA.customertypecode <> 200001 AND FA.statecode = 0"},
                {"ContactNotesWithAndWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"filename\",\"filesize\",\"isdocument\",\"isdocumentname\",\"mimetype\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"ownerid\",\"owneridname\",\"owninguser\",\"subject\"",
                    "SELECT FA.annotationid, FA.createdby, FA.createdbyname, FA.createdon, FA.createdonutc, FA.filename, FA.filesize, FA.isdocument, FA.isdocumentname, FA.mimetype, FA.modifiedby, FA.modifiedbyname, FA.modifiedon, FA.modifiedonutc, FA.notetext, FA.objectid, FA.ownerid, FA.owneridname, FA.owninguser, FA.subject FROM FilteredAnnotationUS FA JOIN FilteredContactUS FC ON FA.objectid = FC.contactid JOIN FilteredAccountUS FAC ON FC.AccountId = FAC.AccountId WHERE FA.objecttypecode = 2 AND FC.statecode = 0 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"},
                {"ContactTasks.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owneridyominame\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"regardingobjectidname\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\"",
                    "SELECT FT.activityid, FT.actualdurationminutes, FT.actualend, FT.actualendutc, FT.actualstart, FT.actualstartutc, FT.createdby, FT.createdbyname, FT.createdon, FT.createdonutc, FT.description, FT.modifiedby, FT.modifiedbyname, FT.modifiedon, FT.modifiedonutc, FT.ownerid, FT.owneridname, FT.owneridyominame, FT.owninguser, FT.prioritycode, FT.prioritycodename, FT.regardingobjectid, FT.regardingobjectidname, FT.scheduleddurationminutes, FT.scheduledend, FT.scheduledendutc, FT.scheduledstart, FT.scheduledstartutc, FT.statecode, FT.statecodename, FT.statuscode, FT.statuscodename, FT.subject FROM FilteredTaskUS FT JOIN FilteredContactUS FC ON FT.regardingobjectid = FC.contactid JOIN FilteredAccountUS FAC ON FC.AccountId = FAC.AccountId WHERE FT.regardingobjecttypecode = 2 AND FC.statecode = 0 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"},
                {"ContactEmailsAndAllParties.CSV",
                    "\"activityid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"deliveryattempts\",\"description\",\"directioncode\",\"directioncodename\",\"messageid\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owneridyominame\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"regardingobjectidname\",\"sender\",\"statuscode\",\"statuscodename\",\"subject\",\"submittedby\",\"torecipients\",\"activitypartyid\",\"addressused\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyiddsc\",\"partyidname\",\"partyobjecttypecode\"",
                    "SELECT E.activityid, E.createdby, E.createdbyname, E.createdon, E.createdonutc, E.deliveryattempts, E.description, E.directioncode, E.directioncodename, E.messageid, E.modifiedby, E.modifiedbyname, E.modifiedon, E.modifiedonutc, E.ownerid, E.owneridname, E.owneridyominame, E.owninguser, E.prioritycode, E.prioritycodename, E.regardingobjectid, E.regardingobjectidname, E.sender, E.statuscode, E.statuscodename, E.subject, E.submittedby, E.torecipients, EP.activitypartyid, EP.addressused, EP.participationtypemask, EP.participationtypemaskname, EP.partyid, EP.partyiddsc, EP.partyidname, EP.partyobjecttypecode FROM FilteredEmailUS E JOIN FilteredActivityPartyUS EP ON E.ActivityId = EP.ActivityId JOIN FilteredContactUS FC ON E.regardingobjectid = FC.contactid JOIN FilteredAccountUS FAC ON FC.AccountId = FAC.AccountId WHERE E.regardingobjecttypecode = 2 AND FC.statecode = 0 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"},
                {"ContactEmailAttachments.CSV",
                    "\"AttachmentNumber\", \"EA.ActivityMimeAttachmentId\", \"EA.ActivityId\", \"EA.FileSize\", \"EA.MimeType\", \"EA.FileName\", \"EA.VersionNumber\"",
                    "SELECT EA.AttachmentNumber, EA.ActivityMimeAttachmentId, EA.ActivityId, EA.FileSize, EA.MimeType, EA.FileName, EA.VersionNumber FROM FilteredEmailUS E JOIN ActivityMimeAttachment EA ON E.ActivityId = EA.ActivityId JOIN FilteredContactUS FC ON E.regardingobjectid = FC.contactid JOIN FilteredAccountUS FAC ON FC.AccountId = FAC.AccountId WHERE E.regardingobjecttypecode = 2 AND FC.statecode = 0 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"},
                {"ContactAppointments.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"category\",\"ccs_clientevent\",\"ccs_clienteventname\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"location\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"regardingobjectid\",\"regardingobjectidname\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"subject\",\"activityid\",\"activitypartyid\",\"addressused\",\"effort\",\"exchangeentryid\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyidname\",\"partyobjecttypecode\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\"",
                    "SELECT A.activityid, A.actualdurationminutes, A.actualend, A.actualendutc, A.actualstart, A.actualstartutc, A.category, A.ccs_clientevent, A.ccs_clienteventname, A.createdby, A.createdbyname, A.createdon, A.createdonutc, A.description, A.location, A.modifiedby, A.modifiedbyname, A.modifiedon, A.modifiedonutc, A.ownerid, A.owneridname, A.owninguser, A.regardingobjectid, A.regardingobjectidname, A.scheduleddurationminutes, A.scheduledend, A.scheduledendutc, A.scheduledstart, A.scheduledstartutc, A.subject, AP.activityid, AP.activitypartyid, AP.addressused, AP.effort, AP.exchangeentryid, AP.participationtypemask, AP.participationtypemaskname, AP.partyid, AP.partyidname, AP.partyobjecttypecode, AP.scheduledend, AP.scheduledendutc, AP.scheduledstart, AP.scheduledstartutc FROM FilteredAppointmentUS A JOIN FilteredActivityPartyUS AP ON A.ActivityId = AP.ActivityId JOIN FilteredContactUS FC ON A.regardingobjectid = FC.contactid JOIN FilteredAccountUS FAC ON FC.AccountId = FAC.AccountId WHERE A.regardingobjecttypecode = 2 AND FC.statecode = 0 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"},
                {"ContactPhoneCalls.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"directioncode\",\"directioncodename\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"phonenumber\",\"regardingobjectid\",\"regardingobjectidname\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\"",
                    "SELECT PC.activityid, PC.actualdurationminutes, PC.actualend, PC.actualendutc, PC.actualstart, PC.actualstartutc, PC.createdby, PC.createdbyname, PC.createdon, PC.createdonutc, PC.description, PC.directioncode, PC.directioncodename, PC.modifiedby, PC.modifiedbyname, PC.modifiedon, PC.modifiedonutc, PC.ownerid, PC.owneridname, PC.owninguser, PC.phonenumber, PC.regardingobjectid, PC.regardingobjectidname, PC.scheduleddurationminutes, PC.scheduledend, PC.scheduledendutc, PC.scheduledstart, PC.scheduledstartutc, PC.statecode, PC.statecodename, PC.statuscode, PC.statuscodename, PC.subject FROM FilteredPhoneCallUS PC JOIN FilteredActivityPartyUS AP ON PC.ActivityId = AP.ActivityId JOIN FilteredContactUS FC ON PC.regardingobjectid = FC.contactid JOIN FilteredAccountUS FAC ON FC.AccountId = FAC.AccountId WHERE PC.regardingobjecttypecode = 2 AND FC.statecode = 0 AND FAC.customertypecode <> 200000 AND FAC.customertypecode <> 200001 AND FAC.statecode = 0"}
                };
            string contactResults = "Begin Contacts";
            for (int i = 0; i < contactQueries.GetLength(0); i++)
            //for (int i = 0; i < 1; i++)
            {
                var fileNameString = contactQueries[i, 0].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(fileNameString);
                var queryString = contactQueries[i, 2].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(queryString);
                var headerString = contactQueries[i, 1].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(headerString);
                contactResults = contactResults + "\n" + GetAndWriteData(connString, queryString, fileNameString, headerString);
                //Console.WriteLine(accountResults);
            }
            Console.WriteLine(contactResults + "\nContacts Complete");
            //Console.ReadLine();
            #endregion
            #region LeadExtraction
            var leadQueries = new[,]
                {
                {"Leads.csv",
                    "\"ccs_answerstoquestionsrcvd\",\"ccs_answerstoquestionsrcvdutc\",\"ccs_contentdevelbegins\",\"ccs_contentdevelbeginsutc\",\"ccs_draft1complete\",\"ccs_draft1completeutc\",\"ccs_estimatedrfpreleasedate\",\"ccs_estimatedrfpreleasedateutc\",\"ccs_internalrvwchanges\",\"ccs_internalrvwchangesutc\",\"ccs_outlinescompleted\",\"ccs_outlinescompletedutc\",\"ccs_pcrvwchanges\",\"ccs_pricingreview\",\"ccs_pricingreviewutc\",\"ccs_printingdate\",\"ccs_printingdateutc\",\"ccs_proposalwriterid\",\"ccs_proposalwriteridname\",\"ccs_shippingdate\",\"ccs_shippingdateutc\",\"ccs_winage\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"estimatedamount\",\"estimatedamount_base\",\"estimatedclosedate\",\"estimatedclosedateutc\",\"lbmc_biddingadpnew\",\"lbmc_incumbentproviderid\",\"lbmc_incumbentprovideridname\",\"lbmc_potentialcustomerid\",\"lbmc_potentialcustomeridname\",\"lbmc_prebiddate\",\"lbmc_prebiddateutc\",\"lbmc_prebidregistrationcomplete\",\"lbmc_prebidregistrationcompletename\",\"lbmc_proposalduedate\",\"lbmc_proposalduedateutc\",\"lbmc_questionsdue\",\"lbmc_questionsdueutc\",\"leadid\",\"leadqualitycode\",\"leadqualitycodename\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owningbusinessunit\",\"owninguser\",\"salesstagecode\",\"salesstagecodename\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\",\"websiteurl\"",
                    "SELECT ccs_answerstoquestionsrcvd, ccs_answerstoquestionsrcvdutc, ccs_contentdevelbegins, ccs_contentdevelbeginsutc, ccs_draft1complete, ccs_draft1completeutc, ccs_estimatedrfpreleasedate, ccs_estimatedrfpreleasedateutc, ccs_internalrvwchanges, ccs_internalrvwchangesutc, ccs_outlinescompleted, ccs_outlinescompletedutc, ccs_pcrvwchanges, ccs_pricingreview, ccs_pricingreviewutc, ccs_printingdate, ccs_printingdateutc, ccs_proposalwriterid, ccs_proposalwriteridname, ccs_shippingdate, ccs_shippingdateutc, ccs_winage, createdby, createdbyname, createdon, createdonutc, description, estimatedamount, estimatedamount_base, estimatedclosedate, estimatedclosedateutc, lbmc_biddingadpnew, lbmc_incumbentproviderid, lbmc_incumbentprovideridname, lbmc_potentialcustomerid, lbmc_potentialcustomeridname, lbmc_prebiddate, lbmc_prebiddateutc, lbmc_prebidregistrationcomplete, lbmc_prebidregistrationcompletename, lbmc_proposalduedate, lbmc_proposalduedateutc, lbmc_questionsdue, lbmc_questionsdueutc, leadid, leadqualitycode, leadqualitycodename, modifiedby, modifiedbyname, modifiedon, modifiedonutc, ownerid, owneridname, owningbusinessunit, owninguser, salesstagecode, salesstagecodename, statecode, statecodename, statuscode, statuscodename, subject, websiteurl FROM FilteredLeadUS"},
                {"LeadNotesWithAndWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"filename\",\"filesize\",\"isdocument\",\"isdocumentname\",\"mimetype\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"ownerid\",\"owneridname\",\"owninguser\",\"subject\"",
                    "SELECT annotationid, createdby, createdbyname, createdon, createdonutc, filename, filesize, isdocument, isdocumentname, mimetype, modifiedby, modifiedbyname, modifiedon, modifiedonutc, notetext, objectid, ownerid, owneridname, owninguser, subject FROM FilteredAnnotationUS WHERE objecttypecode = 4"},
                {"LeadTasks.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"regardingobjectid\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"subject\"",
                    "SELECT activityid, actualdurationminutes, actualend, actualendutc, actualstart, actualstartutc, createdby, createdbyname, createdon, createdonutc, description, modifiedby, modifiedbyname, modifiedon, modifiedonutc, ownerid, owneridname, owninguser, regardingobjectid, scheduleddurationminutes, scheduledend, scheduledendutc, scheduledstart, scheduledstartutc, subject FROM FilteredTaskUS WHERE regardingobjecttypecode = 4"},
                {"LeadEmailsAndAllParties.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"directioncode\",\"directioncodename\",\"messageid\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"notifications\",\"notificationsname\",\"ownerid\",\"owneridname\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"sender\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\",\"submittedby\",\"torecipients\",\"activitypartyid\",\"addressused\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyidname\",\"partyobjecttypecode\"",
                    "SELECT E.activityid, E.actualdurationminutes, E.actualend, E.actualendutc, E.actualstart, E.actualstartutc, E.createdby, E.createdbyname, E.createdon, E.createdonutc, E.description, E.directioncode, E.directioncodename, E.messageid, E.modifiedby, E.modifiedbyname, E.modifiedon, E.modifiedonutc, E.notifications, E.notificationsname, E.ownerid, E.owneridname, E.owninguser, E.prioritycode, E.prioritycodename, E.regardingobjectid, E.sender, E.statecode, E.statecodename, E.statuscode, E.statuscodename, E.subject, E.submittedby, E.torecipients, EP.activitypartyid, EP.addressused, EP.participationtypemask, EP.participationtypemaskname, EP.partyid, EP.partyidname, EP.partyobjecttypecode FROM FilteredEmailUS E JOIN FilteredActivityPartyUS EP ON E.ActivityId = EP.ActivityId WHERE E.regardingobjecttypecode = 4"},
                {"LeadEmailAttachments.CSV",
                    "\"AttachmentNumber\",\"ActivityMimeAttachmentId\",\"ActivityId\",\"FileSize\",\"MimeType\",\"FileName\",\"VersionNumber\"",
                    "SELECT EA.AttachmentNumber, EA.ActivityMimeAttachmentId, EA.ActivityId, EA.FileSize, EA.MimeType, EA.FileName, EA.VersionNumber FROM FilteredEmailUS E JOIN ActivityMimeAttachment EA ON E.ActivityId = EA.ActivityId WHERE E.regardingobjecttypecode = 4"},
                {"LeadAppointments.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"ccs_clientevent\",\"ccs_clienteventname\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"importsequencenumber\",\"isalldayevent\",\"isalldayeventname\",\"isbilled\",\"isbilledname\",\"isworkflowcreated\",\"isworkflowcreatedname\",\"location\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"subject\",\"activitypartyid\",\"addressused\",\"effort\",\"exchangeentryid\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyidname\",\"partyobjecttypecode\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\"",
                    "SELECT A.activityid, A.actualdurationminutes, A.actualend, A.actualendutc, A.actualstart, A.actualstartutc, A.ccs_clientevent, A.ccs_clienteventname, A.createdby, A.createdbyname, A.createdon, A.createdonutc, A.description, A.importsequencenumber, A.isalldayevent, A.isalldayeventname, A.isbilled, A.isbilledname, A.isworkflowcreated, A.isworkflowcreatedname, A.location, A.modifiedby, A.modifiedbyname, A.modifiedon, A.modifiedonutc, A.ownerid, A.owneridname, A.owninguser, A.prioritycode, A.prioritycodename, A.regardingobjectid, A.scheduleddurationminutes, A.scheduledend, A.scheduledendutc, A.scheduledstart, A.scheduledstartutc, A.subject, AP.activitypartyid, AP.addressused, AP.effort, AP.exchangeentryid, AP.participationtypemask, AP.participationtypemaskname, AP.partyid, AP.partyidname, AP.partyobjecttypecode, AP.scheduledend, AP.scheduledendutc, AP.scheduledstart, AP.scheduledstartutc FROM FilteredAppointmentUS A JOIN FilteredActivityPartyUS AP ON A.ActivityId = AP.ActivityId WHERE A.regardingobjecttypecode = 4"},
                {"LeadPhoneCalls.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"category\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"directioncode\",\"directioncodename\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"phonenumber\",\"regardingobjectid\",\"regardingobjectidname\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\"",
                    "SELECT activityid, actualdurationminutes, actualend, actualendutc, actualstart, actualstartutc, category, createdby, createdbyname, createdon, createdonutc, description, directioncode, directioncodename, modifiedby, modifiedbyname, modifiedon, modifiedonutc, ownerid, owneridname, owninguser, phonenumber, regardingobjectid, regardingobjectidname, scheduleddurationminutes, scheduledend, scheduledendutc, scheduledstart, scheduledstartutc, statecode, statecodename, statuscode, statuscodename, subject FROM FilteredPhoneCallUS WHERE regardingobjecttypecode = 4"}
                };
            var leadResults = "Begin Leads";
            for (var i = 0; i < leadQueries.GetLength(0); i++)
            //for (int i = 0; i < 1; i++)
            {
                var fileNameString = leadQueries[i, 0].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(fileNameString);
                var queryString = leadQueries[i, 2].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(queryString);
                var headerString = leadQueries[i, 1].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(headerString);
                leadResults = leadResults + "\n" + GetAndWriteData(connString, queryString, fileNameString, headerString);
                //Console.WriteLine(accountResults);
            }
            Console.WriteLine(leadResults + "\nLeads Complete");
            //Console.ReadLine();
            #endregion
            #region OpportunityExtraction
            var opportunityQueries = new[,]
                {
                {"Opportunities.csv",
                    "\"accountid\",\"accountidname\",\"actualclosedate\",\"actualclosedateutc\",\"actualvalue\",\"actualvalue_base\",\"ccs_biddingadp\",\"ccs_contractstartdate\",\"ccs_contractstartdateutc\",\"ccs_incumbentid\",\"ccs_incumbentidname\",\"ccs_prebiddate\",\"ccs_prebiddateutc\",\"ccs_prebidregistrationcomplete\",\"ccs_prebidregistrationcompletename\",\"ccs_proposalduedate\",\"ccs_proposalduedateutc\",\"ccs_questionsdue\",\"ccs_questionsdueutc\",\"closeprobability\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"customerid\",\"customeridname\",\"description\",\"estimatedvalue\",\"estimatedvalue_base\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"name\",\"opportunityid\",\"opportunityratingcode\",\"opportunityratingcodename\",\"originatingleadid\",\"ownerid\",\"owneridname\",\"owneridyominame\",\"owninguser\",\"pricingerrorcode\",\"pricingerrorcodename\",\"prioritycode\",\"prioritycodename\",\"salesstagecode\",\"salesstagecodename\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"stepid\",\"stepname\"",
                    "SELECT accountid, accountidname, actualclosedate, actualclosedateutc, actualvalue, actualvalue_base, ccs_biddingadp, ccs_contractstartdate, ccs_contractstartdateutc, ccs_incumbentid, ccs_incumbentidname, ccs_prebiddate, ccs_prebiddateutc, ccs_prebidregistrationcomplete, ccs_prebidregistrationcompletename, ccs_proposalduedate, ccs_proposalduedateutc, ccs_questionsdue, ccs_questionsdueutc, closeprobability, createdby, createdbyname, createdon, createdonutc, customerid, customeridname, description, estimatedvalue, estimatedvalue_base, modifiedby, modifiedbyname, modifiedon, modifiedonutc, name, opportunityid, opportunityratingcode, opportunityratingcodename, originatingleadid, ownerid, owneridname, owneridyominame, owninguser, pricingerrorcode, pricingerrorcodename, prioritycode, prioritycodename, salesstagecode, salesstagecodename, statecode, statecodename, statuscode, statuscodename, stepid, stepname FROM FilteredOpportunityUS"},
                {"OpportunityNotesWithAndWithoutAttachments.CSV",
                    "\"annotationid\",\"createdby\",\"createdbydsc\",\"createdbyname\",\"createdbyyominame\",\"createdon\",\"createdonutc\",\"documentbody\",\"filename\",\"filesize\",\"importsequencenumber\",\"isdocument\",\"isdocumentname\",\"isprivatename\",\"langid\",\"mimetype\",\"modifiedby\",\"modifiedbydsc\",\"modifiedbyname\",\"modifiedbyyominame\",\"modifiedon\",\"modifiedonutc\",\"notetext\",\"objectid\",\"objecttypecode\",\"objecttypecodename\",\"overriddencreatedon\",\"overriddencreatedonutc\",\"ownerid\",\"owneriddsc\",\"owneridname\",\"owneridtype\",\"owneridyominame\",\"owningbusinessunit\",\"owningteam\",\"owninguser\",\"stepid\",\"subject\"",
                    "SELECT annotationid, createdby, createdbydsc, createdbyname, createdbyyominame, createdon, createdonutc, documentbody, filename, filesize, importsequencenumber, isdocument, isdocumentname, isprivatename, langid, mimetype, modifiedby, modifiedbydsc, modifiedbyname, modifiedbyyominame, modifiedon, modifiedonutc, notetext, objectid, objecttypecode, objecttypecodename, overriddencreatedon, overriddencreatedonutc, ownerid, owneriddsc, owneridname, owneridtype, owneridyominame, owningbusinessunit, owningteam, owninguser, stepid, subject FROM FilteredAnnotationUS WHERE objecttypecode = 3"},
                {"OpportunityTasks.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"category\",\"createdby\",\"createdbydsc\",\"createdbyname\",\"createdbyyominame\",\"createdon\",\"createdonutc\",\"description\",\"importsequencenumber\",\"isbilled\",\"isbilledname\",\"isworkflowcreated\",\"isworkflowcreatedname\",\"modifiedby\",\"modifiedbydsc\",\"modifiedbyname\",\"modifiedbyyominame\",\"modifiedon\",\"modifiedonutc\",\"overriddencreatedon\",\"overriddencreatedonutc\",\"ownerid\",\"owneriddsc\",\"owneridname\",\"owneridtype\",\"owneridyominame\",\"owningbusinessunit\",\"owninguser\",\"percentcomplete\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"regardingobjectiddsc\",\"regardingobjectidname\",\"regardingobjectidyominame\",\"regardingobjecttypecode\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"serviceid\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subcategory\",\"subject\",\"timezoneruleversionnumber\",\"utcconversiontimezonecode\"",
                    "SELECT activityid, actualdurationminutes, actualend, actualendutc, actualstart, actualstartutc, category, createdby, createdbydsc, createdbyname, createdbyyominame, createdon, createdonutc, description, importsequencenumber, isbilled, isbilledname, isworkflowcreated, isworkflowcreatedname, modifiedby, modifiedbydsc, modifiedbyname, modifiedbyyominame, modifiedon, modifiedonutc, overriddencreatedon, overriddencreatedonutc, ownerid, owneriddsc, owneridname, owneridtype, owneridyominame, owningbusinessunit, owninguser, percentcomplete, prioritycode, prioritycodename, regardingobjectid, regardingobjectiddsc, regardingobjectidname, regardingobjectidyominame, regardingobjecttypecode, scheduleddurationminutes, scheduledend, scheduledendutc, scheduledstart, scheduledstartutc, serviceid, statecode, statecodename, statuscode, statuscodename, subcategory, subject, timezoneruleversionnumber, utcconversiontimezonecode FROM FilteredTaskUS WHERE regardingobjecttypecode = 3"},
                {"OpportunityEmailsAndAllParties.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"category\",\"compressed\",\"compressedname\",\"createdby\",\"createdbydsc\",\"createdbyname\",\"createdbyyominame\",\"createdon\",\"createdonutc\",\"deliveryattempts\",\"deliveryreceiptrequested\",\"deliveryreceiptrequestedname\",\"description\",\"directioncode\",\"directioncodename\",\"importsequencenumber\",\"isbilled\",\"isbilledname\",\"isworkflowcreated\",\"isworkflowcreatedname\",\"messageid\",\"mimetype\",\"modifiedby\",\"modifiedbydsc\",\"modifiedbyname\",\"modifiedbyyominame\",\"modifiedon\",\"modifiedonutc\",\"notifications\",\"notificationsname\",\"overriddencreatedon\",\"overriddencreatedonutc\",\"ownerid\",\"owneriddsc\",\"owneridname\",\"owneridtype\",\"owneridyominame\",\"owningbusinessunit\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"readreceiptrequested\",\"readreceiptrequestedname\",\"regardingobjectid\",\"regardingobjectiddsc\",\"regardingobjectidname\",\"regardingobjectidyominame\",\"regardingobjecttypecode\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"sender\",\"serviceid\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subcategory\",\"subject\",\"submittedby\",\"timezoneruleversionnumber\",\"torecipients\",\"trackingtoken\",\"utcconversiontimezonecode\",\"activityid\",\"activitypartyid\",\"addressused\",\"donotemail\",\"donotemailname\",\"donotfax\",\"donotfaxname\",\"donotphone\",\"donotphonename\",\"donotpostalmail\",\"donotpostalmailname\",\"effort\",\"exchangeentryid\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyiddsc\",\"partyidname\",\"partyobjecttypecode\",\"resourcespecid\",\"resourcespeciddsc\",\"resourcespecidname\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\"",
                    "SELECT E.activityid, E.actualdurationminutes, E.actualend, E.actualendutc, E.actualstart, E.actualstartutc, E.category, E.compressed, E.compressedname, E.createdby, E.createdbydsc, E.createdbyname, E.createdbyyominame, E.createdon, E.createdonutc, E.deliveryattempts, E.deliveryreceiptrequested, E.deliveryreceiptrequestedname, E.description, E.directioncode, E.directioncodename, E.importsequencenumber, E.isbilled, E.isbilledname, E.isworkflowcreated, E.isworkflowcreatedname, E.messageid, E.mimetype, E.modifiedby, E.modifiedbydsc, E.modifiedbyname, E.modifiedbyyominame, E.modifiedon, E.modifiedonutc, E.notifications, E.notificationsname, E.overriddencreatedon, E.overriddencreatedonutc, E.ownerid, E.owneriddsc, E.owneridname, E.owneridtype, E.owneridyominame, E.owningbusinessunit, E.owninguser, E.prioritycode, E.prioritycodename, E.readreceiptrequested, E.readreceiptrequestedname, E.regardingobjectid, E.regardingobjectiddsc, E.regardingobjectidname, E.regardingobjectidyominame, E.regardingobjecttypecode, E.scheduleddurationminutes, E.scheduledend, E.scheduledendutc, E.scheduledstart, E.scheduledstartutc, E.sender, E.serviceid, E.statecode, E.statecodename, E.statuscode, E.statuscodename, E.subcategory, E.subject, E.submittedby, E.timezoneruleversionnumber, E.torecipients, E.trackingtoken, E.utcconversiontimezonecode, EP.activityid, EP.activitypartyid, EP.addressused, EP.donotemail, EP.donotemailname, EP.donotfax, EP.donotfaxname, EP.donotphone, EP.donotphonename, EP.donotpostalmail, EP.donotpostalmailname, EP.effort, EP.exchangeentryid, EP.participationtypemask, EP.participationtypemaskname, EP.partyid, EP.partyiddsc, EP.partyidname, EP.partyobjecttypecode, EP.resourcespecid, EP.resourcespeciddsc, EP.resourcespecidname, EP.scheduledend, EP.scheduledendutc, EP.scheduledstart, EP.scheduledstartutc FROM FilteredEmailUS E JOIN FilteredActivityPartyUS EP ON E.ActivityId = EP.ActivityId WHERE E.regardingobjecttypecode = 3"},
                {"OpportunityEmailAttachments.CSV",
                    "\"AttachmentNumber\",\"ActivityMimeAttachmentId\",\"ActivityId\",\"FileSize\",\"MimeType\",\"FileName\",\"VersionNumber\"",
                    "SELECT EA.AttachmentNumber, EA.ActivityMimeAttachmentId, EA.ActivityId, EA.FileSize, EA.MimeType, EA.FileName, EA.VersionNumber FROM FilteredEmailUS E JOIN ActivityMimeAttachment EA ON E.ActivityId = EA.ActivityId WHERE E.regardingobjecttypecode = 3"},
                {"OpportunityAppointments.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"category\",\"ccs_administrativeevent\",\"ccs_administrativeeventname\",\"ccs_clientevent\",\"ccs_clienteventname\",\"ccs_clinicalevent\",\"ccs_clinicaleventname\",\"ccs_humanresourcesevent\",\"ccs_humanresourceseventname\",\"ccs_sentinelevent\",\"ccs_sentineleventname\",\"createdby\",\"createdbydsc\",\"createdbyname\",\"createdbyyominame\",\"createdon\",\"createdonutc\",\"description\",\"globalobjectid\",\"importsequencenumber\",\"isalldayevent\",\"isalldayeventname\",\"isbilled\",\"isbilledname\",\"isworkflowcreated\",\"isworkflowcreatedname\",\"location\",\"modifiedby\",\"modifiedbydsc\",\"modifiedbyname\",\"modifiedbyyominame\",\"modifiedon\",\"modifiedonutc\",\"outlookownerapptid\",\"overriddencreatedon\",\"overriddencreatedonutc\",\"ownerid\",\"owneriddsc\",\"owneridname\",\"owneridtype\",\"owneridyominame\",\"owningbusinessunit\",\"owninguser\",\"prioritycode\",\"prioritycodename\",\"regardingobjectid\",\"regardingobjectiddsc\",\"regardingobjectidname\",\"regardingobjectidyominame\",\"regardingobjecttypecode\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"serviceid\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subcategory\",\"subject\",\"timezoneruleversionnumber\",\"utcconversiontimezonecode\",\"activityid\",\"activitypartyid\",\"addressused\",\"donotemail\",\"donotemailname\",\"donotfax\",\"donotfaxname\",\"donotphone\",\"donotphonename\",\"donotpostalmail\",\"donotpostalmailname\",\"effort\",\"exchangeentryid\",\"participationtypemask\",\"participationtypemaskname\",\"partyid\",\"partyiddsc\",\"partyidname\",\"partyobjecttypecode\",\"resourcespecid\",\"resourcespeciddsc\",\"resourcespecidname\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\"",
                    "SELECT A.activityid, A.actualdurationminutes, A.actualend, A.actualendutc, A.actualstart, A.actualstartutc, A.category, A.ccs_administrativeevent, A.ccs_administrativeeventname, A.ccs_clientevent, A.ccs_clienteventname, A.ccs_clinicalevent, A.ccs_clinicaleventname, A.ccs_humanresourcesevent, A.ccs_humanresourceseventname, A.ccs_sentinelevent, A.ccs_sentineleventname, A.createdby, A.createdbydsc, A.createdbyname, A.createdbyyominame, A.createdon, A.createdonutc, A.description, A.globalobjectid, A.importsequencenumber, A.isalldayevent, A.isalldayeventname, A.isbilled, A.isbilledname, A.isworkflowcreated, A.isworkflowcreatedname, A.location, A.modifiedby, A.modifiedbydsc, A.modifiedbyname, A.modifiedbyyominame, A.modifiedon, A.modifiedonutc, A.outlookownerapptid, A.overriddencreatedon, A.overriddencreatedonutc, A.ownerid, A.owneriddsc, A.owneridname, A.owneridtype, A.owneridyominame, A.owningbusinessunit, A.owninguser, A.prioritycode, A.prioritycodename, A.regardingobjectid, A.regardingobjectiddsc, A.regardingobjectidname, A.regardingobjectidyominame, A.regardingobjecttypecode, A.scheduleddurationminutes, A.scheduledend, A.scheduledendutc, A.scheduledstart, A.scheduledstartutc, A.serviceid, A.statecode, A.statecodename, A.statuscode, A.statuscodename, A.subcategory, A.subject, A.timezoneruleversionnumber, A.utcconversiontimezonecode, AP.activityid, AP.activitypartyid, AP.addressused, AP.donotemail, AP.donotemailname, AP.donotfax, AP.donotfaxname, AP.donotphone, AP.donotphonename, AP.donotpostalmail, AP.donotpostalmailname, AP.effort, AP.exchangeentryid, AP.participationtypemask, AP.participationtypemaskname, AP.partyid, AP.partyiddsc, AP.partyidname, AP.partyobjecttypecode, AP.resourcespecid, AP.resourcespeciddsc, AP.resourcespecidname, AP.scheduledend, AP.scheduledendutc, AP.scheduledstart, AP.scheduledstartutc FROM FilteredAppointmentUS A JOIN FilteredActivityParty AP ON A.ActivityId = AP.ActivityId WHERE A.regardingobjecttypecode = 3"},
                {"OpportunityPhoneCalls.CSV",
                    "\"activityid\",\"actualdurationminutes\",\"actualend\",\"actualendutc\",\"actualstart\",\"actualstartutc\",\"category\",\"createdby\",\"createdbyname\",\"createdon\",\"createdonutc\",\"description\",\"directioncode\",\"directioncodename\",\"modifiedby\",\"modifiedbyname\",\"modifiedon\",\"modifiedonutc\",\"ownerid\",\"owneridname\",\"owninguser\",\"phonenumber\",\"regardingobjectid\",\"regardingobjectidname\",\"scheduleddurationminutes\",\"scheduledend\",\"scheduledendutc\",\"scheduledstart\",\"scheduledstartutc\",\"statecode\",\"statecodename\",\"statuscode\",\"statuscodename\",\"subject\"",
                    "SELECT activityid, actualdurationminutes, actualend, actualendutc, actualstart, actualstartutc, category, createdby, createdbyname, createdon, createdonutc, description, directioncode, directioncodename, modifiedby, modifiedbyname, modifiedon, modifiedonutc, ownerid, owneridname, owninguser, phonenumber, regardingobjectid, regardingobjectidname, scheduleddurationminutes, scheduledend, scheduledendutc, scheduledstart, scheduledstartutc, statecode, statecodename, statuscode, statuscodename, subject FROM FilteredPhoneCallUS WHERE regardingobjecttypecode = 3"}
                };
            var opportunityResults = "Begin Opportunities";
            for (var i = 0; i < accountQueries.GetLength(0); i++)
            //for (int i = 0; i < 1; i++)
            {
                var fileNameString = opportunityQueries[i, 0].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(fileNameString);
                var queryString = opportunityQueries[i, 2].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(queryString);
                var headerString = opportunityQueries[i, 1].ToString(CultureInfo.InvariantCulture);
                //Console.WriteLine(headerString);
                opportunityResults = opportunityResults + "\n" + GetAndWriteData(connString, queryString, fileNameString, headerString);
                //Console.WriteLine(accountResults);
            }
            Console.WriteLine(opportunityResults + "\nOpportunities Done");
            Console.ReadLine();
            #endregion
        }
    }
}
