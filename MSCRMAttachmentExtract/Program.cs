using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.IO;


namespace MSCRMAttachmentExtraction
{
    class Program
    {
        /// <summary>
        /// Function to save byte array to a file
        /// </summary>
        /// <param name="_FileName">File name to save byte array</param>
        /// <param name="_ByteArray">Byte array to save to external file</param>
        /// <returns>Return true if byte array save successfully, if not return false</returns>
        public static bool ByteArrayToFile(string _FileName, byte[] _ByteArray)
        {
            try
            {
                // Open file for reading
                System.IO.FileStream _FileStream = new System.IO.FileStream(_FileName, System.IO.FileMode.Create, System.IO.FileAccess.Write);

                // Writes a block of bytes to this stream using data from a byte array.
                _FileStream.Write(_ByteArray, 0, _ByteArray.Length);

                // close file stream
                _FileStream.Close();

                return true;
            }
            catch (Exception _Exception)
            {
                // Error
                Console.WriteLine("Exception caught in process: {0}", _Exception.ToString());
            }

            // error occured, return false
            return false;
        }

        static void Main(string[] args)
        {
            string connString = "Server=servername;Database=Database_MSCRM;User Id=username;password=password";
            string queryNonEmailAttachments = "SELECT DocumentBody, ObjectId, FileName, CONVERT(varchar(255), ObjectID) + '_' + CONVERT(varchar(255), Filename) AS SaveAsFileName FROM FilteredAnnotationUS WHERE IsDocument = 1 AND (objecttypecode = 1 or objecttypecode = 2 or objecttypecode = 3 or objecttypecode = 4 or objecttypecode = 4212 or objecttypecode = 4201 or objecttypecode = 4210)";
            using (SqlConnection connection = new SqlConnection(connString))
            {
                SqlCommand command = new SqlCommand(queryNonEmailAttachments, connection);
                try
                {
                    connection.Open();
                    command.CommandTimeout = 300000;
                    SqlDataReader reader = command.ExecuteReader();
                    StreamWriter file = new StreamWriter(@"C:\\CRM\\NONEMAIL\\NonEmailMaster.csv", true);
                    int i = 0;
                    while (reader.Read())
                    {
                        string filePathAndName = "C:\\CRM\\NONEMAIL\\" + reader.GetString(3);
                        /* 
                         * Converts Latin-1 from the CRM database to UTF8 so we can use Base64 decoding to recreate the original file.
                         */
                        string fileDataString = Encoding.UTF8.GetString(Encoding.GetEncoding(1252).GetBytes(reader.GetString(0)));
                        byte[] fileBytes = Convert.FromBase64String(fileDataString);

                        /*
                         * Writes the original file to disk
                         */
                        bool fileWritten = ByteArrayToFile(filePathAndName, fileBytes);

                        /*
                         * Adds the record to the CSV reference file.
                         */
                        file.WriteLine("\"{0}\",\"{1}\",\"{2}\"", reader.GetValue(1).ToString(), reader.GetString(2), reader.GetString(3));
                        i++;
                    }
                    Console.WriteLine("Processed {0} non email records.", i);
                    reader.Close();
                    file.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                connString = "Server=server;Database=database_MSCRM;User Id=username;password=password";
                string queryEmailAttachments = "SELECT EA.Body,  EA.ActivityID,  EA.AttachmentNumber,  EA.FileName,CONVERT(varchar(255), EA.ActivityID) + '_' + CONVERT(varchar(255), EA.AttachmentNumber) + '_' + CONVERT(varchar(255), EA.Filename) AS SaveAsFileName FROM  FilteredEmailUS E JOIN  ActivityMimeAttachment EA ON E.ActivityID = EA.ActivityID WHERE  E.regardingobjecttypecode = 1 or E.regardingobjecttypecode = 2 or E.regardingobjecttypecode = 3 or E.regardingobjecttypecode = 4";
                using (SqlConnection connection2 = new SqlConnection(connString))
                {
                    SqlCommand command2 = new SqlCommand(queryEmailAttachments, connection);
                    try
                    {
                        connection2.Open();
                        command2.CommandTimeout = 300000;
                        SqlDataReader reader = command2.ExecuteReader();
                        StreamWriter file = new StreamWriter(@"C:\\CRM\\\\EMAIL\\EmailMaster.csv", true);
                        int i = 0;
                        while (reader.Read())
                        {

                            string filePathAndName = "C:\\CRM\\EMAIL\\" + reader.GetString(4);
                            string fileDataString = Encoding.UTF8.GetString(Encoding.GetEncoding(1252).GetBytes(reader.GetString(0)));
                            byte[] fileBytes = Convert.FromBase64String(fileDataString);
                            bool fileWritten = ByteArrayToFile(filePathAndName, fileBytes);
                            file.WriteLine("\"{0}\",\"{1}\",\"{2}\",\"{3}\"", reader.GetValue(1).ToString(), reader.GetValue(2).ToString(), reader.GetString(3), reader.GetString(4));
                            i++;
                        }
                        Console.WriteLine("Processed {0} email records.", i);
                        reader.Close();
                        file.Close();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                Console.ReadLine();

            }
        }
    }
}
