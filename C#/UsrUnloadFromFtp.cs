namespace Terrasoft.Configuration.UsrUploadFromFTP
{
    using System;
    using System.Net;
    using System.Data;
    using System.Linq;
    using System.IO;
    using System.IO.Compression;
    using System.ServiceModel;
    using System.ServiceModel.Activation;
    using System.ServiceModel.Web;
    using System.Web;
    using Terrasoft.Common;
    using Terrasoft.Core;
    using System.Collections.Generic;
    using Terrasoft.Core.DB;
    using Terrasoft.Core.Entities;
    using DocumentFormat.OpenXml;
    using DocumentFormat.OpenXml.Packaging;
    using DocumentFormat.OpenXml.Spreadsheet;
    using Column = Terrasoft.Core.DB.Column;
    using System.Globalization;
    using System.Text.RegularExpressions;

    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Required)]
    public class UsrUploadFromFTP
    {
    	public AppConnection appConnection;
        public UserConnection userConnection;
		Response responseFtp = new Response();
		
		public string ftpHost = "";
		public string ftpUserName = "";
		public string ftpPassword = "";
		public string ftpFolder = "";

        private const string fileName = "XML_RUSH_Catalogs_";
		
    	public UsrUploadFromFTP()
    	{
    		appConnection = HttpContext.Current.Application["AppConnection"] as AppConnection;
        	userConnection = appConnection.SystemUserConnection;
        	
        	ftpHost = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPHost"));
        	ftpUserName = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPUserName"));
        	ftpPassword = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPPassword"));
        	ftpFolder = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrPathToFolderIN"));
    	}
    	
        [WebInvoke(Method = "POST", BodyStyle = WebMessageBodyStyle.Wrapped,
        RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json)]
        public void UploadFromFTP()
        {
        	try
        	{
            	List<string> archiveFiles = GetArchiveListDirectoryContentsWithFTP(ftpHost, ftpUserName, ftpPassword, ftpFolder);
            	var archiveFileName = GetFileName(archiveFiles, fileName);
            	var responseDownload = GetDownloadArchiveFileWithFTP(ftpHost, ftpUserName, ftpPassword, ftpFolder, archiveFileName);
                if (responseDownload.Success && responseDownload.Path != String.Empty)
                {
                    var unZipPatch = UnZipFile(responseDownload.Path);
                    if (unZipPatch != String.Empty)
                    {
                        byte[] bytes = OpenBuyersWitchDirectory(unZipPatch);
                        var fileId = InsertFileInBlob(bytes);
                        /*if(fileId != Guid.Empty)
                        {
                        	var storedProcedure = new StoredProcedure(userConnection, "tsp_ProcedureName");
							storedProcedure.WithParameter("FileId", fileId);
							storedProcedure.Execute();
                        }*/
                    }
                }
                else
                {
                    InsertErrorMessage(responseDownload.Error);
                }
            
        	}
        	catch (Exception ex)
        	{
        		var errorMessage = ex.Message;
        	}
        }

        public static List<string> GetArchiveListDirectoryContentsWithFTP(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolder)
        {
        	List<string> filesFtp = new List<string>();
        	
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolder);
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
            
            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            while (!reader.EndOfStream)
            {
                filesFtp.Add(reader.ReadLine());
            }
            reader.Close();
            response.Close();
            return filesFtp;  
        }

        public static string GetFileName(List<string> files, string fileName)
        {
            DateTime maxDate;
            List<DateTime> dateFiles = new List<DateTime>();
            var regex = new Regex(@"\d{4}_\d{2}_\d{2}");
            foreach (var f in files)
            {
                foreach (Match m in regex.Matches(f))
                {
                    DateTime dt;
                    if (DateTime.TryParseExact(m.Value, "yyyy_MM_dd", null, DateTimeStyles.None, out dt))
                    {
                        dateFiles.Add(dt);
                    }
                }
            }
            maxDate = dateFiles.Max();
            return fileName = fileName + maxDate.ToString("yyyy_MM_dd") + ".zip";
        }

        public Response GetDownloadArchiveFileWithFTP(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolder, string archiveFileName)
        {
            string tempPeth = Path.GetTempPath();
            string pathZip = System.IO.Path.Combine(tempPeth, "FTPImport\\" + "Zip\\");
            System.IO.Directory.CreateDirectory(pathZip);
            string savePathFile = pathZip + archiveFileName;
            try
            {
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolder + archiveFileName);
                request.Method = WebRequestMethods.Ftp.DownloadFile;
                request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();

                Stream responseStream = response.GetResponseStream();

                using (Stream s = File.Create(savePathFile))
                {
                    responseStream.CopyTo(s);
                }
                responseFtp.Success = true;
                responseFtp.Path = savePathFile;
            }
            catch (Exception ex)
            {
                responseFtp.Success = false;
                responseFtp.Error = ex.Message;
            }
            return responseFtp;
        }
		
		public string UnZipFile(string pathUnZipFolder)
        {
            string tempPeth = Path.GetTempPath();
            string pathUnZip = System.IO.Path.Combine(tempPeth, "FTPImport\\" + "UnZip\\");
            System.IO.Directory.CreateDirectory(pathUnZip); 
            string removePathFolder = (pathUnZipFolder.Remove(pathUnZipFolder.LastIndexOf("\\XML")));
            ZipFile.ExtractToDirectory(pathUnZipFolder, pathUnZip);
            Directory.Delete(removePathFolder, true);
            return pathUnZip;
        }
        
        public byte[] OpenBuyersWitchDirectory(string pathUnZip)
        {
            string filePath = "";
            string[] fileEntries = Directory.GetFiles(pathUnZip);
            List<string> currentFile = new List<string>();
            var regex = new Regex(@"RUSH_Buyers");
            foreach (string fileName in fileEntries)
            {
                foreach (Match m in regex.Matches(fileName))
                {
                    currentFile.Add(fileName);
                    filePath = fileName;
                }
            }
            byte[] bytes = System.IO.File.ReadAllBytes(filePath);
            
            string removePathFolder = (filePath.Remove(filePath.LastIndexOf("\\UnZip")));
            Directory.Delete(removePathFolder, true);
            
            return bytes;
        }
        
        public Guid InsertFileInBlob(byte[] fileBlob)
        {
        	var date = DateTime.Now.ToString("yyyy_MM_dd");
        	var size = fileBlob.Length / 1024;
        	var name = "RUSH_Buyers_" + date + ".xml";
			var Id = Guid.Empty;
			
			var fileEntity = userConnection.EntitySchemaManager.GetInstanceByName("File").CreateEntity(userConnection);
			fileEntity.FetchFromDB("Id", Id);
			fileEntity.SetDefColumnValues();
            fileEntity.SetColumnValue("Name", name);
            fileEntity.SetColumnValue("Data", fileBlob);
            fileEntity.SetColumnValue("TypeId", UsrConstantsServer.FileType.File);
            fileEntity.SetColumnValue("Size", size);
            fileEntity.Save();
            
            return fileEntity.GetTypedColumnValue<Guid>("Id");;
        }
        
        #region FtpLog
        public void InsertErrorMessage(string logMessage)
        {
        	Insert insert = new Insert(userConnection).Into("UsrIntegrationLogFtp")
                .Set("UsrErrorDescription", Column.Parameter(logMessage));
            insert.Execute();
        }
        #endregion
    }

    public class Response
    {
        public bool Success { get; set; }
        public string Error { get; set; }
        public string Path { get; set; }
    }
}



