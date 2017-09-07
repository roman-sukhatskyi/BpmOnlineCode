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
		MergeReturn mergeReturn = new MergeReturn();
		
		public string ftpHost = "";
		public string ftpUserName = "";
		public string ftpPassword = "";
		public string ftpFolder = "";
        public string pathToFolderInSQL = "";
		
        private const string fileName = "RUSH_Catalogs_";
		
    	public UsrUploadFromFTP(UserConnection userConnection)
    	{
    		//appConnection = HttpContext.Current.Application["AppConnection"] as AppConnection;
        	//userConnection = appConnection.SystemUserConnection;
        	this.userConnection = userConnection;
        	ftpHost = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPHost"));
        	ftpUserName = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPUserName"));
        	ftpPassword = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPPassword"));
        	ftpFolder = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrPathToFolderIN"));
            pathToFolderInSQL = Path.GetTempPath(); //Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrPathToFolderInSQL"));
        }
    	
        [WebInvoke(Method = "POST", BodyStyle = WebMessageBodyStyle.Wrapped,
        RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json)]
        public void UploadFromFTP()
        {
        	var pathRUSH_Buyers = "";
			var pathRUSH_InActivCards = "";
			var pathRUSH_Ours = "";
			var pathRUSH_Prods = "";
			var pathRUSH_Schedule = "";
			//var filePathRegex = new List<Regex>();
        	try
        	{
        		IsDirectoryExist(pathToFolderInSQL);
            	List<string> archiveFiles = GetArchiveListDirectoryContentsWithFTP(ftpHost, ftpUserName, ftpPassword, ftpFolder);
            	var archiveFileName = GetFileName(archiveFiles, fileName);
            	var responseDownload = GetDownloadArchiveFileWithFTP(ftpHost, ftpUserName, ftpPassword, ftpFolder, archiveFileName, pathToFolderInSQL);
                if (responseDownload.Success && responseDownload.Path != String.Empty)
                {
                    var unZipPatch = UnZipFile(responseDownload.Path, pathToFolderInSQL);
                    if (unZipPatch != String.Empty)
                    {
						var pathFiles = GetPathBuyersWitchDirectory(unZipPatch);
						if(pathFiles.Count != 0)
						{
							foreach (var currentPathFile in pathFiles)
				            {
				                if (Regex.IsMatch(currentPathFile, @"RUSH_Buyers"))
				                {
				                    pathRUSH_Buyers = currentPathFile;
				                }
				                if(Regex.IsMatch(currentPathFile, @"RUSH_InActivCards"))
				                {
				                	pathRUSH_InActivCards = currentPathFile;
				                }
				                if(Regex.IsMatch(currentPathFile, @"RUSH_Ours"))
				                {
				                	pathRUSH_Ours = currentPathFile;
				                }
				                if(Regex.IsMatch(currentPathFile, @"RUSH_Schedule"))
				                {
				                	pathRUSH_Schedule = currentPathFile;
				                }
				                if(Regex.IsMatch(currentPathFile, @"RUSH_Prods"))
				                {
				                	pathRUSH_Prods = currentPathFile;
				                }
				            }
				            var logPathMessage = "Шляхи до файлів: \n" + "1. " + pathRUSH_Buyers + "\n" + "2. " + pathRUSH_InActivCards + "\n" + "3. " + pathRUSH_Ours + "\n" + "4. " + pathRUSH_Schedule + "\n" + "5. " + pathRUSH_Prods; 
				            InsertErrorMessage(logPathMessage);
							ExecutionStpUploadBayersFromFtp(pathRUSH_Buyers, pathRUSH_InActivCards, pathRUSH_Ours, pathRUSH_Schedule, pathRUSH_Prods);
						}
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
        		InsertErrorMessage(errorMessage);
        	}
        }
        
        public void IsDirectoryExist(string pathToFolderInSQL) 
        {
        	var pathToFTPImportDirectory = pathToFolderInSQL + @"FTPImport";
            bool directoryExists = Directory.Exists(pathToFTPImportDirectory);
            if (directoryExists)
            {
                Directory.Delete(pathToFTPImportDirectory, true);
            }
        }
        
		public void ExecutionStpUploadBayersFromFtp (string pathToBayersFile, string pathToInActivCardsFile, string pathToOursFile, string pathToScheduleFile, string pathToProdsFile) 
		{
			var uploadBuyersFromFtp = new StoredProcedure(userConnection, "tsp_UploadFromFtp");
				uploadBuyersFromFtp.WithParameter("PathToBayersFile", pathToBayersFile);
				uploadBuyersFromFtp.WithParameter("PathInActivCardsFile", pathToInActivCardsFile);
				uploadBuyersFromFtp.WithParameter("PathOursFile", pathToOursFile);
				uploadBuyersFromFtp.WithParameter("PathSheduleFile", pathToScheduleFile);
				uploadBuyersFromFtp.WithParameter("PathProdsFile", pathToProdsFile);
			uploadBuyersFromFtp.PackageName = "ITS";
			uploadBuyersFromFtp.Execute();
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

        public Response GetDownloadArchiveFileWithFTP(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolder, string archiveFileName, string pathToFolderInSQL)
        {  
            string pathZip = System.IO.Path.Combine(pathToFolderInSQL, "FTPImport\\" + "Zip\\");
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
		
		public string UnZipFile(string pathUnZipFolder, string pathToFolderInSQL)
        {
            string tempPeth = Path.GetTempPath();
            string pathUnZip = System.IO.Path.Combine(pathToFolderInSQL, "FTPImport\\" + "UnZip\\");
            System.IO.Directory.CreateDirectory(pathUnZip); 
            string removePathFolder = (pathUnZipFolder.Remove(pathUnZipFolder.LastIndexOf("\\RUSH")));
            ZipFile.ExtractToDirectory(pathUnZipFolder, pathUnZip);
            Directory.Delete(removePathFolder, true);
            return pathUnZip;
        }
        
        public List<string> GetPathBuyersWitchDirectory(string pathUnZip)
        {
            string filePath = "";
            string[] fileEntries = Directory.GetFiles(pathUnZip);
            var pathtFilesFinaly = new List<string>();
            var avaiblesFileRegex = new List<Regex>();
            avaiblesFileRegex.Add(new Regex(@"RUSH_Buyers"));
            avaiblesFileRegex.Add(new Regex(@"RUSH_InActivCards"));
            avaiblesFileRegex.Add(new Regex(@"RUSH_Ours"));
            avaiblesFileRegex.Add(new Regex(@"RUSH_Prods"));
            avaiblesFileRegex.Add(new Regex(@"RUSH_Schedule"));
            foreach (string fileName in fileEntries)
            {
                if (avaiblesFileRegex.Any(w => w.IsMatch(fileName)))
                {
                    pathtFilesFinaly.Add(fileName);
                }
            }
            return pathtFilesFinaly;
        }
        
        #region FtpLog
        public void InsertErrorMessage(string logMessage)
        {
        	Insert insert = new Insert(userConnection).Into("UsrIntegrationLogFtp")
        		.Set("UsrName", Column.Parameter("ServiceLog"))
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
    
	public class MergeReturn 
	{
		public int Result { get; set; }
		public string Description { get; set; }
	}
}







