namespace Terrasoft.Configuration.UsrUploadFilesFromFtpFolderOut
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
	using Terrasoft.Configuration.UsrUploadFilesFromFtpFolderOut;
    
    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Required)]
    class UploadProducts : UploadFromFtpFolderOut
    {
    	public AppConnection appConnection;
        public UserConnection userConnection;
       	
       	private const string fileName = "Products.csv";
		public string ftpHost = "";
		public string ftpUserName = "";
		public string ftpPassword = "";
		public string ftpFolderOut = "";
       	public DateTime uploadDate = DateTime.Now;
       	
    	public UploadProducts(UserConnection userConnection)
    	{
        	this.userConnection = userConnection;
        	ftpHost = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPHost"));
        	ftpUserName = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPUserName"));
        	ftpPassword = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPPassword"));
        	ftpFolderOut = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrPathToFolderOut"));
    	}
    	
        [WebInvoke(Method = "POST", BodyStyle = WebMessageBodyStyle.Wrapped,
        RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json)]
        public void StartUploadProducts()
        {
        	try
        	{
        		var filesList = GetListDirectoryContentsWithFTP(ftpHost, ftpUserName, ftpPassword, ftpFolderOut);
            	var fileName = GetLastFileName(filesList);
            	var fileByteArray = GetDownloadFileWithFTP(ftpHost, ftpUserName, ftpPassword, ftpFolderOut, fileName);
            	ParseFileByteArray(fileByteArray);
            	DeleteFileOnFTP(ftpHost, ftpUserName, ftpPassword, ftpFolderOut, fileName);
        	}
        	catch (Exception ex)
        	{
        		var errorMessage = ex.Message;
        		InsertErrorMessage(errorMessage);
        	}
        }
        
        public override List<string> GetListDirectoryContentsWithFTP(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolder)
        {
        	List<string> filesFtp = new List<string>();
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolder + "Products/");
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
		public override string GetLastFileName (List<string> files)
		{
			var fileName = "Products.csv";
            DateTime maxDate;
            List<DateTime> dateFiles = new List<DateTime>();
            var regex = new Regex(@"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}");
            foreach (var f in files)
            {
                foreach (Match m in regex.Matches(f))
                {
                    DateTime dt;
                    if (DateTime.TryParseExact(m.Value, "yyyy-MM-dd_HH-mm-ss", null, DateTimeStyles.None, out dt))
                    {
                        dateFiles.Add(dt);
                    }
                }
            }
            maxDate = dateFiles.Max();
            return fileName = maxDate.ToString("yyyy-MM-dd_HH-mm-ss") + fileName;
		}
		public override byte[] GetDownloadFileWithFTP(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolder, string fileName)
		{
			FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolder + "Products/" + fileName);
            request.Method = WebRequestMethods.Ftp.DownloadFile;
            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
            FtpWebResponse response = (FtpWebResponse)request.GetResponse();

            byte[] buffer;

            Stream responseStream = response.GetResponseStream();

            using (MemoryStream br = new MemoryStream())
            {
                responseStream.CopyTo(br);
                buffer = br.ToArray();
            }
            responseStream.Close();
            return buffer;
		}
		public override void ParseFileByteArray(byte[] fileData)
		{
            using (var stream = new System.IO.MemoryStream(fileData))
            {
                using (var reader = new StreamReader(stream))
                {
                    using (var parser = new Microsoft.VisualBasic.FileIO.TextFieldParser(reader))
                    {
                        while (!parser.EndOfData)
                        {
                            var line = parser.ReadLine();
                            string[] values = line.Split(';');
                            UpdateDataBpm(values);
                        }
                    }
                }
            }
		}
		public override void UpdateDataBpm(string [] values)
		{
            var orderId = (Guid?)GetLookupBPMIdByString("Order", "Number", values[0]);
            if (orderId == null || values[1] == String.Empty)
            {
                return;
            }
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { "Order", orderId },
                { "UsrOrderItemNumber", values[1] }
            };

            var orderProductEntity = userConnection.EntitySchemaManager.GetInstanceByName("OrderProduct").CreateEntity(userConnection);
            if (orderProductEntity.FetchFromDB(conditions))
            {
            	orderProductEntity.SetColumnValue("Name", values[2]);
                orderProductEntity.SetColumnValue("UsrSKU", values[3]);
                orderProductEntity.SetColumnValue("Quantity", values[4]);
                orderProductEntity.SetColumnValue("UsrInStock", values[5]);
                orderProductEntity.Save(false);
            }
            InsertUploadDate(uploadDate, orderId);
        }
        
        public override void DeleteFileOnFTP(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolder, string fileName)
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolder + "Products/" + fileName);
            request.Method = WebRequestMethods.Ftp.DeleteFile;
            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);

            using (FtpWebResponse response = (FtpWebResponse)request.GetResponse())
            {
               
            }
        }
		
		public object GetLookupBPMIdByString(string table, string column, string value)
        {
            if (value == String.Empty)
            {
                return String.Empty;
            }

            var lookupBPMId = (new Select(userConnection).Top(1)
                .Column("Id")
                .From(table)
                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<object>();
            return lookupBPMId;
        }
		public object GetLookupBPMIdByInt(string table, string column, long value)
        {
            if (value == 0)
            {
                return Guid.Empty;
            }

            var lookupBPMId = (new Select(userConnection).Top(1)
                .Column("Id")
                .From(table)
                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<object>();
            return lookupBPMId;
        }
        
        #region FtpLog
        public void InsertErrorMessage(string logMessage)
        {
        	Insert insert = new Insert(userConnection).Into("UsrIntegrationLogFtp")
        		.Set("UsrName", Column.Parameter("Загрузка файлов c папки OUTProducts"))
                .Set("UsrErrorDescription", Column.Parameter(logMessage));
            insert.Execute();
        }
        #endregion
        
        #region InsertUploadDate
        public void InsertUploadDate(DateTime uploadDate, Guid? orderId)
        {
    		var orderEntity = userConnection.EntitySchemaManager.GetInstanceByName("Order").CreateEntity(userConnection);
         	if (orderEntity.FetchFromDB("Id", orderId))
         	{
        		orderEntity.SetColumnValue("UsrUploadDate", uploadDate);
        		orderEntity.Save(false);
         	}
    	}
        #endregion
    }
}









