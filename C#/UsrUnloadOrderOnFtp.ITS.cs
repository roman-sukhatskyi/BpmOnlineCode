
namespace Terrasoft.Configuration.UsrUnloadOrderOnFtp
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
    using System.Text;

    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Required)]
    public class UsrUnloadOrderOnFtp
    {
        public AppConnection appConnection;
        public UserConnection userConnection;

        Response response = new Response();

        public string ftpHost = "";
        public string ftpUserName = "";
        public string ftpPassword = "";
        public string ftpFolderOutOrders = "";
        public string dateTime = DateTime.Now.ToString("yyyy-MM-dd_HH:mm:ss");
		public DateTime unloadDate = DateTime.Now;
        public UsrUnloadOrderOnFtp(UserConnection userConnection)
        {
			this.userConnection = userConnection;
            ftpHost = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPHost"));
            ftpUserName = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPUserName"));
            ftpPassword = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrFTPPassword"));
            ftpFolderOutOrders = Convert.ToString(Terrasoft.Core.Configuration.SysSettings.GetValue(userConnection, "UsrPathToFolderInOrders"));
        }

        [WebInvoke(Method = "POST", BodyStyle = WebMessageBodyStyle.Wrapped,
        RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json)]
        public void UnloadOnFTP()
        {
            try
            {
                var ordersId = GetModifiedOrdersId();
                if (ordersId.Count != 0)
                {
                    var Order = GetOrderInfo(ordersId);
                	var orderStatusCsv = GenerateOrderStatusCsv(Order);
                    UploadOrderStatusCsvFile(ftpHost, ftpUserName, ftpPassword, ftpFolderOutOrders, orderStatusCsv);

                    var OrderProducts = GetOrderProductInfo(ordersId);
                    var orderProductsCsv = GenerateOrderProductsCsv(OrderProducts);
                    UploadOrderProductsCsvFile(ftpHost, ftpUserName, ftpPassword, ftpFolderOutOrders, orderProductsCsv);

                    var OrderHeaders = GetOrderHeadersInfo(ordersId);
                    var orderHeadersCsv = GenerateOrderHeadersCsv(OrderHeaders);
                    UploadOrderHeadersCsvFile(ftpHost, ftpUserName, ftpPassword, ftpFolderOutOrders, orderHeadersCsv);

                    var OrderDiscounts = GetOrderDiscountsInfo(ordersId);
                    var orderDiscountsCsv = GenerateOrderDiscountsCsv(OrderDiscounts);
                    UploadOrderDiscountsCsvFile(ftpHost, ftpUserName, ftpPassword, ftpFolderOutOrders, orderDiscountsCsv);	
                    
                    InsertUnloadDate(unloadDate, ordersId);
                }
            }
            catch (Exception ex)
            {
                var errorMessage = ex.Message;
				InsertErrorMessage(errorMessage);
            }
        }
        // Получения заказов которые были изменены за 5 мин
        public List<Guid> GetModifiedOrdersId()
        {
            var dateFormat = "yyyy-MM-dd HH:mm:ss";
            var startDate = DateTime.Now.AddHours(-3).AddMinutes(-5).ToString(dateFormat);
            var endDate = DateTime.Now.AddHours(-3).ToString(dateFormat);
            var orderList = new List<Guid>();

            Select select = new Select(userConnection)
                .Column("Order", "Id")
                .From("Order")
	                .Join(JoinType.LeftOuter, "OrderStatus")
					.On("OrderStatus", "Id").IsEqual("Order", "StatusId")
                .Where("Order", "ModifiedOn").IsBetween(Column.Parameter(startDate)).And(Column.Parameter(endDate))
                //
                .And("OrderStatus", "UsrCode").IsEqual(Column.Parameter(16)).And("Order", "UsrUnloadDate").IsNull()
				//
                /*.And("OrderStatus", "UsrCode").In(
						Column.Parameter(16),
						Column.Parameter(6),
						Column.Parameter(13))*/
                as Select;
            using (DBExecutor dbExecutor = userConnection.EnsureDBConnection())
            {
                using (IDataReader reader = select.ExecuteReader(dbExecutor))
                {
                    while (reader.Read())
                    {
                        orderList.Add(reader.GetColumnValue<Guid>("Id"));
                    }
	                Select selectOrders = new Select(userConnection)
			        	.Column("Order", "Id")
		                .From("Order")
			                .Join(JoinType.LeftOuter, "OrderStatus")
							.On("OrderStatus", "Id").IsEqual("Order", "StatusId")
		                .Where("Order", "ModifiedOn").IsBetween(Column.Parameter(startDate)).And(Column.Parameter(endDate))
		                .And("OrderStatus", "UsrCode").In(
								Column.Parameter(6),
								Column.Parameter(13)).And("Order", "UsrUnloadDate").Not().IsNull()
		                as Select;
			            using (DBExecutor dbExecutor2 = userConnection.EnsureDBConnection())
			            {
			                using (IDataReader reader2 = selectOrders.ExecuteReader(dbExecutor2))
			                {
			                    while (reader2.Read())
			                    {
			                        orderList.Add(reader2.GetColumnValue<Guid>("Id"));
			                    }
			                }
			            }
                }
            }
            return orderList;
        }

        #region OrderStatus
        // Получение информации по заказу
        public List<Order> GetOrderInfo(List<Guid> orders)
        {
            var Order = new List<Order>();
            foreach (var orderId in orders)
            {
                Select selectOrder = new Select(userConnection)
                .Column("Number")
                .Column("StatusId")
                .Column("ModifiedOn")
                .From("Order")
                .Where("Id").IsEqual(Column.Parameter(orderId)) as Select;
                using (DBExecutor dbExecutor = userConnection.EnsureDBConnection())
                {
                    using (IDataReader reader = selectOrder.ExecuteReader(dbExecutor))
                    {
                        while (reader.Read())
                        {
                            var orderStatusId = reader.GetColumnValue<Guid>("StatusId");
                            var orderStatus = GetLookupBPM("OrderStatus", "Id", orderStatusId);
                            Order.Add(new Order()
                            {
                                Number = reader.GetColumnValue<string>("Number"),
                                Status = orderStatus, 
                                ModifiedOn = reader.GetColumnValue<DateTime>("ModifiedOn")
                            });
                        }
                    }
                }
            }
            return Order;
        }
        // Формирование СSV файла с статусами заказа
        public string GenerateOrderStatusCsv(List<Order> orders)
        {
            var delimiter = ";";
            StringBuilder csvExport = new StringBuilder();
            foreach (var order in orders)
            {
                csvExport.AppendLine(string.Join(delimiter, new string[] {
                    order.Number,
                    order.Status,
                    order.ModifiedOn.ToString()
                }));
            }
            return csvExport.ToString();
        }
        // Выгрузка файла на FTP по статусам заказов
        public void UploadOrderStatusCsvFile(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolderOutOrders, string orderStatusCsv)
        {
            var fileName = dateTime + "Status.csv";
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolderOutOrders + fileName);
            request.Method = WebRequestMethods.Ftp.UploadFile;

            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
            //var text = Encoding.UTF8.GetBytes(orderStatusCsv);
            var text = Encoding.GetEncoding("Windows-1251").GetBytes(orderStatusCsv);
            request.ContentLength = text.Length;
            request.Proxy = null;
            using (Stream body = request.GetRequestStream())
            {
                body.Write(text, 0, text.Length);
            }
            try
            {
                using (var response = (FtpWebResponse)request.GetResponse())
                {
                    // TODO: nothing; just close
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion

        #region OrderProducts
        //Получение продуктов в заказе
        public List<Order> GetOrderProductInfo(List<Guid> orders)
        {
            var OrderProduct = new List<Order>();
            foreach (var orderId in orders)
            {
                Select selectOrder = new Select(userConnection)
                    .Column("OrderProduct", "OrderId")
                    .Column("OrderProduct", "UsrOrderItemNumber")
                    .Column("Order", "Number")
                    .Column("OrderProduct", "Name")
                    .Column("OrderProduct", "UsrSKU")
                    .Column("OrderProduct", "Quantity")
                    .Column("OrderProduct", "UsrInStock")
                    .Column("Order", "UsrWeight")
                    .Column("OrderProduct", "UsrOriginalPrice")
                    .Column("OrderProduct", "Price")
                    .Column("OrderProduct", "UsrDiscountedPrice")
                    .Column("OrderProduct", "DiscountAmount")
                    .Column("OrderProduct", "Amount")
                    .Column("OrderProduct", "UsrAmountPriceDiscount")
                .From("Order")
                .Join(JoinType.Inner, "OrderProduct")
                .On("OrderProduct", "OrderId").IsEqual("Order", "Id")
                .Where("Order", "Id").IsEqual(Column.Parameter(orderId)) as Select;
                using (DBExecutor dbExecutor = userConnection.EnsureDBConnection())
                {
                    using (IDataReader reader = selectOrder.ExecuteReader(dbExecutor))
                    {
                        while (reader.Read())
                        {
                            OrderProduct.Add(new Order
                            {
                                UsrOrderItemNumber = reader.GetColumnValue<int>("UsrOrderItemNumber"),
                                Number = reader.GetColumnValue<string>("Number"),
                                Name = reader.GetColumnValue<string>("Name"),
                                UsrSKU = reader.GetColumnValue<string>("UsrSKU"),
                                Quantity = reader.GetColumnValue<double>("Quantity"),
                                UsrInStock = reader.GetColumnValue<double>("UsrInStock"),
                                UsrWeightProduct = reader.GetColumnValue<double>("UsrWeight"),
                                UsrOriginalPrice = reader.GetColumnValue<double>("UsrOriginalPrice"),
                                Price = reader.GetColumnValue<double>("Price"),
                                UsrDiscountedPrice = reader.GetColumnValue<double>("UsrDiscountedPrice"),
                                DiscountAmount = reader.GetColumnValue<double>("DiscountAmount"),
                                Amount = reader.GetColumnValue<double>("Amount"),
                                UsrAmountPriceDiscount = reader.GetColumnValue<double>("UsrAmountPriceDiscount")
                            });
                        }
                    }
                }
            }
            return OrderProduct.OrderBy(x => x.OrderId).ToList<Order>();
        }

        // Формирование СSV файла с продуктов в заказе
        public string GenerateOrderProductsCsv(List<Order> orders)
        {
            var delimiter = ";";
            StringBuilder csvExport = new StringBuilder();
            foreach (var orderProduct in orders)
            {
                csvExport.AppendLine(string.Join(delimiter, new string[] {
                      orderProduct.UsrOrderItemNumber.ToString(),
                      orderProduct.Number,
                      orderProduct.Name,
                      orderProduct.UsrSKU,
                      orderProduct.Quantity.ToString(),
                      orderProduct.UsrInStock.ToString(),
                      orderProduct.UsrWeightProduct.ToString(),
                      orderProduct.UsrOriginalPrice.ToString(),
                      orderProduct.Price.ToString(),
                      orderProduct.UsrDiscountedPrice.ToString(),
                      orderProduct.DiscountAmount.ToString(),
                      orderProduct.Amount.ToString(),
                      orderProduct.UsrAmountPriceDiscount.ToString()
                }));
            }
            return csvExport.ToString();
        }

        // Выгрузка файла на FTP по продуктам заказов
        public void UploadOrderProductsCsvFile(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolderOutOrders, string orderProductsCsv)
        {
            var fileName = dateTime + "Products.csv";
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolderOutOrders + fileName);
            request.Method = WebRequestMethods.Ftp.UploadFile;

            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
            var text = Encoding.GetEncoding("Windows-1251").GetBytes(orderProductsCsv);
            //var text = Encoding.UTF8.GetBytes(orderProductsCsv);
            //var text = Encoding.GetEncoding("Windows-1251").GetBytes(orderStatusCsv);
            request.ContentLength = text.Length;
            request.Proxy = null;
            using (Stream body = request.GetRequestStream())
            {
                body.Write(text, 0, text.Length);
            }
            try
            {
                using (var response = (FtpWebResponse)request.GetResponse())
                {
                    // TODO: nothing; just close
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion

        #region OrderHeaders
        public List<Order> GetOrderHeadersInfo(List<Guid> orders)
        {
            var OrderHeaders = new List<Order>();
            foreach (var orderId in orders)
            {
                Select selectOrder = new Select(userConnection)
                    .Column("Order", "Id")
                    .Column("Order", "Number")
                    .Column("Order", "UsrTypeId")
                    .Column("Order", "UsrId")
                    .Column("Order", "StatusId")
                    .Column("Order", "OwnerId")
                    .Column("Order", "PaymentStatusId")
                    .Column("Order", "UsrDeliveryDate")
                    .Column("Order", "PaymentAmount")
                    .Column("Order", "UsrCostDelivery")
                    .Column("Order", "UsrWeight")
                    .Column("Order", "UsrPaymentMethod")
                    .Column("Order", "ReceiverName")
                    .Column("Contact", "Email")
                    .Column("Order", "ContactNumber")
                    .Column("Contact", "UsrNumberActiveCard")
                    .Column("Order", "Comment")
                    .Column("Order", "UsrParentId")
                    .Column("Order", "UsrDeliveryServiceId")
                    .Column("Order", "UsrDeliveryDepartment")
                    .Column("Order", "DeliveryTypeId")
                    .Column("Order", "UsrCity")
                    .Column("Order", "UsrAddress")
                    .Column("Order", "UsrHouse")
                    .Column("Order", "UsrCorps")
                    .Column("Order", "UsrApartment")
					.Column("Order", "UsrCityCode")
                    .Column("Order", "UsrStreetCode")
                .From("Order")
                .Join(JoinType.LeftOuter, "Contact")
                .On("Contact", "Id").IsEqual("Order", "ContactId")
                .Where("Order", "Id").IsEqual(Column.Parameter(orderId)) as Select;
                using (DBExecutor dbExecutor = userConnection.EnsureDBConnection())
                {
                    using (IDataReader reader = selectOrder.ExecuteReader(dbExecutor))
                    {
                        while (reader.Read())
                        {
                            var statusId = reader.GetColumnValue<Guid>("StatusId");
                            var status = GetLookupBPM("OrderStatus", "Id", statusId);
                            var ownerId = reader.GetColumnValue<Guid>("OwnerId");
                            var owner = GetLookupBPM("Contact", "Id", ownerId);
                            var parentOrderId = reader.GetColumnValue<Guid>("UsrParentId");
                            var parentOrder = GetLookupBPM("Order", "Id", parentOrderId);
                            var deliveryServiceId = reader.GetColumnValue<Guid>("UsrDeliveryServiceId");
                            var deliveryService = GetLookupBPM("UsrDeliveryService", "Id", deliveryServiceId);
                            var deliveryTypeId = reader.GetColumnValue<Guid>("DeliveryTypeId");
                            var deliveryType = GetLookupBPM("DeliveryType", "Id", deliveryTypeId);
                            var paymentStatusId = reader.GetColumnValue<Guid>("PaymentStatusId");
                            var paymentStatus = GetLookupBPM("OrderPaymentStatus", "Id", paymentStatusId);
                            var typeId = reader.GetColumnValue<Guid>("UsrTypeId");
                            var type = GetLookupBPM("UsrOrderType", "Id", typeId);
                            OrderHeaders.Add(new Order
                            {
                                Number = reader.GetColumnValue<string>("Number"),
                                UsrType = type,
                                UsrId = reader.GetColumnValue<string>("UsrId"),
                                Status = status,
                                Owner = owner,
                                PaymentStatus = paymentStatus,
                                PaymentAmount = reader.GetColumnValue<double>("PaymentAmount"),
                                UsrWeightOrder = reader.GetColumnValue<double>("UsrWeight"),
                                UsrPaymentMethod = reader.GetColumnValue<string>("UsrPaymentMethod"),
                                UsrDeliveryDate = reader.GetColumnValue<DateTime>("UsrDeliveryDate"),
                                Contact = reader.GetColumnValue<string>("ReceiverName"),
                                Email = reader.GetColumnValue<string>("Email"),
                                ContactNumber = reader.GetColumnValue<string>("ContactNumber"),
                                UsrNumberActiveCard = reader.GetColumnValue<string>("UsrNumberActiveCard"),
                                Comment = reader.GetColumnValue<string>("Comment"),
                                UsrParent = parentOrder,
                                UsrDeliveryService = deliveryService,
                                UsrDeliveryDepartment = reader.GetColumnValue<string>("UsrDeliveryDepartment"),
                                DeliveryType = deliveryType,
                                UsrCity = reader.GetColumnValue<string>("UsrCity"),
                                UsrAddress = reader.GetColumnValue<string>("UsrAddress"),
                                UsrHouse = reader.GetColumnValue<string>("UsrHouse"),
                                UsrCorps = reader.GetColumnValue<string>("UsrCorps"),
                                UsrApartment = reader.GetColumnValue<string>("UsrApartment"),
                                UsrCostDelivery = reader.GetColumnValue<double>("UsrCostDelivery"),
                                UsrCityCode = reader.GetColumnValue<string>("UsrCityCode"),
                                UsrStreetCode = reader.GetColumnValue<string>("UsrStreetCode")
                            });
                        }
                    }
                }
            }
            return OrderHeaders;
        }
        public string GenerateOrderHeadersCsv(List<Order> orders)
        {
            var delimiter = ";";
            StringBuilder csvExport = new StringBuilder();
            foreach (var orderHeader in orders)
            {
                csvExport.AppendLine(string.Join(delimiter, new string[] {
                    orderHeader.Number,
                    orderHeader.UsrType,
                    orderHeader.UsrId,
                    orderHeader.Status,
                    orderHeader.Owner,
                    orderHeader.PaymentStatus,
                    orderHeader.PaymentAmount.ToString(),
                    orderHeader.UsrWeightOrder.ToString(),
                    orderHeader.UsrPaymentMethod,
                    orderHeader.UsrDeliveryDate.ToString(),
                    orderHeader.Contact,
                    orderHeader.Email,
                    orderHeader.ContactNumber,
                    orderHeader.UsrNumberActiveCard,
                    orderHeader.Comment,
                    orderHeader.UsrParent,
                    orderHeader.UsrDeliveryService,
                    orderHeader.UsrDeliveryDepartment,
                    orderHeader.DeliveryType,
                    orderHeader.UsrCity,
                    orderHeader.UsrAddress,
                    orderHeader.UsrHouse,
                    orderHeader.UsrCorps,
                    orderHeader.UsrApartment,
					orderHeader.UsrCostDelivery.ToString(),
					orderHeader.UsrCityCode,
					orderHeader.UsrStreetCode
                }));
            }
            return csvExport.ToString();
        }
        public void UploadOrderHeadersCsvFile(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolderOutOrders, string orderHeadersCsv)
        {
            var fileName = dateTime + "Headers.csv";
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolderOutOrders + fileName);
            request.Method = WebRequestMethods.Ftp.UploadFile;

            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
            var text = Encoding.GetEncoding("Windows-1251").GetBytes(orderHeadersCsv);
            request.ContentLength = text.Length;
            request.Proxy = null;
            using (Stream body = request.GetRequestStream())
            {
                body.Write(text, 0, text.Length);
            }
            try
            {
                using (var response = (FtpWebResponse)request.GetResponse())
                {
                    // TODO: nothing; just close
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion

        #region OrderDiscounts
        public List<Order> GetOrderDiscountsInfo(List<Guid> orders)
        {
            var OrderDiscounts = new List<Order>();
            foreach (var orderId in orders)
            {
                Select selectOrder = new Select(userConnection)
                    .Column("UsrShares", "UsrOrderSharesId")
                    .Column("UsrShares", "UsrOrderItemNumber")
                    .Column("UsrShares", "UsrProductSharesId")
                    .Column("UsrShares", "UsrSKU")
                    .Column("UsrShares", "UsrQuantity")
                    .Column("UsrShares", "UsrRuleId")
                    .Column("UsrShares", "UsrRuleName")
                    .Column("UsrShares", "UsrPrice")
                    .Column("UsrShares", "UsrAmountPrice")
                    .Column("UsrShares", "UsrDiscountPercent")
                    .Column("UsrShares", "UsrDiscount")
                    .Column("UsrShares", "UsrDiscountAmount")
                    .Column("UsrShares", "UsrPriceDiscounted")
                    .Column("UsrShares", "UsrAmountPriceDiscounted")
                    .Column("UsrShares", "UsrSetId")
                .From("UsrShares")
                .Join(JoinType.Inner, "Order")
                .On("Order", "Id").IsEqual("UsrShares", "UsrOrderSharesId")
                .Join(JoinType.LeftOuter, "Product")
                .On("Product", "Id").IsEqual("UsrShares", "UsrProductSharesId")
                .Where("UsrShares", "UsrOrderSharesId").IsEqual(Column.Parameter(orderId)) as Select;
                using (DBExecutor dbExecutor = userConnection.EnsureDBConnection())
                {
                    using (IDataReader reader = selectOrder.ExecuteReader(dbExecutor))
                    {
                        while (reader.Read())
                        {
                            var orderSharesId = reader.GetColumnValue<Guid>("UsrOrderSharesId");
                            var order = GetLookupBPM("Order", "Id", orderSharesId);
                            var productSharesId = reader.GetColumnValue<Guid>("UsrProductSharesId");
                            var product = GetLookupBPM("Product", "Id", productSharesId);
                            OrderDiscounts.Add(new Order
                            {
                                Number = order,
                                UsrOrderItemNumber = reader.GetColumnValue<int>("UsrOrderItemNumber"),
                                ProductName = product,
                                UsrSKU = reader.GetColumnValue<string>("UsrSKU"),
                                UsrQuantity = reader.GetColumnValue<double>("UsrQuantity"),
                                UsrRuleId = reader.GetColumnValue<string>("UsrRuleId"),
                                UsrRuleName = reader.GetColumnValue<string>("UsrRuleName"),
                                UsrPrice = reader.GetColumnValue<double>("UsrPrice"),
                                UsrAmountPrice = reader.GetColumnValue<double>("UsrAmountPrice"),
                                UsrDiscountPercent = reader.GetColumnValue<double>("UsrDiscountPercent"),
                                UsrDiscount = reader.GetColumnValue<double>("UsrDiscount"),
                                UsrDiscountAmount = reader.GetColumnValue<double>("UsrDiscountAmount"),
                                UsrPriceDiscounted = reader.GetColumnValue<double>("UsrPriceDiscounted"),
                                UsrAmountPriceDiscounted = reader.GetColumnValue<double>("UsrAmountPriceDiscounted"),
                                UsrSetId = reader.GetColumnValue<double>("UsrSetId")
                            });
                        }
                    }
                }
            }
            return OrderDiscounts;
        }

        public string GenerateOrderDiscountsCsv(List<Order> orders)
        {
            var delimiter = ";";
            StringBuilder csvExport = new StringBuilder();
            foreach (var orderDiscount in orders)
            {
                csvExport.AppendLine(string.Join(delimiter, new string[] {
                    orderDiscount.Number,
                    orderDiscount.UsrOrderItemNumber.ToString(),
                    orderDiscount.ProductName,
                    orderDiscount.UsrSKU,
                    orderDiscount.UsrQuantity.ToString(),
                    orderDiscount.UsrRuleId,
                    orderDiscount.UsrRuleName,
                    orderDiscount.UsrPrice.ToString(),
                    orderDiscount.UsrAmountPrice.ToString(),
                    orderDiscount.UsrDiscountPercent.ToString(),
                    orderDiscount.UsrDiscount.ToString(),
                    orderDiscount.UsrDiscountAmount.ToString(),
                    orderDiscount.UsrPriceDiscounted.ToString(),
                    orderDiscount.UsrAmountPriceDiscounted.ToString(),
                    orderDiscount.UsrSetId.ToString()
                }));
            }
            return csvExport.ToString();
        }

        public void UploadOrderDiscountsCsvFile(string ftpHost, string ftpUserName, string ftpPassword, string ftpFolderOutOrders, string orderDiscountsCsv)
        {
            var fileName = dateTime + "Discounts.csv";
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpHost + ftpFolderOutOrders + fileName);
            request.Method = WebRequestMethods.Ftp.UploadFile;

            request.Credentials = new NetworkCredential(ftpUserName, ftpPassword);
            var text = Encoding.GetEncoding("Windows-1251").GetBytes(orderDiscountsCsv);
            request.ContentLength = text.Length;
            request.Proxy = null;
            using (Stream body = request.GetRequestStream())
            {
                body.Write(text, 0, text.Length);
            }
            try
            {
                using (var response = (FtpWebResponse)request.GetResponse())
                {
                    // TODO: nothing; just close
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        
        #endregion 
        //Получение значения з БД 
        public string GetLookupBPM(string table, string column, Guid value)
        {
            if (value == null)
            {
                return String.Empty;
            }
            if (table == "Order")
            {
                var orderName = (new Select(userConnection).Top(1)
	                .Column("Number")
	                .From(table)
	                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<string>();
            	return orderName;
            }
            if(table == "OrderStatus")
            {
            	var orderStatus = (new Select(userConnection).Top(1)
	                .Column("UsrCode")
	                .From(table)
	                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<string>();
	            return orderStatus;
            }
            if(table == "UsrDeliveryService")
            {
	            var deliveryService = (new Select(userConnection).Top(1)
		                .Column("UsrERPCode")
		                .From(table)
		                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<string>();
		        return deliveryService;
            }
            if(table == "OrderPaymentStatus")
            {
	            var paymentStatus = (new Select(userConnection).Top(1)
		                .Column("UsrPaymentStatusCode")
		                .From(table)
		                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<string>();
		        return paymentStatus;
            }
            if(table == "DeliveryType")
            {
            	var deliveryType = (new Select(userConnection).Top(1)
		                .Column("UsrErpCode")
		                .From(table)
		                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<string>();
		        return deliveryType;
            }
	        var data = (new Select(userConnection).Top(1)
	                .Column("Name")
	                .From(table)
	                .Where(column).IsEqual(Column.Parameter(value)) as Select).ExecuteScalar<string>();
	        return data;
        }
        
        #region FtpLog
        public void InsertErrorMessage(string logMessage)
        {
        	Insert insert = new Insert(userConnection).Into("UsrIntegrationLogFtp")
        		.Set("UsrName", Column.Parameter("Выгрузка данных на FTP"))
                .Set("UsrErrorDescription", Column.Parameter(logMessage));
            insert.Execute();
        }
        #endregion
        #region InsertUnloadDate
        public void InsertUnloadDate(DateTime unloadDate, List<Guid> ordersId)
        {
            foreach (var orderId in ordersId)
            {
                Select s = new Select(userConnection).
                    Column("UsrUnloadDate").From("Order").
                    Where("Id").IsEqual(Column.Parameter(orderId)) as Select;
                using (var dbExecutor = userConnection.EnsureDBConnection())
                {
                    using (var dataReader = s.ExecuteReader(dbExecutor))
                    {
                        if (dataReader.Read())
                        {
                            var date = dataReader.GetColumnValue<DateTime>("UsrUnloadDate");
                            if (date == DateTime.MinValue || date == null)
                            {
                                var orderEntity = userConnection.EntitySchemaManager.GetInstanceByName("Order").CreateEntity(userConnection);
                                if (orderEntity.FetchFromDB("Id", orderId))
                                {
                                    orderEntity.SetColumnValue("UsrUnloadDate", unloadDate);
                                    orderEntity.Save(false);
                                }
                            }
                        }
                    }
                }
            }
        }
        #endregion
    }

    public class Order
    {
        public string Number { get; set; }
        public string Status { get; set; }
        public DateTime ModifiedOn { get; set; }
        public Guid OrderId { get; set; }
        public int UsrPositionNumber { get; set; }
        public string Name { get; set; }
        public string UsrSKU { get; set; }
        public int UsrIsInSet { get; set; }
        public double Quantity { get; set; }
        public string UsrId { get; set; }
        public string Owner { get; set; }
        public double UsrCostDelivery { get; set; }
        public double PaymentAmount { get; set; }
        public string UsrPaymentMethod { get; set; }
        public string Contact { get; set; }
        public string Email { get; set; }
        public string UsrNumberActiveCard { get; set; }
        public string ContactNumber { get; set; }
        public string Comment { get; set; }
        public string UsrParent { get; set; }
        public string UsrDeliveryService { get; set; }
        public string UsrDeliveryDepartment { get; set; }
        public string DeliveryType { get; set; }
        public string UsrCity { get; set; }
        public string UsrAddress { get; set; }
        public string UsrHouse { get; set; }
        public string UsrCorps { get; set; }
        public string UsrApartment { get; set; }
        public double UsrInStock { get; set; }
        public double UsrWeightProduct { get; set; }
        public double UsrOriginalPrice { get; set; }
        public double Price { get; set; }
        public double DiscountAmount { get; set; }
        public double Amount { get; set; }
        public double UsrAmountPriceDiscount { get; set; }
        public string PaymentStatus { get; set; }
        public DateTime UsrDeliveryDate { get; set; }
        public double UsrWeightOrder { get; set; }
        public string UsrType { get; set; }
        public double UsrDiscountedPrice { get; set; }
        public string UsrCityCode {get; set;}
        public string UsrStreetCode {get; set;}
        //Discounts fields
        public Guid UsrOrderSharesId { get; set; }
        public int UsrOrderItemNumber { get; set; }
        public Guid UsrProductSharesId { get; set; }
        public double UsrQuantity { get; set; }
        public string UsrRuleId { get; set; }
        public string UsrRuleName { get; set; }
        public double UsrPrice { get; set; }
        public double UsrAmountPrice { get; set; }
        public double UsrDiscountPercent { get; set; }
        public double UsrDiscount { get; set; }
        public double UsrDiscountAmount { get; set; }
        public double UsrPriceDiscounted { get; set; }
        public double UsrAmountPriceDiscounted { get; set; }
        public double UsrSetId { get; set; }
        public string ProductName { get; set; }
    }

    public class Response
    {
        public bool Success { get; set; }
        public string Error { get; set; }
    }
}



