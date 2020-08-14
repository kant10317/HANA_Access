using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess.Common
{
    /// <summary>
    /// 取得連線字串
    /// </summary>
    public class DBConnectionString
    {
        #region 存取連線字串的物件

        /// <summary>
        /// 存取連線字串的物件
        /// </summary>
        public string ConnectionString { get; set; }

        #endregion

        #region 建構子

        /// <summary>
        /// 傳空參數的建構子
        /// 並取得預設KeyName=DefaultConnection的連線字串
        /// </summary>
        public DBConnectionString()
        {
            GetConnection();
        }

        /// <summary>
        /// 傳入連線字串的建構子
        /// </summary>
        /// <param name="connectionString">連線字串</param>
        public DBConnectionString(string connectionString)
        {
            GetConnection(ConnectionType.ConnectionString, connectionString);
        }

        /// <summary>
        /// 傳入取的連線字串的Type
        /// Tpye=DefaultConnection connData可Null or ""
        /// Tpye=ConnectionString connData 為連線字串
        /// Tpye=ConnectionKeyName connData 為 KeyName
        /// </summary>
        /// <param name="connType">連線字串的Type</param>
        /// <param name="connData">依Tpye而定</param>
        public DBConnectionString(ConnectionType connType, string connData)
        {
            switch (connType)
            {
                case ConnectionType.DefaultConnection:
                    {
                        GetConnection();
                        break;
                    }
                case ConnectionType.ConnectionString:
                    {
                        GetConnection(connType, connData);
                        break;
                    }
                case ConnectionType.ConnectionKeyName:
                    {
                        GetConnection(connData);
                        break;
                    }
            }
        }

        #endregion

        #region 取得連線字串的方法

        /// <summary>
        /// 取得預設連線字串的方法
        /// </summary>
        /// <returns>連線字串</returns>
        public string GetConnection()
        {
            return GetConnection(ConnectionType.DefaultConnection, "DefaultConnection");
        }

        /// <summary>
        /// 取得Config中，KeyName連線字串的內容的方法
        /// </summary>
        /// <param name="connectionStringsKeyName">KeyName</param>
        /// <returns>連線字串</returns>
        public string GetConnection(string connectionStringsKeyName)
        {
            return GetConnection(ConnectionType.ConnectionKeyName, connectionStringsKeyName);
        }

        /// <summary>
        /// 自訂義取得連線字串的方法
        /// </summary>
        /// <param name="connType">取得的Type</param>
        /// <param name="connData">Null、連線字串、KeyName</param>
        /// <returns>連線字串</returns>
        public string GetConnection(ConnectionType connType, string connData)
        {
            switch (connType)
            {
                case ConnectionType.DefaultConnection:
                    {
                        ConnectionString = ConfigHelper.GetConfigConnectionString("ConnectionString");
                        break;
                    }
                case ConnectionType.ConnectionString:
                    {
                        ConnectionString = connData;
                        break;
                    }
                case ConnectionType.ConnectionKeyName:
                    {
                        ConnectionString = ConfigHelper.GetConfigConnectionString(connData);
                        break;
                    }
            }
            return ConnectionString;
        }

        #endregion
    }
}
