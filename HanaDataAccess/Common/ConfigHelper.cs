using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess.Common
{
    /// <summary>
    /// Config管理用工具
    /// </summary>
    public class ConfigHelper
    {
        /// <summary>
        /// 建構子
        /// </summary>
        public ConfigHelper()
        {
        }

        /// <summary>
        /// 取得AppSetting的方法
        /// </summary>
        /// <param name="key">KeyName</param>
        /// <returns>Value</returns>
        public static string GetConfigAppSetting(string key)
        {
            return ConfigurationManager.AppSettings[key];
        }

        /// <summary>
        /// 取得ConnectionString的方法
        /// </summary>
        /// <param name="key">KeyName</param>
        /// <returns>Value</returns>
        public static string GetConfigConnectionString(string key)
        {
            return ConfigurationManager.ConnectionStrings[key].ConnectionString;
        }
    }
}
