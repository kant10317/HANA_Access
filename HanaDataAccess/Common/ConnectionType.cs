using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess.Common
{
    /// <summary>
    /// 取得連線字串的Type
    /// </summary>
    public enum ConnectionType
    {
        /// <summary>
        /// 預設連線字串
        /// </summary>
        DefaultConnection,
        /// <summary>
        /// 自訂義連線字串
        /// </summary>
        ConnectionString,
        /// <summary>
        /// 給KeyName的連線字串
        /// </summary>
        ConnectionKeyName
    }
}
