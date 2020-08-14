using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess.Common
{
    /// <summary>
    /// 擴充方法
    /// 處理null值問題
    /// </summary>
    public static class ExtensionObject
    {
        /// <summary>
        /// 把DB回傳回來的null值轉成Null
        /// </summary>
        /// <param name="original">DB傳回來的資料</param>
        /// <returns>回傳的值</returns>
        public static object DbNullToNull(this object original)
        {
            return (original == DBNull.Value ? null : original);
        }
    }
}
