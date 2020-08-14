using Sap.Data.Hana;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess.Model
{
    /// <summary>
    /// Cmd用的設定Model
    /// </summary>
    public class CmdSettingModel
    {
        /// <summary>
        /// 連線物件
        /// </summary>
        public HanaConnection Conn { get; set; }
        /// <summary>
        /// 交易物件
        /// </summary>
        public HanaTransaction Trans { get; set; }
        /// <summary>
        /// SQL語法或SP名稱
        /// </summary>
        public string Text { get; set; }
    }
}
