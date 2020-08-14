using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess.Common
{
    public class DataTransform
    {
        /// <summary>
        /// 建構子
        /// </summary>
        public DataTransform()
        {
        }

        /// <summary>
        /// 把DataSet轉成IEnumerable的方法
        /// </summary>
        /// <typeparam name="T">泛型的Class</typeparam>
        /// <param name="ds">DataSet</param>
        /// <param name="tableIndex">第幾個Table</param>
        /// <returns>IEnumerable資料</returns>
        public IEnumerable<T> DataSetToEnumerable<T>(DataSet ds, int tableIndex)
        {
            foreach (DataRow dataRow in ds.Tables[tableIndex].Rows)
            {
                T t = Activator.CreateInstance<T>();

                foreach (DataColumn dataColumn in ds.Tables[tableIndex].Columns)
                    typeof(T).GetProperty(dataColumn.ColumnName).SetValue(t, dataRow[dataColumn.ColumnName].DbNullToNull(), null);

                yield return t;
                t = default(T);
            }
        }

        /// <summary>
        /// 把DataTable轉成IEnumerable的方法
        /// </summary>
        /// <typeparam name="T">泛型的Class</typeparam>
        /// <param name="dt">DataTable</param>
        /// <returns>IEnumerable資料</returns>
        public IEnumerable<T> DataTableToEnumerable<T>(DataTable dt)
        {
            foreach (DataRow dataRow in dt.Rows)
            {
                T t = Activator.CreateInstance<T>();

                foreach (DataColumn dataColumn in dt.Columns)
                    typeof(T).GetProperty(dataColumn.ColumnName).SetValue(t, dataRow[dataColumn.ColumnName].DbNullToNull(), null);

                yield return t;
                t = default(T);
            }
        }
    }
}
