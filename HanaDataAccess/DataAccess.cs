using HanaDataAccess.Common;
using HanaDataAccess.Model;
using Sap.Data.Hana;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HanaDataAccess
{
    public class DataAccess
    {
        #region 變數

        /// <summary>
        /// 私有連線字串
        /// </summary>
        private static string _connectionString;

        /// <summary>
        /// 用DBConnectionString物件設定連線字串
        /// </summary>
        public DBConnectionString ConnectionString
        {
            set
            {
                _connectionString = value.ConnectionString;
            }
        }

        /// <summary>
        /// 存取SP Output用的變數
        /// </summary>
        public HanaParameter[] ParamOutput { get; set; }

        #endregion

        #region 建構子

        /// <summary>
        /// 靜態建構子
        /// </summary>
        static DataAccess()
        {
            _connectionString = "";
        }

        /// <summary>
        /// 建構子
        /// 會取用config中預設key=ConnectionString的連線字串
        /// </summary>
        public DataAccess()
        {
            _connectionString = (new DBConnectionString()).ConnectionString;
            ParamOutput = null;
        }

        /// <summary>
        /// 傳入config中連線字串keyName的建構子
        /// </summary>
        /// <param name="connectionStringKeyName">連線字串KeyName</param>
        public DataAccess(string connectionStringKeyName)
        {
            _connectionString = (new DBConnectionString(ConnectionType.ConnectionKeyName, connectionStringKeyName)).ConnectionString;
            ParamOutput = null;
        }

        /// <summary>
        /// 傳入連線字串物件的建構子
        /// </summary>
        /// <param name="dbconnstr">連線字串物件</param>
        public DataAccess(DBConnectionString dbconnstr)
        {
            _connectionString = dbconnstr.ConnectionString;
            ParamOutput = null;
        }

        #endregion

        #region 回傳DataSet

        /// <summary>
        /// 查詢
        /// 傳入SqlString回傳DataSet
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <returns>查詢的DataSet</returns>
        public DataSet QueryDataSet(string sqlString)
        {
            return QueryDataSet(sqlString, null);
        }

        /// <summary>
        /// 查詢
        /// 傳入SqlString跟SqlParameter參數回傳DataSet
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>查詢的DataSet</returns>
        public DataSet QueryDataSet(string sqlString, object[] paramsArr)
        {
            return QueryDataSet(sqlString, CommandType.Text, paramsArr);
        }

        /// <summary>
        /// 查詢
        /// 傳入SqlString跟使用的Type跟SqlParameter參數回傳DataSet
        /// Text=SQL語法
        /// StoredProcedure=SP
        /// </summary>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>查詢的DataSet</returns>
        public DataSet QueryDataSet(string sqlString, CommandType cmdType, object[] paramsArr)
        {
            DataSet dataSet;
            using (HanaConnection connection = new HanaConnection(_connectionString))
            {
                HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

                HanaParameter[] cmdParams = null;

                if (paramsArr != null)
                {
                    cmdParams = new HanaParameter[paramsArr.Length];
                    SettingParams(cmdParams, paramsArr);
                }

                CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = null, Text = sqlString };
                SettingCommand(cmd, cmdSetModel, cmdParams);

                using (HanaDataAdapter da = new HanaDataAdapter(cmd))
                {
                    DataSet ds = new DataSet();

                    try
                    {
                        da.Fill(ds, "ds");
                        SqlParameterOutputSetting(cmdParams);

                        cmd.Parameters.Clear();
                        dataSet = ds;
                    }
                    catch (HanaException sqlException)
                    {
                        throw new Exception(sqlException.Message);
                    }
                    finally
                    {
                        if (connection.State != ConnectionState.Closed)
                            connection.Close();

                        connection.Dispose();
                        cmd.Dispose();
                    }
                }
            }
            return dataSet;
        }

        #endregion

        #region 回傳DataTable

        /// <summary>
        /// 查詢
        /// 傳入SqlString回傳DataTable
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <returns>查詢的DataTable</returns>
        public DataTable QueryDataTable(string sqlString)
        {
            return QueryDataTable(sqlString, null);
        }

        /// <summary>
        /// 查詢
        /// 傳入SqlString跟HanaParameter參數回傳DataTable
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>查詢的DataTable</returns>
        public DataTable QueryDataTable(string sqlString, object[] paramsArr)
        {
            return QueryDataTable(sqlString, CommandType.Text, paramsArr);
        }

        /// <summary>
        /// 查詢
        /// 傳入SqlString跟使用的Type跟HanaParameter參數回傳DataTable
        /// Text=SQL語法
        /// StoredProcedure=SP
        /// </summary>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>查詢的DataTable</returns>
        public DataTable QueryDataTable(string sqlString, CommandType cmdType, object[] paramsArr)
        {
            DataTable dataTable;
            using (HanaConnection connection = new HanaConnection(_connectionString))
            {
                HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

                HanaParameter[] cmdParams = null;

                if (paramsArr != null)
                {
                    cmdParams = new HanaParameter[paramsArr.Length];
                    SettingParams(cmdParams, paramsArr);
                }

                CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = null, Text = sqlString };
                SettingCommand(cmd, cmdSetModel, cmdParams);

                using (HanaDataAdapter da = new HanaDataAdapter(cmd))
                {
                    DataTable dt = new DataTable();
                    try
                    {
                        try
                        {
                            da.Fill(dt);
                            SqlParameterOutputSetting(cmdParams);

                            cmd.Parameters.Clear();
                            dataTable = dt;
                        }
                        catch (HanaException sqlException)
                        {
                            throw new Exception(sqlException.Message);
                        }
                    }
                    finally
                    {
                        if (connection.State != ConnectionState.Closed)
                            connection.Close();

                        connection.Dispose();
                        cmd.Dispose();
                    }
                }
            }
            return dataTable;
        }

        #endregion

        #region 回傳IEnumerable

        /// <summary>
        /// 查詢
        /// 傳入SqlString回傳IEnumerable
        /// </summary>
        /// <typeparam name="T">回傳的ClassType</typeparam>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <returns>查詢的IEnumerable</returns>
        public IEnumerable<T> QueryDataTable<T>(string sqlString)
        {
            return QueryDataTable<T>(sqlString, null);
        }

        /// <summary>
        /// 查詢
        /// 傳入SqlString跟HanaParameter參數回傳IEnumerable
        /// </summary>
        /// <typeparam name="T">回傳的ClassType</typeparam>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>查詢的IEnumerable</returns>
        public IEnumerable<T> QueryDataTable<T>(string sqlString, object[] paramsArr)
        {
            return QueryDataTable<T>(sqlString, CommandType.Text, paramsArr);
        }

        /// <summary>
        /// 查詢
        /// 傳入SqlString跟使用的Type跟HanaParameter參數回傳IEnumerable
        /// Text=SQL語法
        /// StoredProcedure=SP
        /// </summary>
        /// <typeparam name="T">回傳的ClassType</typeparam>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>查詢的IEnumerable</returns>
        public IEnumerable<T> QueryDataTable<T>(string sqlString, CommandType cmdType, object[] paramsArr)
        {
            IEnumerable<T> enumerableByDataTable;
            using (HanaConnection connection = new HanaConnection(_connectionString))
            {
                HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

                HanaParameter[] cmdParams = null;
                if (paramsArr != null)
                {
                    cmdParams = new HanaParameter[paramsArr.Length];
                    SettingParams(cmdParams, paramsArr);
                }

                CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = null, Text = sqlString };
                SettingCommand(cmd, cmdSetModel, cmdParams);

                using (HanaDataAdapter da = new HanaDataAdapter(cmd))
                {
                    DataTable dt = new DataTable();

                    try
                    {
                        da.Fill(dt);
                        SqlParameterOutputSetting(cmdParams);

                        cmd.Parameters.Clear();
                        enumerableByDataTable = (new DataTransform()).DataTableToEnumerable<T>(dt);
                    }
                    catch (HanaException sqlException)
                    {
                        throw new Exception(sqlException.Message);
                    }
                    finally
                    {
                        if (connection.State != ConnectionState.Closed)
                            connection.Close();

                        connection.Dispose();
                        cmd.Dispose();
                    }
                }
            }
            return enumerableByDataTable;
        }

        #endregion

        #region 新增、修改、刪除

        #region 不包含交易物件

        /// <summary>
        /// 新增、修改、刪除
        /// 不含參數(Parms)的異動資料
        /// 不含交易物件的方法
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <returns>回傳資料異動數</returns>
        public int ExcuteSQL(string sqlString)
        {
            return ExcuteSQL(sqlString, null);
        }

        /// <summary>
        /// 新增、修改、刪除
        /// 傳入參數對SQL資料做更動
        /// 不含交易物件的方法
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料異動數</returns>
        public int ExcuteSQL(string sqlString, object[] paramsArr)
        {
            return ExcuteSQL(sqlString, CommandType.Text, paramsArr);
        }

        /// <summary>
        /// 新增、修改、刪除
        /// 傳入參數對SQL資料做更動
        /// 此方法可自行設定要用哪種Type(SP OR SQLString)
        /// 不含交易物件的方法
        /// </summary>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料異動數</returns>
        public int ExcuteSQL(string sqlString, CommandType cmdType, object[] paramsArr)
        {
            int num;
            using (HanaConnection connection = new HanaConnection(_connectionString))
            {
                int result = 0;
                HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

                HanaParameter[] cmdParams = null;
                if (paramsArr != null)
                {
                    cmdParams = new HanaParameter[paramsArr.Length];
                    SettingParams(cmdParams, paramsArr);
                }

                CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = null, Text = sqlString };
                SettingCommand(cmd, cmdSetModel, cmdParams);

                try
                {
                    if (connection.State != ConnectionState.Open)
                        connection.Open();

                    result = cmd.ExecuteNonQuery();
                    SqlParameterOutputSetting(cmdParams);

                    cmd.Parameters.Clear();
                    num = result;
                }
                catch (HanaException sqlException)
                {
                    throw new Exception(string.Concat("存取SQL Server發生錯誤. SysInfo=", sqlException.Message));
                }
                catch (Exception exception)
                {
                    throw exception;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();

                    connection.Dispose();
                    cmd.Dispose();
                }
            }
            return num;
        }

        #endregion

        #region 包含交易物件

        /// <summary>
        /// 新增、修改、刪除
        /// 不含參數(parms)對SQL資料做更動
        /// 包含交易物件
        /// </summary>
        /// <param name="sqlString"></param>
        /// <param name="connection"></param>
        /// <param name="tran"></param>
        /// <returns></returns>
        public int ExcuteSQL(string sqlString, ref HanaConnection connection, ref HanaTransaction tran)
        {
            return ExcuteSQL(sqlString, ref connection, ref tran, null);
        }

        /// <summary>
        /// 新增、修改、刪除
        /// 傳入參數對SQL資料做更動
        /// 包含交易物件
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="connection">連線物件(為了確保同一個連線)</param>
        /// <param name="tran">交易物件(為了確保同一筆交易)</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料異動數</returns>
        public int ExcuteSQL(string sqlString, ref HanaConnection connection, ref HanaTransaction tran, object[] paramsArr)
        {
            return ExcuteSQL(sqlString, CommandType.Text, ref connection, ref tran, paramsArr);
        }

        /// <summary>
        /// 新增、修改、刪除
        /// 傳入參數對SQL資料做更動
        /// 此方法可自行設定要用哪種Type(SP OR SQLString)
        /// 包含交易物件
        /// </summary>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="connection">連線物件(為了確保同一個連線)</param>
        /// <param name="tran">交易物件(為了確保同一筆交易)</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料異動數</returns>
        public int ExcuteSQL(string sqlString, CommandType cmdType, ref HanaConnection connection, ref HanaTransaction tran, object[] paramsArr)
        {
            int num;
            int result = 0;

            HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

            HanaParameter[] cmdParams = null;
            if (paramsArr != null)
            {
                cmdParams = new HanaParameter[paramsArr.Length];
                SettingParams(cmdParams, paramsArr);
            }

            CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = tran, Text = sqlString };
            SettingCommand(cmd, cmdSetModel, cmdParams);

            try
            {
                result = cmd.ExecuteNonQuery();
                SqlParameterOutputSetting(cmdParams);
                num = result;
            }
            catch (HanaException sqlException)
            {
                throw new Exception(string.Concat("存取SQL Server發生錯誤. SysInfo=", sqlException.Message));
            }
            catch (Exception exception)
            {
                throw exception;
            }
            return num;
        }

        #endregion

        #endregion

        #region 回傳Scalar

        #region 不含交易物件

        /// <summary>
        /// 透過SQLString回傳Scalar(一筆資料的一個欄位)
        /// (不含參數物件)
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <returns>回傳資料</returns>
        public object ExecuteScalar(string sqlString)
        {
            return ExecuteScalar(sqlString, null);
        }

        /// <summary>
        /// 透過SQLString回傳Scalar(一筆資料的一個欄位)
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料</returns>
        public object ExecuteScalar(string sqlString, object[] paramsArr)
        {
            return ExecuteScalar(sqlString, CommandType.Text, paramsArr);
        }

        /// <summary>
        /// 回傳Scalar(一筆資料的一個欄位)
        /// 此方法可自行設定要用哪種Type(SP OR SQLString)
        /// </summary>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料</returns>
        public object ExecuteScalar(string sqlString, CommandType cmdType, object[] paramsArr)
        {
            object obj;
            object result = null;

            using (HanaConnection connection = new HanaConnection(_connectionString))
            {

                HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

                HanaParameter[] cmdParams = null;
                if (paramsArr != null)
                {
                    cmdParams = new HanaParameter[paramsArr.Length];
                    SettingParams(cmdParams, paramsArr);
                }

                try
                {
                    CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = null, Text = sqlString };
                    SettingCommand(cmd, cmdSetModel, cmdParams);

                    result = cmd.ExecuteScalar();
                    SqlParameterOutputSetting(cmdParams);
                    obj = result;
                }
                catch (HanaException sqlException)
                {
                    throw new Exception(string.Concat("存取SQL Server發生錯誤. SysInfo=", sqlException.Message));
                }
                catch (Exception exception)
                {
                    throw exception;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                    {
                        connection.Close();
                        connection.Dispose();
                    }
                    if (cmd != null)
                    {
                        cmd.Dispose();
                        cmd = null;
                    }
                }
            }
            return obj;
        }

        #endregion

        #region 包含交易物件

        /// <summary>
        /// 透過SQLString回傳Scalar(一筆資料的一個欄位)
        /// 不含參數物件
        /// 包含交易物件
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="connection">連線物件(為了確保同一個連線)</param>
        /// <param name="tran">交易物件(為了確保同一筆交易)</param>
        /// <returns>回傳資料</returns>
        public object ExecuteScalarTran(string sqlString, ref HanaConnection connection, ref HanaTransaction tran)
        {
            return ExecuteScalarTran(sqlString, ref connection, ref tran, null);
        }

        /// <summary>
        /// 透過SQLString回傳Scalar(一筆資料的一個欄位)
        /// 包含交易物件
        /// </summary>
        /// <param name="sqlString">SqlSreing語法</param>
        /// <param name="connection">連線物件(為了確保同一個連線)</param>
        /// <param name="tran">交易物件(為了確保同一筆交易)</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料</returns>
        public object ExecuteScalarTran(string sqlString, ref HanaConnection connection, ref HanaTransaction tran, object[] paramsArr)
        {
            return ExecuteScalarTran(sqlString, CommandType.Text, ref connection, ref tran, paramsArr);
        }

        /// <summary>
        /// 回傳Scalar(一筆資料的一個欄位)
        /// 此方法可自行設定要用哪種Type(SP OR SQLString)
        /// 包含交易物件
        /// </summary>
        /// <param name="sqlString">SqlSreing語法或SP名稱</param>
        /// <param name="cmdType">使用的Type</param>
        /// <param name="connection">連線物件(為了確保同一個連線)</param>
        /// <param name="tran">交易物件(為了確保同一筆交易)</param>
        /// <param name="paramsArr">HanaParameter參數陣列</param>
        /// <returns>回傳資料</returns>
        public object ExecuteScalarTran(string sqlString, CommandType cmdType, ref HanaConnection connection, ref HanaTransaction tran, object[] paramsArr)
        {
            object obj;
            object result = null;
            try
            {
                HanaCommand cmd = new HanaCommand() { CommandType = cmdType };

                HanaParameter[] cmdParams = null;
                if (paramsArr != null)
                {
                    cmdParams = new HanaParameter[paramsArr.Length];
                    SettingParams(cmdParams, paramsArr);
                }

                CmdSettingModel cmdSetModel = new CmdSettingModel() { Conn = connection, Trans = tran, Text = sqlString };
                SettingCommand(cmd, cmdSetModel, cmdParams);

                result = cmd.ExecuteScalar();
                SqlParameterOutputSetting(cmdParams);
                obj = result;
            }
            catch (HanaException sqlException)
            {
                throw new Exception(string.Concat("存取SQL Server發生錯誤. SysInfo=", sqlException.Message));
            }
            catch (Exception exception)
            {
                throw exception;
            }
            return obj;
        }

        #endregion

        #endregion

        #region Command、Output相關設定

        /// <summary>
        /// 設置params參數
        /// </summary>
        /// <param name="cmdParams"></param>
        /// <param name="paramsArr"></param>
        private void SettingParams(HanaParameter[] cmdParams, object[] paramsArr)
        {
            int count = paramsArr.Length;
            HanaParameter[] result = new HanaParameter[count];
            if (paramsArr.Length > 0)
                for (int i = 0; i < paramsArr.Length; i++)
                {
                    if (paramsArr[i] == null)
                        cmdParams[i] = new HanaParameter("@p" + i, (object)DBNull.Value);
                    else
                        cmdParams[i] = new HanaParameter("@p" + i, paramsArr[i]);
                }
        }

        /// <summary>
        /// 設定HanaCommand物件的方法
        /// </summary>
        /// <param name="cmd">HanaCommand物件</param>
        /// <param name="cmdSetModel">cmd設定物件</param>
        /// <param name="cmdParams">HanaParameter參數陣列</param>
        private void SettingCommand(HanaCommand cmd, CmdSettingModel cmdSetModel, HanaParameter[] cmdParams)
        {
            cmd.Connection = cmdSetModel.Conn;
            cmd.CommandText = cmdSetModel.Text;

            if (cmdSetModel.Trans != null)
                cmd.Transaction = cmdSetModel.Trans;
            else if (cmdSetModel.Conn.State != ConnectionState.Open)
                cmdSetModel.Conn.Open();

            cmd.Parameters.Clear();
            if (cmdParams != null)
            {
                HanaParameter[] sqlParameterArray = cmdParams;
                for (int i = 0; i < sqlParameterArray.Length; i++)
                {
                    HanaParameter param = sqlParameterArray[i];
                    cmd.Parameters.Add(param);
                }
                SqlParameterOriginal(cmdParams);
            }
        }

        /// <summary>
        /// 把起始的HanaParameter陣列存取到ParamOutput中
        /// </summary>
        /// <param name="cmdParams">HanaParameter參數陣列</param>
        private void SqlParameterOriginal(HanaParameter[] cmdParams)
        {
            ParamOutput = null;
            ParamOutput = (
                from ICloneable x in cmdParams
                select x.Clone() as HanaParameter
                ).ToArray<HanaParameter>();
        }

        /// <summary>
        /// Excute後抓取Output值，寫入到ParamOutput物件上
        /// </summary>
        /// <param name="cmdParams">HanaParameter參數陣列</param>
        private void SqlParameterOutputSetting(HanaParameter[] cmdParams)
        {
            if (cmdParams != null)
            {
                List<HanaParameter> outPutParams = (
                    from x in cmdParams
                    where x.Direction == ParameterDirection.Output
                    select x).ToList();

                foreach (HanaParameter list in outPutParams)
                {
                    //只存取有output值ParameterName寫入
                    (from x in ParamOutput
                     where x.ParameterName == list.ParameterName
                     select x).First().Value = list.Value;
                }
            }
        }

        #endregion
    }
}
