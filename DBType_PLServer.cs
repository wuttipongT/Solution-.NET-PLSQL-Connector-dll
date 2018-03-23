using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Oracle.DataAccess.Client;
using System.Data.SqlClient;

namespace SFC_DBType
{
    public class DBType_PLServer : WV_Module
    {

        #region "ENUM"

        public enum eData
        {
            DataReader,
            DataAdapter
        }
        public enum eCommandType
        {
            Text,
            StoredProcedure
        }
        #endregion
        #region "Variable"
        // *** Variable
        private int fldConnectionTimeout = 0;
        public OracleConnection Connection = new OracleConnection(WV_ConnectionString);
        public SqlDataAdapter DataAdapter;
        public OracleCommand PLServerCommand = new OracleCommand();
        public DataSet DS = new DataSet();

        private OracleDataReader dmssql_DataReader;
        private OracleDataAdapter dmssql_DataAdapter = new OracleDataAdapter();
        private string dmssql_ScalarResult = "";
        #endregion
        #region "Constructor"
        public DBType_PLServer() : base()
        {

        }

        #endregion
        #region "DBExeQuery"
        public DataTable DBExeQuery(string StrSQL, eCommandType eCommandType = eCommandType.Text, eData eData = eData.DataReader, int eCommandTimeout = 0) {
            DataTable dt = new DataTable();
            try
            {
                var command = PLServerCommand;
                    command.CommandText = StrSQL;
                    command.Connection = Connection;

                    if (eCommandTimeout == 0)
                    {
                        command.CommandTimeout = fldConnectionTimeout;
                    }
                    else
                    {
                        command.CommandTimeout = eCommandTimeout;
                    }

                    if (eCommandType == eCommandType.StoredProcedure)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandType = CommandType.Text;
                    }

                switch (eData)
                {
                    case eData.DataAdapter:
                        dmssql_DataAdapter.SelectCommand = command;
                        dmssql_DataAdapter.Fill(dt);
                        break;
                    default:
                        DBconnect();
                        dmssql_DataReader = command.ExecuteReader();
                        dt.Load(dmssql_DataReader);
                        dmssql_DataReader.Close();
                        DBdisconnect();
                        break;
                }

                return dt;

            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DataTable DBExeQuery(string StrSQL, OracleConnection Connection, OracleTransaction Transaction, eCommandType eCommandType = eCommandType.Text, eData eData = eData.DataReader, int eCommandTimeout = 0)
        {
            DataTable dt = new DataTable();
            try
            {
                var command = PLServerCommand;
                command.CommandText = StrSQL;
                command.Connection = Connection;
                command.Transaction = Transaction;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                switch (eData)
                {
                    case eData.DataAdapter:
                        dmssql_DataAdapter.SelectCommand = command;
                        dmssql_DataAdapter.Fill(dt);
                        break;
                    default:
                        dmssql_DataReader = command.ExecuteReader();
                        dt = GetDrToDTManuel(dmssql_DataReader);
                        dmssql_DataReader.Close();
                        break;
                }

                return dt;

            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DataTable DBExeQuery(string StrSQL, OracleConnection Connection, OracleTransaction Transaction, OracleCommand pPLServerCommand, eCommandType eCommandType = eCommandType.Text, eData eData = eData.DataReader, int eCommandTimeout = 0)
        {
            DataTable dt = new DataTable();
            try
            {
                var command = pPLServerCommand;
                command.CommandText = StrSQL;
                command.Connection = Connection;
                command.Transaction = Transaction;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                switch (eData)
                {
                    case eData.DataAdapter:
                        dmssql_DataAdapter.SelectCommand = command;
                        dmssql_DataAdapter.Fill(dt);
                        break;
                    default:
                        dmssql_DataReader = command.ExecuteReader();
                        dt = GetDrToDTManuel(dmssql_DataReader);
                        dmssql_DataReader.Close();
                        break;
                }

                return dt;

            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        #endregion
        #region "DBExeNonQuery"
        public int DBExeNonQuery(string StrSQL, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0)
        {
            try
            {
                DBconnect();
                var command = PLServerCommand;
                command.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                DBdisconnect();
                return command.ExecuteNonQuery();

            }
            catch (Exception ex )
            {

                throw ex;
            }
        }
        public int DBExeNonQuery(string StrSQL, OracleConnection Connection, OracleTransaction Transaction, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0)
        {
            try
            {

                var command = PLServerCommand;
                command.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                command.Connection = Connection;
                command.Transaction = Transaction;


                return command.ExecuteNonQuery();

            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public int DBExeNonQuery(string StrSQL, OracleConnection Connection, OracleTransaction Transaction, OracleCommand pPLServerCommand, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0)
        {
            try
            {

                var command = pPLServerCommand;
                command.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                command.Connection = Connection;
                command.Transaction = Transaction;

                return command.ExecuteNonQuery();

            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        #endregion
        #region "DBExeQuery_Scalar"
        public string DBExeQuery_Scalar(string StrSQL, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0)
        {
            try
            {
                DBconnect();
                var command = PLServerCommand;
                    command.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                command.Connection = Connection;
                dmssql_ScalarResult = (string) command.ExecuteScalar();

                DBdisconnect();


            }
            catch (Exception ex)
            {

                throw ex;
            }


            return dmssql_ScalarResult;
        }
        public string DBExeQuery_Scalar(string StrSQL, OracleConnection Connection, OracleTransaction Transaction, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0)
        {
            try
            {

                var command = PLServerCommand;
                command.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                command.Connection = Connection;
                command.Transaction = Transaction;
                dmssql_ScalarResult = (string)command.ExecuteScalar();

            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dmssql_ScalarResult;
        }
        public string DBExeQuery_Scalar(string StrSQL, OracleConnection Connection, OracleTransaction Transaction, OracleCommand pSQLServerCommand, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0)
        {
            try
            {

                var command = pSQLServerCommand;
                command.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    command.CommandTimeout = fldConnectionTimeout;
                }
                else
                {
                    command.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                command.Connection = Connection;
                command.Transaction = Transaction;
                dmssql_ScalarResult = (string)command.ExecuteScalar();

            }
            catch (Exception ex)
            {
                throw ex;
            }


            return dmssql_ScalarResult;
        }
        #endregion
        public DataTable GetDrToDTManuel(OracleDataReader dr)
        {


            DataTable dt = new DataTable();
            GC.Collect();
            //  dr = cmd.ExecuteReader(CommandBehavior.CloseConnection)
            DataTable dtSchema = dr.GetSchemaTable();
            // You can also use an ArrayList instead of List<>
            List<DataColumn> listCols = new List<DataColumn>();
            if ((dtSchema != null))
            {
                foreach (DataRow drow in dtSchema.Rows)
                {
                    string columnName = System.Convert.ToString(drow["ColumnName"]);
                    DataColumn column = new DataColumn(columnName, (Type)drow["DataType"]);
                    column.ReadOnly = false;
                    column.Unique = Convert.ToBoolean(drow["IsUnique"]);
                    column.AllowDBNull = Convert.ToBoolean(drow["AllowDBNull"]);
                    column.AutoIncrement = Convert.ToBoolean(drow["IsAutoIncrement"]);
                    listCols.Add(column);
                    dynamic a = column.ToString();
                    dt.Columns.Add(column);
                }
            }
            while (dr.Read())
            {
                DataRow dataRow = dt.NewRow();
                for (int i = 0; i <= listCols.Count - 1; i++)
                {
                    dataRow[((DataColumn)listCols[i])] = dr[i];
                }
                dt.Rows.Add(dataRow);
            }
            return dt;
        }

        public void DBconnect()
        {
            if (Connection.State == ConnectionState.Open)
                Connection.Close();
            Connection.Open();
        }

        public void DBdisconnect()
        {
            Connection.Close();
        }

    }

    public class WV_Module
    {
        public static string WV_ConnectionString { get; set; }
    }
}
