using CodedThought.Core.Data.Interfaces;
using CodedThought.Core.Exceptions;

using System;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Text;

namespace CodedThought.Core.Data.OleDb {

	/// <summary>OleDbDatabaseObject provides all OleDb specific functionality needed by DBStore and its family of classes.</summary>
	public class OleDbDatabaseObject : DatabaseObject, IDatabaseObject {

		#region Constructor

		public OleDbDatabaseObject(){
		}

		#endregion Constructor

		#region Transaction and Connection Methods

		/// <summary>
		/// Commits updates and inserts.  This is only for Oracle database operations.
		/// </summary>
		public override void Commit() => throw new NotImplementedException();


		/// <summary>
		/// Opens an Oracle Connection
		/// </summary>
		/// <returns></returns>
		protected override IDbConnection OpenConnection()
		{
			try
			{
				OleDbConnection sqlCn = new(ConnectionString);
				sqlCn.Open();
				return sqlCn;
			}
			catch (OleDbException ex)
			{
				throw new ApplicationException("Could not open Connection.  Check connection string" + "/r/n" + ex.Message + "/r/n" + ex.StackTrace, ex);
			}
		}

		#endregion Transaction and Connection Methods

		#region Other Override Methods

		/// <summary>
		/// Tests the connection to the database.
		/// </summary>
		/// <returns></returns>
		public override bool TestConnection() {
			try
			{
				OpenConnection();
				return Connection.State == ConnectionState.Open;
			} catch (CodedThoughtException)
			{
				throw;
			}
		}
		/// <summary>
		/// Creates a Sql Data Adapter object with the passed Command object.
		/// </summary>
		/// <param name="cmd"></param>
		/// <returns></returns>
		protected override IDataAdapter CreateDataAdapter(IDbCommand cmd) => new OleDbDataAdapter(cmd as OleDbCommand);

		/// <summary>Convert any data type to Char</summary>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public override string ConvertToChar(string columnName) => "CONVERT(varchar, " + columnName + ")";
		/// <summary>Creates the parameter collection.</summary>
		/// <returns></returns>
		public override ParameterCollection CreateParameterCollection() => new(this);

		public override IDataParameter CreateApiParameter(string paraemterName, string parameterValue) => throw new NotImplementedException();

		#region Parameters

		/// <summary>Returns the param connector for OleDb, @</summary>
		/// <returns></returns>
		public override string ParameterConnector => "@";

		/// <summary>Gets the wild card character.</summary>
		/// <value>The wild card character.</value>
		public override string WildCardCharacter => "%";

		/// <summary>Gets the column delimiter character.</summary>
		public override string ColumnDelimiter => throw new NotImplementedException();

		/// <summary>Creates the SQL server param.</summary>
		/// <param name="srcTableColumnName">Name of the SRC table column.</param>
		/// <param name="paramType">         Type of the param.</param>
		/// <returns></returns>
		private OleDbParameter CreateDbServerParam(string srcTableColumnName, OleDbType paramType) {
			OleDbParameter param = new(ToSafeParamName(srcTableColumnName), paramType);
			param.SourceColumn = srcTableColumnName;
			return param;
		}

		/// <summary>Creates the SQL server param.</summary>
		/// <param name="srcTableColumnName">Name of the SRC table column.</param>
		/// <param name="paramType">         Type of the param.</param>
		/// <param name="size">              The size.</param>
		/// <returns></returns>
		private OleDbParameter CreateDbServerParam(string srcTableColumnName, OleDbType paramType, int size) {
			OleDbParameter param = new(ToSafeParamName(srcTableColumnName), paramType, size);
			param.SourceColumn = srcTableColumnName;
			return param;
		}

		/// <summary>Creates the XML parameter.</summary>
		/// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
		/// <param name="parameterValue">   The parameter value.</param>
		/// <returns></returns>
		public override IDataParameter CreateXMLParameter(string srcTaleColumnName, string parameterValue) => throw new NotImplementedException();

		/// <summary>Creates a boolean parameter.</summary>
		/// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
		/// <param name="parameterValue">   The parameter value.</param>
		/// <returns></returns>
		public override IDataParameter CreateBooleanParameter(string srcTableColumnName, bool parameterValue) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.Boolean);
			returnValue.Value = parameterValue;
			return returnValue;
		}

		/// <summary>
		/// Creates parameters for the supported database.  
		/// </summary>
		/// <param name="obj">The Business Entity from which to extract the data</param>
		/// <param name="col">The column for which the data must be extracted from the buisiness entity</param>
		/// <param name="store">The store that handles the IO</param>
		/// <returns></returns>
		public override IDataParameter CreateParameter(object obj, TableColumn col, IDBStore store)
		{
			Boolean isNull = false;
			int sqlDataType = 0;

			object extractedData = store.Extract(obj, col.name);
			try
			{
				switch (col.type)
				{
					case DbTypeSupported.dbVarChar:
						isNull = (extractedData == null || (string) extractedData == "");
						sqlDataType = (int) OleDbType.VarChar;
						break;
					case DbTypeSupported.dbInt32:
						isNull = ((int) extractedData == int.MinValue);
						sqlDataType = (int) OleDbType.Integer;
						break;
					case DbTypeSupported.dbDouble:
						isNull = ((double) extractedData == double.MinValue);
						sqlDataType = (int) OleDbType.Double;
						break;
					case DbTypeSupported.dbDateTime:
						isNull = ((DateTime) extractedData == DateTime.MinValue);
						sqlDataType = (int) OleDbType.DBDate;
						break;
					case DbTypeSupported.dbChar:
						isNull = (extractedData == null || System.Convert.ToString(extractedData) == "");
						sqlDataType = (int) OleDbType.Char;
						break;
					case DbTypeSupported.dbBlob:    // Text, not Image
					case DbTypeSupported.dbVarBinary:
						isNull = (extractedData == null);
						sqlDataType = (int) OleDbType.Variant;
						break;
					case DbTypeSupported.dbDecimal:
						isNull = ((decimal) extractedData == decimal.MinValue);
						sqlDataType = (int) OleDbType.Decimal;
						break;
					case DbTypeSupported.dbBit:
						isNull = (extractedData == null);
						sqlDataType = (int) OleDbType.Boolean;
						break;
					default:
						throw new ApplicationException("Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
				}
			}
			catch (Exception ex)
			{
				throw new ApplicationException("Error creating Parameter", ex);
			}

			OleDbParameter parameter = CreateDbServerParam(col.name, (OleDbType) sqlDataType);

			parameter.Value = isNull ? DBNull.Value : extractedData;

			return parameter;
		}

		/// <summary>Create an empty parameter for OleDb</summary>
		/// <returns></returns>
		public override IDataParameter CreateEmptyParameter() {
			IDataParameter returnValue = null;

			returnValue = new OleDbParameter();

			return returnValue;
		}

		/// <summary>
		/// Creates the output parameter.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="returnType">Type of the return.</param>
		/// <returns></returns>
		/// <exception cref="ApplicationException">Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar</exception>
		public override IDataParameter CreateOutputParameter(string parameterName, DbTypeSupported returnType)
		{
			IDataParameter returnParam = null;
			OleDbType sqlDataType;
			switch (returnType)
			{
				case DbTypeSupported.dbVarChar:
					sqlDataType = OleDbType.VarChar;
					break;
				case DbTypeSupported.dbInt32:
					sqlDataType = OleDbType.Integer;
					break;
				case DbTypeSupported.dbDouble:
					sqlDataType = OleDbType.Double;
					break;
				case DbTypeSupported.dbDateTime:
					sqlDataType = OleDbType.DBDate;
					break;
				case DbTypeSupported.dbChar:
					sqlDataType = OleDbType.Char;
					break;
				case DbTypeSupported.dbBlob:    // Text, not Image
					sqlDataType = OleDbType.Variant;
					break;
				case DbTypeSupported.dbDecimal:
					sqlDataType = OleDbType.Decimal;
					break;
				case DbTypeSupported.dbBit:
					sqlDataType = OleDbType.Boolean;
					break;
				default:
					throw new ApplicationException("Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
			}

			returnParam = CreateDbServerParam(parameterName, sqlDataType);
			returnParam.Direction = ParameterDirection.Output;
			return returnParam;

		}

		/// <summary>
		/// Creates and returns a return parameter for the supported database.
		/// </summary>
		/// <param name="parameterName"></param>
		/// <param name="returnType"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationException">Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar</exception>
		public override IDataParameter CreateReturnParameter(string parameterName, DbTypeSupported returnType)
		{
			IDataParameter returnParam = null;
			OleDbType sqlDataType;
			switch (returnType)
			{
				case DbTypeSupported.dbVarChar:
					sqlDataType = OleDbType.VarChar;
					break;
				case DbTypeSupported.dbInt32:
					sqlDataType = OleDbType.Integer;
					break;
				case DbTypeSupported.dbDouble:
					sqlDataType = OleDbType.Double;
					break;
				case DbTypeSupported.dbDateTime:
					sqlDataType = OleDbType.DBDate;
					break;
				case DbTypeSupported.dbChar:
					sqlDataType = OleDbType.Char;
					break;
				case DbTypeSupported.dbBlob:    // Text, not Image
					sqlDataType = OleDbType.Variant;
					break;
				case DbTypeSupported.dbDecimal:
					sqlDataType = OleDbType.Decimal;
					break;
				case DbTypeSupported.dbBit:
					sqlDataType = OleDbType.Boolean;
					break;
				default:
					throw new ApplicationException("Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
			}

			returnParam = CreateDbServerParam(parameterName, sqlDataType);
			returnParam.Direction = ParameterDirection.ReturnValue;
			return returnParam;
		}

		/// <summary>Creates and returns a string parameter for the supported database.</summary>
		/// <param name="srcTableColumnName"></param>
		/// <param name="parameterValue">    </param>
		/// <returns></returns>
		public override IDataParameter CreateStringParameter(string srcTableColumnName, string parameterValue) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.VarChar);
			returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

			return returnValue;
		}

		/// <summary>Creates a Int32 parameter for the supported database</summary>
		/// <param name="srcTableColumnName"></param>
		/// <param name="parameterValue">    </param>
		/// <returns></returns>
		public override IDataParameter CreateInt32Parameter(string srcTableColumnName, int parameterValue) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.Integer);
			returnValue.Value = parameterValue != int.MinValue ? parameterValue : DBNull.Value;

			return returnValue;
		}

		/// <summary>Creates a Double parameter based on supported database</summary>
		/// <param name="srcTableColumnName"></param>
		/// <param name="parameterValue">    </param>
		/// <returns></returns>
		public override IDataParameter CreateDoubleParameter(string srcTableColumnName, double parameterValue) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.Double);
			returnValue.Value = parameterValue != double.MinValue ? parameterValue : DBNull.Value;

			return returnValue;
		}

		/// <summary>Create a data time parameter based on supported database.</summary>
		/// <param name="srcTableColumnName"></param>
		/// <param name="parameterValue">    </param>
		/// <returns></returns>
		public override IDataParameter CreateDateTimeParameter(string srcTableColumnName, DateTime parameterValue) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.DBDate);
			returnValue.Value = parameterValue != DateTime.MinValue ? parameterValue : DBNull.Value;

			return returnValue;
		}

		/// <summary>Creates a Char parameter based on supported database.</summary>
		/// <param name="srcTableColumnName"></param>
		/// <param name="parameterValue">    </param>
		/// <param name="size">              </param>
		/// <returns></returns>
		public override IDataParameter CreateCharParameter(string srcTableColumnName, string parameterValue, int size) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.VarChar);
			returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

			return returnValue;
		}

		/// <summary>Creates a Blob parameter based on supported database.</summary>
		/// <param name="srcTableColumnName"></param>
		/// <param name="parameterValue">    </param>
		/// <param name="size">              </param>
		/// <returns></returns>
		public IDataParameter CreateBlobParameter(string srcTableColumnName, byte[] parameterValue, int size) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.Variant, size);
			returnValue.Value = parameterValue;

			return returnValue;
		}

		/// <summary>Creates the GUID parameter.</summary>
		/// <param name="srcTableColumnName">Name of the SRC table column.</param>
		/// <param name="parameterValue">    The parameter value.</param>
		/// <returns></returns>
		public override IDataParameter CreateGuidParameter(string srcTableColumnName, Guid parameterValue) {
			IDataParameter returnValue = null;

			returnValue = CreateDbServerParam(srcTableColumnName, OleDbType.Guid);
			returnValue.Value = parameterValue;

			return returnValue;
		}

		public override IDataParameter CreateBetweenParameter(string srcTableColumnName, BetweenParameter betweenParam) => throw new NotImplementedException();

		#endregion Parameters

		#region Add method
		/// <summary>
		/// Adds data to the database
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="obj"></param>
		/// <param name="columns"></param>
		/// <param name="store"></param>
		/// <returns></returns>
		/// 

		public override void Add(string tableName, object obj, List<TableColumn> columns, IDBStore store)
		{
			try
			{
				ParameterCollection parameters = new ParameterCollection();
				StringBuilder sbColumns = new StringBuilder();
				StringBuilder sbValues = new StringBuilder();

				for (int i = 0; i < columns.Count; i++)
				{
					TableColumn col = columns[i];

					if (col.isInsertable)
					{
						//we do not insert columns such as autonumber columns
						IDataParameter parameter = CreateParameter(obj, col, store);
						sbColumns.Append(__comma).Append(col.name);
						sbValues.Append(__comma).Append(this.ParameterConnector).Append(parameter.ParameterName);
						parameters.Add(parameter);
					}
				}

				StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
				sql.Append(sbColumns.Remove(0, 2));
				sql.Append(") VALUES (");
				sql.Append(sbValues.Remove(0, 2));
				sql.Append(") ");

				// ================================================================
				// print sql to output window to debuging purpose
				DebugParameters(sql, tableName, parameters);
				// ================================================================

				//Check if we have an identity Column
				if (store.GetPrimaryKey(obj) == 0 || store.GetPrimaryKey(obj) == Int32.MinValue)
				{
					sql.Append("SELECT SCOPE_IDENTITY() ");
					// ExecuteScalar will execute both the INSERT statement and the SELECT statement.
					int retVal = System.Convert.ToInt32(this.ExecuteScalar(sql.ToString(), System.Data.CommandType.Text, parameters));
					store.SetPrimaryKey(obj, retVal);
				}
				else
				{
					this.ExecuteNonQuery(sql.ToString(), System.Data.CommandType.Text, parameters);
				}

				// this is the way to get the CONTEXT_INFO of a SQL connection session
				// string contextInfo = System.Convert.ToString( this.ExecuteScalar( "SELECT dbo.AUDIT_LOG_GET_USER_NAME() ", System.Data.CommandType.Text, null ) );
			}
			catch (ApplicationException irEx)
			{
				RollbackTransaction();
				// this is not a good method to catch DUPLICATE
				if (irEx.Message.IndexOf("duplicate key") >= 0)
				{
					throw new FolderException(irEx.Message, irEx);
				}
				else
				{
					throw new ApplicationException("Failed to add record to: " + tableName + "<BR>" + irEx.Message + "<BR>" + irEx.Source, irEx);
				}
			}
			catch (Exception ex)
			{
				RollbackTransaction();
				throw new ApplicationException("Failed to add record to: " + tableName + "<BR>" + ex.Message + "<BR>" + ex.Source, ex);
			}
		}


		#endregion Add method

		#region Executing Queries





		#endregion Executing Queries

		#region GetValue Methods



		/// <summary>
		/// Get a BLOB from a TEXT or IMAGE column.
		/// In order to get BLOB, a IDataReader's CommandBehavior must be set to SequentialAccess.
		/// That also means to Get columns in sequence is extremely important. 
		/// Otherwise the GetBlobValue method won't return correct data.
		/// [EXAMPLE]
		/// this.DataReaderBehavior = CommandBehavior.SequentialAccess;
		///	using(IDataReader reader = this.ExecuteReader("select BigName, ID, BigBlob from BigTable", CommandType.Text)) 
		///	{
		///		while (reader.Read())
		///		{
		///			string bigName = reader.GetString(0);
		///			int id = this.GetInt32Value( reader, 1);
		///			byte[] bigText = this.GetBlobValue( reader, 2 );
		///		}
		///	}
		/// </summary>
		/// <param name="reader"></param>
		///<param name="columnName"></param>
		/// <returns></returns>
		protected override byte[] GetBlobValue(IDataReader reader, string columnName)
		{

			int position = reader.GetOrdinal(columnName);

			// The DataReader's CommandBehavior must be CommandBehavior.SequentialAccess. 
			if (this.DataReaderBehavior != CommandBehavior.SequentialAccess)
			{
				throw new ApplicationException("Please set the DataReaderBehavior to SequentialAccess to call this method.");
			}
			OleDbDataReader sqlReader = (OleDbDataReader) reader;
			int bufferSize = 100;                   // Size of the BLOB buffer.
			byte[] outBuff = new byte[bufferSize];  // a buffer for every read in "bufferSize" bytes
			long totalBytes;                        // The total chars returned from GetBytes.
			long retval;                            // The bytes returned from GetBytes.
			long startIndex = 0;                    // The starting position in the BLOB output.
			byte[] outBytes = null;                 // The BLOB byte[] buffer holder.

			// Read the total bytes into outbyte[] and retain the number of chars returned.
			totalBytes = sqlReader.GetBytes(position, startIndex, outBytes, 0, bufferSize);
			outBytes = new byte[totalBytes];

			// initial reading from the BLOB column
			retval = sqlReader.GetBytes(position, startIndex, outBytes, 0, bufferSize);

			// Continue reading and writing while there are bytes beyond the size of the buffer.
			while (retval == bufferSize)
			{
				// Reposition the start index to the end of the last buffer and fill the buffer.
				startIndex += bufferSize;
				retval = sqlReader.GetBytes(position, startIndex, outBytes, System.Convert.ToInt32(startIndex), bufferSize);
			}

			return outBytes;
		}

		/// <summary>
		/// Gets a string from a BLOB, Text (SQLServer) or CLOB (Oracle),. developers should use
		/// this method only if they know for certain that the data stored in the field is a string.
		/// </summary>
		/// <param name="reader"></param>
		///<param name="columnName"></param>
		/// <returns></returns>
		public override string GetStringFromBlob(IDataReader reader, string columnName) => System.Text.Encoding.ASCII.GetString(GetBlobValue(reader, columnName));

		#endregion GetValue Methods

		#region Database Specific

		public override string ConnectionName => base.ConnectionName;

		public override DBSupported SupportedDatabase => DBSupported.OleDb;

		public override string GetTableName(string defaultSchema, string tableName)
		{

			if (!String.IsNullOrEmpty(defaultSchema))
			{
				return $"{defaultSchema}.{tableName}";
			}
			else
			{
				return tableName;
			}

		}
		protected override string GetDefaultSessionSchemaNameQuery() => "SELECT DATABASE()";

		/// <summary>
		/// Gets the table definition.
		/// </summary>
		/// <param name="tableName">Name of the table.</param>
		/// <returns></returns>
		/// <exception cref="System.NotImplementedException"></exception>
		protected override String GetTableDefinitionQuery(string tableName) => throw new NotImplementedException();
		/// <summary>
		/// Gets SQL syntax of Year
		/// </summary>
		/// <param name="dateString"></param>
		/// <returns></returns>
		public override string GetYearSQLSyntax(string dateString) => "FORMAT(#" + dateString + "#, \"yyyy\")";
		/// <summary>
		/// Gets database function name
		/// </summary>
		/// <param name="functionName"></param>
		/// <returns></returns>
		public override string GetFunctionName(FunctionName functionName)
		{
			string retStr = string.Empty;
			switch (functionName)
			{
				case FunctionName.SUBSTRING:
					retStr = "LEFT";
					break;
				case FunctionName.ISNULL:
					retStr = "ISNULL";
					break;
				case FunctionName.CURRENTDATE:
					retStr = "NOW()";
					break;
				case FunctionName.CONCATENATE:
					retStr = "&";
					break;
			}
			return retStr;
		}

		/// <summary>
		/// Gets Date string format.
		/// </summary>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="dateFormat">The date format.</param>
		/// <returns></returns>
		public override string GetDateToStringForColumn(string columnName, DateFormat dateFormat)
		{
			StringBuilder sb = new StringBuilder();
			switch (dateFormat)
			{
				case DateFormat.MMDDYYYY:
					sb.Append(" FORMAT(").Append(columnName).Append(", \"mm/dd/yyyy\") ");
					break;
				case DateFormat.MMDDYYYY_Hyphen:
					sb.Append(" FORMAT(").Append(columnName).Append(", \"mm-dd-yyyy\") ");
					break;
				case DateFormat.MonDDYYYY:
					sb.Append(" FORMAT(").Append(columnName).Append(", \"mmm d yyyy\") ");
					break;
				default:
					sb.Append(columnName);
					break;
			}
			return sb.ToString();
		}
		/// <summary>
		/// Gets the date to string for value.
		/// </summary>
		/// <param name="value">The value.</param>
		/// <param name="dateFormat">The date format.</param>
		/// <returns></returns>
		public override string GetDateToStringForValue(string value, DateFormat dateFormat)
		{
			StringBuilder sb = new();
			switch (dateFormat)
			{
				case DateFormat.MMDDYYYY:
					sb.Append(" FORMAT(\"").Append(value).Append("\", \"mm/dd/yyyy\") ");
					break;
				case DateFormat.MMDDYYYY_Hyphen:
					sb.Append(" FORMAT(\"").Append(value).Append("\", \"mm-dd-yyyy\") ");
					break;
				case DateFormat.MonDDYYYY:
					sb.Append(" FORMAT(\"").Append(value).Append("\", \"mmm d yyyy\") ");
					break;
				default:
					sb.Append(value);
					break;
			}
			return sb.ToString();
		}
		/// <summary>
		/// Get CASE (SQL Server) or DECODE (Oracle) SQL syntax.
		/// </summary>
		/// <param name="columnName"></param>
		/// <param name="equalValue"></param>
		/// <param name="trueValue"></param>
		/// <param name="falseValue"></param>
		/// <param name="alias"></param>
		/// <returns></returns>
		public override string GetCaseDecode(string columnName, string equalValue, string trueValue, string falseValue, string alias)
		{
			StringBuilder sb = new();

			sb.Append(" (CASE ").Append(columnName);
			sb.Append(" WHEN ").Append(equalValue);
			sb.Append(" THEN ").Append(trueValue).Append(" ELSE ").Append(falseValue).Append(" END) ");
			sb.Append(alias).Append(" ");

			return sb.ToString();
		}


		/// <summary>
		/// Get an IsNull (SQLServer) or NVL (Oracle)
		/// </summary>
		/// <param name="validateColumnName"></param>
		/// <param name="optionColumnName"></param>
		/// <returns></returns>
		public override string GetIfNullFunction(string validateColumnName, string optionColumnName) => " IsNULL(" + validateColumnName + ", " + optionColumnName + ") ";

		/// <summary>
		/// Get a function name for NULL validation
		/// </summary>
		/// <returns></returns>
		public override string GetIfNullFunction() => "IsNULL";

		/// <summary>
		/// Get a function name that return current date
		/// </summary>
		/// <returns></returns>
		public override string GetCurrentDateFunction() => "Now()";

		/// <summary>
		/// Get a database specific date only SQL syntax.
		/// </summary>
		/// <param name="dateColumn"></param>
		/// <returns></returns>
		public override string GetDateOnlySqlSyntax(string dateColumn) => "CSTR(" + dateColumn + ")";

		/// <summary>
		/// Get a database specific syntax that converts string to date.
		/// Oracle does not convert date string to date implicitly like SQL Server does
		/// when there is a date comparison.
		/// </summary>
		/// <param name="dateString"></param>
		/// <returns></returns>
		public override string GetStringToDateSqlSyntax(string dateString) => "#" + dateString + "# ";

		/// <summary>
		/// Get a database specific syntax that converts string to date.
		/// Oracle does not convert date string to date implicitly like SQL Server does
		/// when there is a date comparison.
		/// </summary>
		/// <param name="dateSQL"></param>
		/// <returns></returns>
		public override string GetStringToDateSqlSyntax(DateTime dateSQL) => "#" + dateSQL.ToString("G", System.Globalization.DateTimeFormatInfo.InvariantInfo) + "# ";


		/// <summary>
		/// Gets  date part(Day, month or year) of date
		/// </summary>
		/// <param name="datestring"></param>
		/// <param name="dateFormat"></param>
		/// <param name="datePart"></param>
		/// <returns></returns>
		public override string GetDatePart(string datestring, DateFormat dateFormat, DatePart datePart)
		{
			string datePartstring = string.Empty;
			switch (datePart)
			{
				case DatePart.DAY:
					datePartstring = $"DAY({datestring})";
					break;
				case DatePart.MONTH:
					datePartstring = $"MONTH({datestring})";
					break;
				case DatePart.YEAR:
					datePartstring = $"YEAR({datestring})";
					break;
			}
			return datePartstring;
		}

		/// <summary>
		/// Convert a datestring to datetime when used for between.... and 
		/// </summary>
		/// <param name="datestring">string</param>
		/// <param name="dateFormat">DatabaseObject.DateFormat</param>
		/// <returns></returns>
		public override string ToDate(string datestring, DateFormat dateFormat) => __singleQuote + datestring + __singleQuote;
		/// <summary>
		/// Converts a database type name to a system type.
		/// </summary>
		/// <param name="dbTypeName">Name of the db type.</param>
		/// <returns>
		/// System.Type
		/// </returns>
		/// <exception cref="System.NotImplementedException"></exception>
		public override Type ToSystemType(string dbTypeName) => throw new NotImplementedException();
		#endregion



		#endregion Other Override Methods
	}
}