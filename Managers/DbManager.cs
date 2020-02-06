using System;
using System.Text;
using System.Data.SqlClient;
using System.Configuration;
using System.IO;
using System.Text.RegularExpressions;
using RAIDnet.HostModels;
using System.Data;
using System.Collections.Generic;
using log4net;

namespace RAIDnet.Managers
{
    public static class DbManager
    {
        private static List<string> _listOfDbs = new List<string>();
        private static List<string> _listOfServers = new List<string>();
        private static List<DbDescription> _listOfDbDescriptions { get; set; }

        private static ILog _log { get; set; }
        
        public static void PrepareRAID()
        {
            _listOfDbDescriptions = CheckDatabasesExistence(); 

            for (int i = 0; i < _listOfDbDescriptions.Count; i++) // informing each database about its mirrors
            {
                var mirrorDbs = _listOfDbDescriptions.FindAll(db => ((db.Server != _listOfDbDescriptions[i].Server) && (db.MirrorSide == _listOfDbDescriptions[i].MirrorSide)));
                foreach (var mirror in mirrorDbs)
                    _listOfDbDescriptions[i].DbMirrors.Add(mirror);
            }
            
            foreach (var dbDescription in _listOfDbDescriptions)
            {
                foreach (var dbMirror in dbDescription.DbMirrors)
                {
                    if (!dbMirror.Exists && !dbDescription.Exists)
                    {
                        dbDescription.ShouldBeRecreated = dbMirror.ShouldBeRecreated = CreationType.FromScratch;
                    }
                    else if (!dbMirror.Exists && dbDescription.Exists)
                    {
                        dbMirror.ShouldBeRecreated = CreationType.ByMirroring;
                        dbDescription.ShouldBeRecreated = CreationType.None;
                    }
                }
            }
            
            if (_listOfDbDescriptions.Find(db => db.ShouldBeRecreated == CreationType.FromScratch) != null)
            {
                foreach (var dbDescription in _listOfDbDescriptions)
                {
                    if (dbDescription.ShouldBeRecreated == CreationType.FromScratch)
                    {
                        RunSqlAgainstDatabase(dbDescription, ConfigurationManager.AppSettings["sqlCreateBackupDb"], dbDescription.ServerDirectory);
                    }
                }
                DeleteData();
                SpreadData();
            }
            else
            {
                foreach (var dbDescription in _listOfDbDescriptions)
                {
                    if (dbDescription.ShouldBeRecreated == CreationType.ByMirroring)
                    {
                        // get mirror data
                        var workingMirror = dbDescription.DbMirrors.Find(db => db.ShouldBeRecreated == CreationType.None); // it means the mirror exists and data should be taken from there
                        
                        SynchManager.CreateDbMirror(dbDescription, workingMirror);

                        // update recreation status to none
                        dbDescription.ShouldBeRecreated = CreationType.None;
                    }
                }
            }

            // set d0 db as the main database
            DataOperationManager.InitializeDbsData(_listOfDbDescriptions, _listOfDbDescriptions[0]);
            DataOperationManager.UpdateConnString(_listOfDbDescriptions[0]);
        }

        public static string[] FetchAllTableNames()
        {
            List<string> values = new List<string>();
            foreach (string key in ConfigurationManager.AppSettings)
            {
                if (key.StartsWith("Table"))
                {
                    string value = ConfigurationManager.AppSettings[key];
                    values.Add(value);
                }
            }
            return values.ToArray();
        }
        
        public static List<DependentQuery> BuildInsertsFrom(DbDescription sourceDb, DbDescription destinationDb)
        {
            string[] tables = FetchAllTableNames();
            List<DependentQuery> dpQueries = new List<DependentQuery>();
            foreach (var table in tables)
            {
                string sqlGetAllDataFromTable = string.Format("SELECT * FROM {0}.dbo.{1}", sourceDb.Name, table);
                using (var conn = ServerManager.EstablishBackupServerConnWithCredentials(sourceDb.Server, ConfigurationManager.AppSettings["SqlServerLogin"], ConfigurationManager.AppSettings["SqlServerPassword"]))
                {
                    SqlCommand cmd = new SqlCommand(sqlGetAllDataFromTable, conn);
                    conn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dataTable = new DataTable();
                    da.Fill(dataTable);

                    string insertQuery = "SET IDENTITY_INSERT dbName.dbo." + table + " ON \r\n";
                    insertQuery += "INSERT INTO dbName.dbo." + table + "(";
                    foreach (var column in dataTable.Columns)
                    {
                        insertQuery += column + ",";
                    }
                    insertQuery = insertQuery.Remove(insertQuery.Length - 1);
                    insertQuery += ") Values(";

                    int iterationNumber = 0;
                    foreach (DataRow dataRow in dataTable.Rows)
                    {
                        List<SqlParameter> byteParams = new List<SqlParameter>();
                        string inQueryWithVals = insertQuery;
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            var data = dataRow[column.ToString()];
                            if (data.ToString() == string.Empty)
                            {
                                inQueryWithVals += "null,";
                            }
                            else
                            {
                                if
                                (column.DataType == typeof(string) || column.DataType == typeof(DateTime))
                                {
                                    inQueryWithVals += "'" + dataRow[column.ToString()].ToString() + "',";
                                }
                                else if (column.DataType == typeof(Byte[]))
                                {
                                    string sqlParamName = "@byteArrs" + iterationNumber++;
                                    byte[] xy = (byte[])dataRow[column.ToString()];
                                    inQueryWithVals += sqlParamName + ",";

                                    byteParams.Add(new SqlParameter(sqlParamName, SqlDbType.VarBinary)
                                    {
                                        Direction = ParameterDirection.Input,
                                        Size = 16,
                                        Value = xy
                                    });
                                }
                                else
                                {
                                    inQueryWithVals += dataRow[column.ToString()] + ",";
                                }
                            }
                        }
                        inQueryWithVals = inQueryWithVals.Remove(inQueryWithVals.Length - 1);
                        inQueryWithVals += ")";
                        inQueryWithVals += "\r\n SET IDENTITY_INSERT dbName.dbo." + table + " OFF \r\n";

                        DependentQuery dpQuery = new DependentQuery
                        {
                            DatabaseDescription = destinationDb,
                            Query = inQueryWithVals,
                            DbSqlParams = byteParams
                        };
                        dpQuery = dpQuery.UpdateQueryParams();
                        dpQueries.Add(dpQuery);
                    }

                    conn.Close();
                    da.Dispose();
                }
            }
            return dpQueries;
        }

        public static void RunSqlAgainstDatabase(DbDescription dbDescription, string sqlFileDirectory, string serverDirectory)
        {
            string script = LoadPreparedSqlQueryForDbCreation(sqlFileDirectory, dbDescription.Name, dbDescription.ServerDirectory);

            using (var conn = ServerManager.EstablishBackupServerConnWithCredentials(dbDescription.Server, ConfigurationManager.AppSettings["SqlServerLogin"], ConfigurationManager.AppSettings["SqlServerPassword"]))
            {
                try
                {
                    IEnumerable<string> splitScript = Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase);

                    conn.Open();
                    foreach (string splitted in splitScript)
                    {
                        if (!string.IsNullOrEmpty(splitted.Trim()))
                        {
                            using (var command = new SqlCommand(splitted, conn))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                    conn.Close();

                }
                catch (Exception e)
                {
                    _log.Error(e.Message);
                }

            }
        }

        public static void RunDQueriesAcrossDb(List<DependentQuery> dependentQueries)
        {
            // run sql queries on each database
            foreach (var currQuery in dependentQueries)
            {
                using (var conn = ServerManager.EstablishBackupServerConnWithCredentials(currQuery.DatabaseDescription.Server, ConfigurationManager.AppSettings["SqlServerLogin"], ConfigurationManager.AppSettings["SqlServerPassword"]))
                {
                    try
                    {
                        conn.Open();
                        using (var command = new SqlCommand(currQuery.Query, conn))
                        {
                            command.Parameters.AddRange(currQuery.DbSqlParams.ToArray());
                            command.ExecuteNonQuery();
                            command.Parameters.Clear();
                        }
                        conn.Close();

                    }
                    catch (Exception e)
                    {
                        _log.Error(e.Message);
                    }

                }
            }
        }

        private static void InitializeLists()
        {
            _listOfDbs = new List<string>();
            _listOfServers = new List<string>();

            _listOfDbs.Add(ConfigurationManager.AppSettings["D0_Database"]);
            _listOfDbs.Add(ConfigurationManager.AppSettings["D1_Database"]);
            _listOfDbs.Add(ConfigurationManager.AppSettings["D2_Database"]);
            _listOfDbs.Add(ConfigurationManager.AppSettings["D3_Database"]);

            _listOfServers.Add(ConfigurationManager.AppSettings["DbServer_One"]);
            _listOfServers.Add(ConfigurationManager.AppSettings["DbServer_Two"]);         
        }
        
        private static void DeleteData()
        {
            // fetch all data from each table from master database
            string[] tables = FetchAllTableNames();
            foreach (var dbDesc in _listOfDbDescriptions)
            {
                foreach (var table in tables)
                {
                    string sqlDeleteDataFromTable = string.Format("DELETE FROM {0}.dbo.{1}", dbDesc.Name, table);
                    using (var conn = ServerManager.EstablishBackupServerConnWithCredentials(dbDesc.Server, ConfigurationManager.AppSettings["SqlServerLogin"], ConfigurationManager.AppSettings["SqlServerPassword"]))
                    {
                        conn.Open();
                        var command = new SqlCommand(sqlDeleteDataFromTable, conn);
                        command.ExecuteNonQuery();
                        conn.Close();
                    }
                }
            }
        }

        private static void SpreadData()
        {
            // fetch all data from each table from master database
            string[] tables = FetchAllTableNames();
            List<DependentQuery> dpQueries = new List<DependentQuery>();
            foreach (var table in tables)
            {
                string sqlGetAllDataFromTable = string.Format("SELECT * FROM {0}.dbo.{1}", ConfigurationManager.AppSettings["DbMasterDatabase"], table);
                using (var conn = ServerManager.EstablishBackupServerConnWithCredentials(ConfigurationManager.AppSettings["DbServer_One"], ConfigurationManager.AppSettings["SqlServerLogin"], ConfigurationManager.AppSettings["SqlServerPassword"]))
                {
                    SqlCommand cmd = new SqlCommand(sqlGetAllDataFromTable, conn);
                    conn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dataTable = new DataTable();
                    da.Fill(dataTable);

                    // initialize columns into insert statement
                    // build insert sql query for each table (2 inserts per table, each containing half of the data)
                    string insertQuery = "SET IDENTITY_INSERT dbName.dbo." + table + " ON \r\n";
                    insertQuery += "INSERT INTO dbName.dbo." + table + "(";
                    foreach (var column in dataTable.Columns)
                    {
                        insertQuery += column + ",";
                    }
                    insertQuery = insertQuery.Remove(insertQuery.Length - 1);
                    insertQuery += ") Values(";

                    int dbParity = 0;
                    int iterationNumber = 0;
                    foreach (DataRow dataRow in dataTable.Rows)
                    {
                        List<SqlParameter> byteParams = new List<SqlParameter>();
                        string inQueryWithVals = insertQuery;
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            var data = dataRow[column.ToString()];
                            if (data.ToString() == string.Empty)
                            {
                                inQueryWithVals += "null,";
                            }
                            else
                            {
                                if
                                (column.DataType == typeof(string) || column.DataType == typeof(DateTime))
                                {
                                    inQueryWithVals += "'" + dataRow[column.ToString()].ToString() + "',";
                                }
                                else if (column.DataType == typeof(Byte[]))
                                {
                                    string sqlParamName = "@byteArrs" + iterationNumber++;
                                    byte[] xy = (byte[])dataRow[column.ToString()];
                                    inQueryWithVals += sqlParamName + ",";

                                    byteParams.Add(new SqlParameter(sqlParamName, SqlDbType.VarBinary)
                                    {
                                        Direction = ParameterDirection.Input,
                                        Size = 16,
                                        Value = xy
                                    });
                                }
                                else
                                {
                                    inQueryWithVals += dataRow[column.ToString()] + ",";
                                }
                            }
                        }
                        inQueryWithVals = inQueryWithVals.Remove(inQueryWithVals.Length - 1);
                        inQueryWithVals += ")";
                        inQueryWithVals += "\r\n SET IDENTITY_INSERT dbName.dbo." + table + " OFF \r\n";
                        
                        List<DbDescription> dbsToAssign = new List<DbDescription>();
                        // assign to which database on which server the specific built in the next step sql query is going to be run
                        if (dbParity % 2 == 0)
                        {
                            var x1 = _listOfDbDescriptions
                                .Find(d => d.Name == ConfigurationManager.AppSettings["D0_Database"]);
                            var x2 = _listOfDbDescriptions
                                .Find(d => d.Name == ConfigurationManager.AppSettings["D2_Database"]);
                            dbsToAssign.Add(x1);
                            dbsToAssign.Add(x2);
                            
                        }
                        else
                        {
                            var x1 = _listOfDbDescriptions
                                .Find(d => d.Name == ConfigurationManager.AppSettings["D1_Database"]);
                            var x2 = _listOfDbDescriptions
                                .Find(d => d.Name == ConfigurationManager.AppSettings["D3_Database"]);
                            dbsToAssign.Add(x1);
                            dbsToAssign.Add(x2);
                        }

                        foreach (var dbToAssign in dbsToAssign)
                        {
                            DependentQuery dpQuery = new DependentQuery();
                            dpQuery.DatabaseDescription = dbToAssign;
                            dpQuery.Query = inQueryWithVals;
                            dpQuery.DbSqlParams = byteParams;
                            dpQuery = dpQuery.UpdateQueryParams();
                            dpQueries.Add(dpQuery);
                        }
                        dbParity++;
                    }
                    conn.Close();
                    da.Dispose();
                }


            }

            // run sql queries on each database
            foreach (var currQuery in dpQueries)
            {
                using (var conn = ServerManager.EstablishBackupServerConnWithCredentials(currQuery.DatabaseDescription.Server, ConfigurationManager.AppSettings["SqlServerLogin"], ConfigurationManager.AppSettings["SqlServerPassword"]))
                {
                    try
                    {
                        conn.Open();
                        using (var command = new SqlCommand(currQuery.Query, conn))
                        {
                            command.Parameters.AddRange(currQuery.DbSqlParams.ToArray());
                            command.ExecuteNonQuery();
                            command.Parameters.Clear();
                        }
                        conn.Close();

                    }
                    catch (Exception e)
                    {
                        _log.Error(e.Message);
                    }

                }
            }

        }


        private static string LoadPreparedSqlQueryForDbCreation(string sqlDirectoryToModify, string dbName, string serverDirectory)
        {
            string script = File.ReadAllText(sqlDirectoryToModify);
            script = script.Replace("NAME_OF_THE_DATABASE_FROM_CREATE_DATABASE_QUERY_WHICH_IS_TO_BE_REPLACED", dbName);
            script = script.Replace("SERVER_DIRECTORY\\", serverDirectory);

            return script;
        }

        private static List<DbDescription> CheckDatabasesExistence()
        {
            bool dbExists = false;
            int dbId = 0;

            InitializeLists();
            List<DbDescription> dbDescriptions = new List<DbDescription>();
            try
            {
                int counter = 0, serverNum = 0, numberOfServers = 2;
                string serverDirectory;
                foreach (var currentServer in _listOfServers)
                {
                    serverDirectory = serverNum == 0 
                        ? 
                        ConfigurationManager.AppSettings["DbServer_One_Directory"] 
                        : 
                        ConfigurationManager.AppSettings["DbServer_Two_Directory"];

                    // establish connection with the first server and check if specific databases are in there
                    using (var conn = ServerManager.EstablishBackupServerConn(currentServer))
                    {
                        conn.Open();

                        for(int i = counter; i < _listOfDbs.Count; i++)
                        {
                            if (serverNum == 0)
                            {
                                if (counter >= numberOfServers)
                                {
                                    serverNum++;
                                    break;
                                }
                            }
                            
                            // check if on this server, there exist all databases
                            string sqlDbIsOnTheServer = string.Format("SELECT database_id FROM sys.databases WHERE Name = '{0}'", _listOfDbs[i]);
                            using (SqlCommand sqlCmd = new SqlCommand(sqlDbIsOnTheServer, conn))
                            {
                                var foundRow = sqlCmd.ExecuteScalar();

                                if (foundRow != null)
                                    int.TryParse(foundRow.ToString(), out dbId);
                                else
                                    dbId = 0;

                                dbExists = dbId != 0;

                                dbDescriptions.Add(new DbDescription()
                                {
                                    Name = _listOfDbs[i],
                                    Server = currentServer,
                                    Exists = dbExists,
                                    IsCurrentlyConnected = dbExists,
                                    ShouldBeRecreated = CreationType.None,
                                    MirrorSide = (i % 2 == 0) ? MirrorSide.Left : MirrorSide.Right,
                                    ServerDirectory = serverDirectory
                                });
                            }
                            counter++;
                        }
                        conn.Close();
                    }
                }
            }
            catch (Exception e)
            {
                _log.Error(e.Message);
            }
            return dbDescriptions;
        }

    }
}

