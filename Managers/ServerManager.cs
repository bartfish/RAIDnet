using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;

namespace RAIDnet.Managers
{
    public static class ServerManager
    {
        public static SqlConnection EstablishBackupServerConn(string serverName)
        {
            return new SqlConnection("Server=" + serverName + ";Integrated security=SSPI;database=master");
        }

        public static SqlConnection EstablishBackupServerConnWithCredentials(string serverName, string login, string password)
        {
            return new SqlConnection("Server=" + serverName + ";user id=" + login + ";password=" + password + ";MultipleActiveResultSets=True;");
        }
    }
}
