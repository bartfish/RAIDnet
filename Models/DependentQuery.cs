using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace RAIDnet.HostModels
{
    public class DependentQuery
    {
        public DbDescription DatabaseDescription { get; set; }
        public string Query { get; set; }
        public List<SqlParameter> DbSqlParams = new List<SqlParameter>();

        public DependentQuery UpdateQueryParams()
        {
            this.Query = this.Query.Replace("dbName", DatabaseDescription.Name);
            return this;
        }
    }
}
