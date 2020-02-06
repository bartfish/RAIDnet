using RAIDnet.HostModels;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Entity;
using System.Data.Entity.Core.EntityClient;
using System.Data.SqlClient;
using System.Reflection;
using System.Text;
using System.Web;

namespace RAIDnet.Managers
{
    public static class DataOperationManager
    {
        private static int nullCounter = 0;
        private static int errorCounter = 1; // if the value is the same as the number of databases return null to the user and to not allow for the operation to continue
        private static DbDescription _currentlyConnectedDb;
        private static List<DbDescription> _currentListOfDbs = new List<DbDescription>();
        private static int wasMirrorUpdated = 0;

        /// <summary>
        /// Method responsible for veryfying the results assigned by the programmer
        /// </summary>
        public static object VerifyResult(Delegate calledMethod, object[] paramsSent, MethodReturnStatus methodReturnStatus)
        {
            if (methodReturnStatus == MethodReturnStatus.Value)
            {
                // if everything went well 
                // return to the method and continue its execution
                return calledMethod.DynamicInvoke(paramsSent);
            }
            else if (methodReturnStatus == MethodReturnStatus.Null)
            {
                // if error occured
                // switch webconfig to the mirror db from other server
                if (nullCounter < _currentListOfDbs.Count / 2)
                {
                    // increment counter
                    nullCounter++;

                    // switch databases
                    SwitchToOtherPart();
 
                    // call the method once again
                    return calledMethod.DynamicInvoke(paramsSent);
                }
                nullCounter = 0;
                return null;
            }
            else if (methodReturnStatus == MethodReturnStatus.Error)
            {
                if (errorCounter <= _currentListOfDbs.Count / 2)
                {
                    errorCounter++;
                    
                    SwitchToMirror();
                  
                    return calledMethod.DynamicInvoke(paramsSent);
                } 
                else
                {
                    errorCounter = 0;
                    return null; 
                }
            }
            else
            {
                return calledMethod.DynamicInvoke(paramsSent);
            }
        }

        public static void InitializeDbsData(List<DbDescription> dbs, DbDescription currDb)
        {
            _currentListOfDbs = dbs;
            _currentlyConnectedDb = currDb;
        }

        public static void SwitchToMirror()
        {
            foreach(var mirror in _currentListOfDbs)
            {
                if (mirror.Name != _currentlyConnectedDb.Name && mirror.MirrorSide == _currentlyConnectedDb.MirrorSide)
                {
                    _currentlyConnectedDb = mirror;
                    break;
                }
            }
            UpdateConnString(_currentlyConnectedDb);
        }

        public static void SwitchToOtherPart()
        {
            foreach (var otherPart in _currentListOfDbs)
            {
                if (otherPart.Name != _currentlyConnectedDb.Name && otherPart.MirrorSide != _currentlyConnectedDb.MirrorSide)
                {
                    _currentlyConnectedDb = otherPart;
                    break;
                }
            }
            UpdateConnString(_currentlyConnectedDb);
        }

        public static string UpdateConnString(DbDescription newDbDescription)
        {
            string initialStr = $"data source={newDbDescription.Server};initial catalog={newDbDescription.Name};user id=" + ConfigurationManager.AppSettings["SqlServerLogin"] + "; password=" + ConfigurationManager.AppSettings["SqlServerPassword"] + "; MultipleActiveResultSets=True;App=EntityFramework";
            HttpContext.Current.Session["dbConnectionString"] = initialStr;
            return initialStr;
        }

        public static void Synch(Delegate calledMethod, object[] paramsSent)
        {
            if (wasMirrorUpdated != _currentlyConnectedDb.DbMirrors.Count)
            {
                wasMirrorUpdated++;
                SwitchToMirror();
                calledMethod.DynamicInvoke(paramsSent);
            }
            else
            {
                wasMirrorUpdated = 0;
                return;
            }
        }

    }
}
