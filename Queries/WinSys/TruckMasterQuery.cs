using System;
using System.Collections.Generic;
using UMDGeneral.DataManager.Interfaces;

namespace MobileDataManagerWinsys.Queries
{
    public class TruckMasterQuery : isaWTSQuery
    {
        
        public TruckMasterQuery(string filename, string sql) { FileName = filename; SQL = sql; }
        public TruckMasterQuery(string filename, string sql, string dateField) { FileName = filename; SQL = sql; DateField = dateField; }
        public TruckMasterQuery(string filename, string sql, string dateField, string additionalConditions) { FileName = filename; SQL = sql; DateField = dateField; AdditionalConditions = additionalConditions; }

        public string SQL { get; set; }
        public string FileName { get; set; }
        public string DateField { get; set; }
        public string AdditionalConditions { get; set; }
    }
}
