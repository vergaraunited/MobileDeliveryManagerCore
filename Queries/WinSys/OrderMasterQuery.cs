using UMDGeneral.DataManager.Interfaces;

namespace MobileDataManager.Queries.WinSys
{
    public class OrderMasterQuery : isaWTSQuery
    {
        public OrderMasterQuery(string filename, string sql) { FileName = filename; SQL = sql; }
        public OrderMasterQuery(string filename, string sql, string dateField) { FileName = filename; SQL = sql; DateField = dateField; }
        public OrderMasterQuery(string filename, string sql, string dateField, string additionalConditions) { FileName = filename; SQL = sql; DateField = dateField; AdditionalConditions = additionalConditions; }

        public string SQL { get; set; }
        public string FileName { get; set; }
        public string DateField { get; set; }
        public string AdditionalConditions { get; set; }

    }
}
