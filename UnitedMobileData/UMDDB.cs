using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using MobileDeliveryGeneral.Data;
using MobileDeliveryGeneral.DataManager.Interfaces;
using MobileDeliveryGeneral.Interfaces.DataInterfaces;
using static MobileDeliveryGeneral.Definitions.enums;
using static MobileDeliveryGeneral.Definitions.MsgTypes;
using MobileDeliveryGeneral.ExtMethods;
using MobileDeliveryLogger;

namespace MobileDataManager.UnitedMobileData
{
    public class UMDDB : isaDataAccess<SqlConnection, SqlCommand, SqlDataReader>
    {
        static public string connectionString { get; private set; }
        public SqlConnection cnn { get; private set; }
        public SqlCommand cmd { get; private set; }

        public UMDDB(string strCon)
        {
            connectionString = strCon;
        }

        SqlConnection NewConnection() {
            Logger.Info($"DB SQLConn Str = {connectionString}");
            SqlConnection con = new SqlConnection(connectionString);
            //con.
            con.Open();
            return con;
        }
        SqlCommand NewCommand(string SQL)
        {
            return new SqlCommand(SQL, cnn);
        }
        
        public IEnumerable<IMDMMessage> InsertData(SPCmds SP, IMDMMessage dat)
        {
            if (dat != null)
            {
                switch (SP)
                {
                    case SPCmds.INSERTMANIFEST:
                        ManifestMasterData mst = (ManifestMasterData)dat;
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                long manid;
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@DriverId", SqlDbType.Int).Value = 9;  //use mst.UserId query the Driver table
                                    command.Parameters.AddWithValue("@TRK_CDE", SqlDbType.VarChar).Value = mst.TRK_CDE;
                                    command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = mst.SHIP_DTE.Date;
                                    command.Parameters.AddWithValue("@DESC$", SqlDbType.VarChar).Value = mst.Desc;
                                    command.Parameters.AddWithValue("@LINK", SqlDbType.BigInt).Value = mst.LINK;
                                    command.Parameters.AddWithValue("@NOTES", SqlDbType.VarChar).Value = mst.NOTES;
                                    var retVal = command.Parameters.Add("@ManifestId", SqlDbType.BigInt);
                                    retVal.Direction = ParameterDirection.Output;

                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();
                                    manid= (long)retVal.Value;
                                    mst.ManifestId = manid;
                                    yield return mst;
                                }
                            }
                        }
                        break;
                    case SPCmds.INSERTMANIFESTDETAILS:
                        ManifestDetailsData mdd = (ManifestDetailsData)dat;
                        //SQL = @"";
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = mdd.ManId;
                                    command.Parameters.AddWithValue("@DSP_SEQ", SqlDbType.Int).Value = mdd.DSP_SEQ;
                                    command.Parameters.AddWithValue("@DLR_NO", SqlDbType.BigInt).Value = mdd.DLR_NO;

                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();
                                    
                                    yield return mdd;
                                }
                            }
                        }
                        break;
                    case SPCmds.INSERTORDER:
                        OrderMasterData omd = (OrderMasterData)dat;
                        //SQL = @"";
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;

                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = omd.ORD_NO;
                                    command.Parameters.AddWithValue("@DLR_NO", SqlDbType.BigInt).Value = omd.DLR_NO;
                                    command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = omd.SHP_DTE.Date;
                                    command.Parameters.AddWithValue("@DLR_NME", SqlDbType.VarChar).Value = omd.DLR_NME;
                                    command.Parameters.AddWithValue("@SHP_NME", SqlDbType.VarChar).Value = omd.SHP_NME;
                                    command.Parameters.AddWithValue("@SHP_ADDR", SqlDbType.VarChar).Value = omd.SHP_ADDR;
                                    command.Parameters.AddWithValue("@SHP_ADDR2", SqlDbType.VarChar).Value = omd.SHP_ADDR2;
                                    command.Parameters.AddWithValue("@SHP_TEL", SqlDbType.VarChar).Value = omd.SHP_TEL;
                                    command.Parameters.AddWithValue("@SHP_ZIP", SqlDbType.VarChar).Value = omd.SHP_ZIP;
                                    command.Parameters.AddWithValue("@CUS_NME", SqlDbType.VarChar).Value = omd.CUS_NME;
                                    command.Parameters.AddWithValue("@RTE_CDE", SqlDbType.VarChar).Value = omd.RTE_CDE;
                                    command.Parameters.AddWithValue("@SHP_QTY", SqlDbType.Int).Value = omd.SHP_QTY;
                                    command.Parameters.AddWithValue("@MAN_ID", SqlDbType.Int).Value = omd.ManId;
                                    var retVal = command.Parameters.Add("@CustomerIdOut", SqlDbType.Int);
                                    retVal.Direction = ParameterDirection.Output;

                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();
                                    
                                    yield return omd;
                                }
                            }
                        }
                        break;
                    case SPCmds.INSERTORDERDETAILS:
                        OrderDetailsData odd = (OrderDetailsData)dat;
                        //SQL = @"";
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = odd.ORD_NO;
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.Int).Value = odd.MDL_NO;
                                    command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = odd.DESC;

                                    command.Parameters.AddWithValue("@CLR", SqlDbType.Int).Value = odd.CLR;
                                    command.Parameters.AddWithValue("@WIDTH", SqlDbType.BigInt).Value = odd.WIDTH;
                                    command.Parameters.AddWithValue("@HEIGHT", SqlDbType.BigInt).Value = odd.HEIGHT;
                                    command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.Int).Value = odd.MDL_CNT;
                                    command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.Int).Value = odd.WIN_CNT;
                                    command.Parameters.AddWithValue("@Status", SqlDbType.VarChar).Value = "LOADED";
                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();
                                    yield return odd;
                                }
                            }
                        }
                        break;
                    case SPCmds.INSERTORDEROPTIONS:
                        OrderOptionsData oop = (OrderOptionsData)dat;
                        //SQL = @"";
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {

                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = oop.ORD_NO;
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.Int).Value = oop.MDL_NO;
                                    command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = oop.DESC;

                                    command.Parameters.AddWithValue("@CLR", SqlDbType.Int).Value = oop.CLR;
                                    command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.Int).Value = oop.MDL_CNT;
                                   // command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.Int).Value = oop.WIN_CNT;
                                    command.Parameters.AddWithValue("@Status", SqlDbType.VarChar).Value = "LOADED";
                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();

                                    yield return oop;
                                }
                            }
                        }
                        break;
                    default:
                        throw new Exception("UM SQL Server Stored Procedure Not Found.");
                }
            }
            yield break;
        }

        public bool InsertData(string sql, IMDMMessage msg) { throw new Exception("not impl"); }
        IMDMMessage MyQueryReader_OrderMaster(DbDataReader reader)
        {
            //ManifestMasterData md = new ManifestMasterData();
            try
            {
                string sValue1 = "";
                int sValue2 = 0;

                sValue1 = reader.GetString(reader.GetOrdinal("WINSYSLITEORDER"));
                sValue2 = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                if (sValue1 != "0")
                {
                   // md.Desc = sValue1;
                    //md.ManifestId = sValue2;
                }
            }
            catch (Exception ex) { }
            return null;
        }
        public static isaCommand MyQueryReader_Driver(SqlDataReader reader, Func<byte[], Task> cb)
        {
            drivers md = new drivers();
            try
            {
                int did;
                string fname;
                string lname;
                
                    did = reader.GetInt32(reader.GetOrdinal("DriverId"));
                    fname = reader.GetString(reader.GetOrdinal("FirstName"));
                    lname = reader.GetString(reader.GetOrdinal("LastName"));
                    if (did != 0)
                    {
                        md.DriverId = did;
                        md.FirstName = fname;
                        md.LastName = lname;
                    }
                cb(md.ToArray());
            }
            catch (Exception ex) { }
            return md;
        }
        public static isaCommand MyQueryReader_Trucks(SqlDataReader reader, Func<byte[], Task> cb)
        {
            trucks trk = new trucks();
            try
            {
                trk.ManifestId = reader.GetInt64(reader.GetOrdinal("ManifestId"));
                trk.DriverId = reader.GetInt32(reader.GetOrdinal("DriverId"));
                trk.FirstName = reader.GetString(reader.GetOrdinal("Firstname"));
                trk.LastName = reader.GetString(reader.GetOrdinal("Lastname"));
                trk.TruckCode = reader.GetString(reader.GetOrdinal("TRK_CDE"));
                trk.ShipDate = reader.GetDateTime(reader.GetOrdinal("SHP_DTE")).ToBinary();
                trk.Description = reader.GetString(reader.GetOrdinal("DESC$"));
                trk.Notes = reader.GetString(reader.GetOrdinal("NOTES"));
                Logger.Info($"Truck: {trk.ManifestId} {trk.LastName} {trk.TruckCode} {trk.Notes}");
            }
            catch (Exception ex) { }
            return trk;
        }
        public static isaCommand MyQueryReader_Stops(SqlDataReader reader, Func<byte[], Task> cb)
        {
            stops st = new stops();
            try
            {
                st.ManifestId = reader.GetInt64(reader.GetOrdinal("ManifestId"));

                st.DisplaySeq = reader.GetInt32(reader.GetOrdinal("DSP_SEQ"));
                st.DealerNo = reader.GetInt64(reader.GetOrdinal("DLR_NO"));

                if (reader.IsDBNull(reader.GetOrdinal("CUS_NME")))
                    st.DealerName = "";
                else
                    st.DealerName = reader.GetString(reader.GetOrdinal("CUS_NME"));

                st.Address = reader.GetString(reader.GetOrdinal("SHP_ADDR"));
                st.PhoneNumber = reader.GetString(reader.GetOrdinal("SHP_TEL"));

                st.Description = reader.GetString(reader.GetOrdinal("DESC$"));
                st.Notes = reader.GetString(reader.GetOrdinal("NOTES"));
                st.TRK_CDE = reader.GetString(reader.GetOrdinal("TRK_CDE"));
                st.CustomerId = reader.GetInt32(reader.GetOrdinal("CustomerId"));
                //st.BillComplete = reader.GetBoolean(reader.GetOrdinal("BillComplete"));
                Logger.Info($"Stop: {st.ManifestId} {st.DisplaySeq} {st.DealerNo} {st.TRK_CDE}");
            }
            catch (Exception ex) { }
            return st;
        }
        public static isaCommand MyQueryReader_Orders(SqlDataReader reader, Func<byte[], Task> cb)
        {
            orders ord = new orders();
            try
            {
                ord.requestId = Guid.Empty.ToByteArray();
                ord.ManifestId = reader.GetInt64(reader.GetOrdinal("ManifestId"));
                ord.DSP_SEQ = reader.GetInt32(reader.GetOrdinal("DSP_SEQ"));
                ord.CustomerId = reader.GetInt32(reader.GetOrdinal("CustomerId"));
                ord.DLR_NO = reader.GetInt64(reader.GetOrdinal("DLR_NO"));
                ord.ORD_NO = reader.GetInt64(reader.GetOrdinal("ORD_NO"));
                ord.CLR = reader.GetString(reader.GetOrdinal("CLR"));
                ord.MDL_CNT = reader.GetInt32(reader.GetOrdinal("MDL_CNT"));
                ord.MDL_NO = reader.GetInt32(reader.GetOrdinal("MDL_NO"));
                ord.WIN_CNT = reader.GetInt32(reader.GetOrdinal("WIN_CNT"));
                ord.command = eCommand.OrdersLoad;
            }
            catch (Exception ex) { }
            return ord;
        }

        public static isaCommand myQueryData_GetManifest(SqlDataReader reader, Func<byte[], Task> cb)
        {
            manifestMaster mm = new manifestMaster();
            try
            {
                mm.id = reader.GetInt64(reader.GetOrdinal("ManifestId"));
                mm.DriverId = reader.GetInt32(reader.GetOrdinal("DriverId"));
                mm.TRK_CDE = reader.GetString(reader.GetOrdinal("TRK_CDE")).GetBytes();
                mm.SHIP_DTE = reader.GetDateTime(reader.GetOrdinal("SHP_DTE")).ToBinary();
                mm.DESC = reader.GetString(reader.GetOrdinal("DESC$")).StringToByteArray(fldsz_DESC);
                mm.NOTES = reader.GetString(reader.GetOrdinal("NOTES")).StringToByteArray(fldsz_NOTES * sizeof(char)); ;
                mm.LINK = reader.GetInt64(reader.GetOrdinal("LINK"));
                //mm.command = eCommand.ManifestLoadComplete;
            }
            catch (Exception ex) { }
            return mm;
        }

        public IMDMMessage QueryData(Func<byte[], Task> cb, isaCommand dat)
        {
            IMDMMessage retVal = null;
            SPCmds SP;
            manifestRequest req; // (manifestRequest)dat;
            switch (dat.command)
            {
                case eCommand.Drivers:
                    SP = SPCmds.GETAVAILABLEDRIVERS;
                    break;
                case eCommand.Manifest:
                    SP = SPCmds.GETDRIVERMANIFEST;
                    break;
                case eCommand.Trucks:
                    SP = SPCmds.GETTRUCKS;
                    break;
                case eCommand.Stops:
                    SP = SPCmds.GETSTOPS;
                    break;
                case eCommand.Orders:
                case eCommand.OrdersLoad:
                    SP = SPCmds.GETORDERS;
                    break;
                case eCommand.GenerateManifest:
                    SP = SPCmds.GETMANIFEST;
                    break;
                default:
                    throw new Exception("Not handled");
            }

            using (SqlConnection cnn = new SqlConnection(UMDDB.connectionString))
            {
                using (var adapter = new SqlDataAdapter())
                {
                    using (var command = new SqlCommand(SP.ToString(), cnn))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        switch (dat.command)
                        {
                            case eCommand.Drivers:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd");
                                dat.command = eCommand.DriversLoadComplete;
                                retVal = new DriverData() { RequestId = new Guid(req.requestId) };
                                Logger.Info($"QueryData Drivers. Cmd:{retVal.Command.ToString()}  reqId: {retVal.RequestId.ToString()} SHP_DTE: {DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd")}");
                                break;
                            case eCommand.Manifest:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@DriverId", SqlDbType.Int).Value = req.id;
                                retVal = new ManifestMasterData() { RequestId = new Guid(req.requestId) };
                                Logger.Info($"QueryData Manifest. Cmd:{retVal.Command.ToString()}  reqId: {retVal.RequestId.ToString()}  DriverId: {req.id} ");
                                dat.command = eCommand.ManifestLoadComplete;
                                break;
                            case eCommand.Trucks:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd");
                                dat.command = eCommand.TrucksLoadComplete;
                                Logger.Info($"QueryData Trucks. Cmd:{req.command.ToString()}  reqId: {new Guid(req.requestId).ToString()} SHP_DTE: {DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd")}");

                                retVal = new TruckData() { RequestId = new Guid(req.requestId) };
                                break;
                            case eCommand.Stops:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = req.id;
                                dat.command = eCommand.StopsLoadComplete;
                                Logger.Info($"QueryData Stops. Cmd:{req.command.ToString()}  reqId: {new Guid(req.requestId).ToString()}  manId: {req.id}");
                                retVal = new StopData() { RequestId = new Guid(req.requestId) };
                                break;
                            case eCommand.Orders:
                            case eCommand.OrdersLoad:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = req.id;
                                dat.command = eCommand.OrdersLoadComplete;
                                command.Parameters.AddWithValue("@Stop", SqlDbType.Int).Value = req.Stop;
                                Logger.Info($"QueryData Orders. Cmd:{req.command.ToString()}  reqId: {new Guid(req.requestId).ToString()}  stop: {req.Stop.ToString()}");
                                retVal = new OrderMasterData() { RequestId = new Guid(req.requestId) };
                                break;
                            case eCommand.GenerateManifest:
                                // req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@Link", SqlDbType.BigInt).Value = ((manifestMaster)dat).LINK;
                                dat.command = eCommand.ManifestLoadComplete;

                                Logger.Info($"QueryData GenerateManifest. " +
                                    $"Cmd:{dat.command.ToString()}  " +
                                    $"reqId: {new Guid(dat.requestId).ToString()}" +
                                    $"link: { ((manifestMaster)dat).LINK}");

                                retVal = new ManifestMasterData() { LINK= ((manifestMaster)dat).LINK, RequestId = new Guid(dat.requestId) };
                                break;
                            default:
                                {
                                    Logger.Error($"QueryData Unkonwn Command. " +
                                   $"Cmd:{retVal.Command.ToString()}  " +
                                   $"reqId: {retVal.RequestId.ToString()}");

                                    throw new Exception("Not handled");
                                }
                        }
                        adapter.SelectCommand = command;
                        SqlDataReader reader=null;
                        try
                        {
                            cnn.Open();
                            reader = adapter.SelectCommand.ExecuteReader();
                            Logger.Info($"Query Reader field count: {reader.FieldCount}");
                        }
                        catch (Exception ex) { Logger.Error($"Error Querying SQL Server : {ex.Message}"); }
                        
                        while (reader.Read())
                        {
                            switch (retVal.Command)
                            {
                                case eCommand.Drivers:
                                    drivers drv = (drivers)MyQueryReader_Driver(reader, cb);
                                    drv.requestId = dat.requestId;
                                    drv.command = eCommand.Drivers;
                                    Logger.Info($"QueryData Manifest. Cmd:{drv.command.ToString()}  reqId: {new Guid(drv.requestId).ToString()}  Link: {drv.DriverId}  manId: {drv.LastName}, {drv.FirstName} ");
                                    cb(drv.ToArray());
                                    break;
                                case eCommand.Manifest:
                                case eCommand.GenerateManifest:
                                    manifestMaster micmd = (manifestMaster)myQueryData_GetManifest(reader, cb);
                                    micmd.requestId = dat.requestId;
                                    micmd.command = eCommand.Manifest;
                                    Logger.Info($"QueryData Manifest. Cmd:{micmd.command.ToString()}  reqId: {new Guid(micmd.requestId).ToString()}  Link: {micmd.LINK}  manId: {micmd.id} ");
                                    cb(micmd.ToArray());
                                    break;
                                case eCommand.Trucks:
                                    trucks tcmd = (trucks)MyQueryReader_Trucks(reader, cb);
                                    tcmd.requestId = dat.requestId;
                                    tcmd.command = eCommand.Trucks;
                                    Logger.Info($"QueryData Trucks. Cmd:{tcmd.command.ToString()}  reqId: {new Guid(tcmd.requestId).ToString()}  TRK_CDE: {tcmd.TruckCode}  ManId: {tcmd.ManifestId}");
                                    cb(tcmd.ToArray());
                                    break;
                                case eCommand.Stops:
                                    stops scmd = (stops)MyQueryReader_Stops(reader, cb);
                                    scmd.requestId = dat.requestId;
                                    scmd.command = eCommand.Stops;
                                    Logger.Info($"QueryData Stops. Cmd:{scmd.command.ToString()} reqId: {new Guid(scmd.requestId).ToString()} manId: {scmd.ManifestId}");
                                    cb(scmd.ToArray());
                                    break;
                                case eCommand.Orders:
                                case eCommand.OrdersLoad:
                                    var ocmd = (orders)MyQueryReader_Orders(reader, cb);
                                    ocmd.requestId = dat.requestId;
                                    ocmd.command = eCommand.OrdersLoad;
                                    Logger.Info($"QueryData Orders. Cmd:{ocmd.command.ToString()} reqId: {new Guid(ocmd.requestId).ToString()}");
                                    cb(ocmd.ToArray());
                                    break;
                                default:
                                    throw new Exception("Not handled");
                            }
                        }

                        Logger.Info($"QueryData Completed. Cmd:{dat.command.ToString()} reqId: {new Guid(dat.requestId).ToString()}");
                        cb(dat.ToArray());

                    }
                }
            }

            return retVal;
        }

    }
}
