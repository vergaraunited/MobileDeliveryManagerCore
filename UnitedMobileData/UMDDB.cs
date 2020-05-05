﻿using System;
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
                        Logger.Info($"{SP.ToString()} {mst.ToString()}");
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
                                    manid = (long)retVal.Value;
                                    mst.ManifestId = manid;
                                    yield return mst;
                                }
                            }
                        }
                        break;
                    case SPCmds.INSERTMANIFESTDETAILS:
                        ManifestDetailsData mdd = (ManifestDetailsData)dat;
                        Logger.Info($"{SP.ToString()} {mdd.ToString()}");
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {

                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = CommandType.StoredProcedure;
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
                        Logger.Info($"{SP.ToString()} {omd.ToString()}");
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
                                    command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = omd.Status;
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
                        Logger.Info($"{SP.ToString()} {odd.ToString()}");
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {

                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = odd.ORD_NO;
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.Int).Value = odd.MDL_NO;
                                    command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = odd.DESC;

                                    command.Parameters.AddWithValue("@CLR", SqlDbType.Int).Value = odd.CLR;
                                    command.Parameters.AddWithValue("@WIDTH", SqlDbType.Decimal).Value = odd.WIDTH;
                                    command.Parameters.AddWithValue("@HEIGHT", SqlDbType.Decimal).Value = odd.HEIGHT;
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
                        Logger.Info($"{SP.ToString()} {oop.ToString()}");
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {

                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = oop.ORD_NO;
                                    command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.SmallInt).Value = oop.MDL_CNT;
                                    command.Parameters.AddWithValue("@PAT_POS", SqlDbType.VarBinary).Value = oop.PAT_POS;
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.VarChar).Value = oop.MDL_NO;
                                    command.Parameters.AddWithValue("@OPT_TYPE", SqlDbType.VarChar).Value = oop.OPT_TYPE;
                                    command.Parameters.AddWithValue("@OPT_NUM", SqlDbType.SmallInt).Value = oop.OPT_NUM;
                                    command.Parameters.AddWithValue("@CLR", SqlDbType.VarChar).Value = oop.CLR;
                                    command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = oop.DESC;
                                    // command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.Int).Value = oop.WIN_CNT;
                                    //command.Parameters.AddWithValue("@Status", SqlDbType.VarChar).Value = "LOADED";
                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();

                                    yield return oop;
                                }
                            }
                        }
                        break;
                case SPCmds.COMPLETESTOP:
                        OrderMasterData ocs = (OrderMasterData)dat;
                        Logger.Info($"{SP.ToString()} {ocs.ToString()}");
                        ocs.status = MobileDeliveryGeneral.Definitions.status.Completed;
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = ocs.ORD_NO;
                                    command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = ocs.Status;
                                    var MDL_CNT = command.Parameters.Add("@MDL_CNT", SqlDbType.SmallInt);
                                    MDL_CNT.Direction = ParameterDirection.Output;
                                    var PAT_POS = command.Parameters.Add("@PAT_POS", SqlDbType.VarBinary,1);
                                    PAT_POS.Direction = ParameterDirection.Output;
                                    var MDL_NO = command.Parameters.Add("@MDL_NO", SqlDbType.VarChar, 4);
                                    MDL_NO.Direction = ParameterDirection.Output;
                                    var OPT_TYPE = command.Parameters.Add("@OPT_TYPE", SqlDbType.VarChar, 3);
                                    OPT_TYPE.Direction = ParameterDirection.Output;
                                    var OPT_NUM = command.Parameters.Add("@OPT_NUM", SqlDbType.SmallInt);
                                    OPT_NUM.Direction = ParameterDirection.Output;
                                    var CLR = command.Parameters.Add("@CLR", SqlDbType.VarChar, 4);
                                    CLR.Direction = ParameterDirection.Output;
                                    var DESC = command.Parameters.Add("@DESC", SqlDbType.VarChar, 60);
                                    DESC.Direction = ParameterDirection.Output;
                                    var WIDTH = command.Parameters.Add("@WIDTH", SqlDbType.Decimal);
                                    WIDTH.Direction = ParameterDirection.Output;
                                    var HEIGHT = command.Parameters.Add("@HEIGHT", SqlDbType.Decimal);
                                    HEIGHT.Direction = ParameterDirection.Output;
                                    var ScanTime = command.Parameters.Add("@ScanTime", SqlDbType.DateTime);
                                    ScanTime.Direction = ParameterDirection.Output;

                                    adapter.InsertCommand = command;
                                    adapter.InsertCommand.ExecuteNonQuery();
                                    try
                                    {
                                        //if (MDL_CNT.Value is DBNull)
                                        ocs.MDL_CNT = (short)MDL_CNT.Value;
                                        //if (PAT_POS.Value is DBNull)
                                        byte[] btar = new byte[1];
                                        btar = (byte[])PAT_POS.Value;
                                        ocs.PAT_POS = btar[0];
                                        //if (MDL_NO.Value is DBNull)
                                        ocs.MDL_NO = (string)MDL_NO.Value;
                                        //if (OPT_TYPE.Value is DBNull)
                                        ocs.OPT_TYPE = (string)OPT_TYPE.Value;
                                        //if (OPT_NUM.Value is DBNull)
                                        ocs.OPT_NUM = (short)OPT_NUM.Value;
                                        //if (CLR.Value is DBNull)
                                        ocs.CLR = (string)CLR.Value;
                                        //if (DESC.Value is DBNull)
                                        ocs.DESC = (string)DESC.Value;
                                        //if (WIDTH.Value is DBNull)
                                        ocs.WIDTH = (decimal)WIDTH.Value;
                                        //if (HEIGHT.Value is DBNull)
                                        ocs.HEIGHT = (decimal)HEIGHT.Value;
                                        if (!(ScanTime.Value is DBNull))
                                            ocs.SCAN_DATE_TIME = (DateTime)ScanTime.Value;
                                    }
                                    catch (Exception ex) {
                                        Logger.Error($"COMPLETESTOP Exception {ex.Message}");
                                        }
                                    yield return ocs;
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

        //public IMDMMessage InsertDataSlow(SPCmds SP, IMDMMessage dat)
        //{
        //    IMDMMessage ret = null;
        //    if (dat != null)
        //    {

        //        switch (SP)
        //        {
        //            case SPCmds.INSERTMANIFEST:
        //                ManifestMasterData mst = (ManifestMasterData)dat;
        //                Logger.Info($"{SP.ToString()} {mst.ToString()}");
        //                using (cnn = NewConnection())
        //                {
        //                    using (var adapter = new SqlDataAdapter())
        //                    {
        //                        long manid;
        //                        using (var command = new SqlCommand(SP.ToString(), cnn))
        //                        {
        //                            command.CommandType = CommandType.StoredProcedure;
        //                            command.Parameters.AddWithValue("@DriverId", SqlDbType.Int).Value = 9;  //use mst.UserId query the Driver table
        //                            command.Parameters.AddWithValue("@TRK_CDE", SqlDbType.VarChar).Value = mst.TRK_CDE;
        //                            command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = mst.SHIP_DTE.Date;
        //                            command.Parameters.AddWithValue("@DESC$", SqlDbType.VarChar).Value = mst.Desc;
        //                            command.Parameters.AddWithValue("@LINK", SqlDbType.BigInt).Value = mst.LINK;
        //                            command.Parameters.AddWithValue("@NOTES", SqlDbType.VarChar).Value = mst.NOTES;
        //                            var retVal = command.Parameters.Add("@ManifestId", SqlDbType.BigInt);
        //                            retVal.Direction = ParameterDirection.Output;

        //                            adapter.InsertCommand = command;
        //                            adapter.InsertCommand.ExecuteNonQuery();
        //                            manid = (long)retVal.Value;
        //                            mst.ManifestId = manid;
        //                            ret = mst;
        //                        }
        //                    }
        //                }
        //                break;
        //            case SPCmds.INSERTMANIFESTDETAILS:
        //                ManifestDetailsData mdd = (ManifestDetailsData)dat;
        //                //SQL = @"";
        //                Logger.Info($"{SP.ToString()} {mdd.ToString()}");
        //                using (cnn = NewConnection())
        //                {
        //                    using (var adapter = new SqlDataAdapter())
        //                    {

        //                        using (var command = new SqlCommand(SP.ToString(), cnn))
        //                        {
        //                            command.CommandType = System.Data.CommandType.StoredProcedure;
        //                            command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = mdd.ManId;
        //                            command.Parameters.AddWithValue("@DSP_SEQ", SqlDbType.Int).Value = mdd.DSP_SEQ;
        //                            command.Parameters.AddWithValue("@DLR_NO", SqlDbType.BigInt).Value = mdd.DLR_NO;

        //                            adapter.InsertCommand = command;
        //                            adapter.InsertCommand.ExecuteNonQuery();

        //                            ret = mdd;
        //                        }
        //                    }
        //                }
        //                break;
        //            case SPCmds.INSERTORDER:
        //                OrderMasterData omd = (OrderMasterData)dat;
        //                //SQL = @"";
        //                Logger.Info($"{SP.ToString()} {omd.ToString()}");
        //                using (cnn = NewConnection())
        //                {
        //                    using (var adapter = new SqlDataAdapter())
        //                    {

        //                        using (var command = new SqlCommand(SP.ToString(), cnn))
        //                        {
        //                            command.CommandType = System.Data.CommandType.StoredProcedure;

        //                            command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = omd.ORD_NO;
        //                            command.Parameters.AddWithValue("@DLR_NO", SqlDbType.BigInt).Value = omd.DLR_NO;
        //                            command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = omd.SHP_DTE.Date;
        //                            command.Parameters.AddWithValue("@DLR_NME", SqlDbType.VarChar).Value = omd.DLR_NME;
        //                            command.Parameters.AddWithValue("@SHP_NME", SqlDbType.VarChar).Value = omd.SHP_NME;
        //                            command.Parameters.AddWithValue("@SHP_ADDR", SqlDbType.VarChar).Value = omd.SHP_ADDR;
        //                            command.Parameters.AddWithValue("@SHP_ADDR2", SqlDbType.VarChar).Value = omd.SHP_ADDR2;
        //                            command.Parameters.AddWithValue("@SHP_TEL", SqlDbType.VarChar).Value = omd.SHP_TEL;
        //                            command.Parameters.AddWithValue("@SHP_ZIP", SqlDbType.VarChar).Value = omd.SHP_ZIP;
        //                            command.Parameters.AddWithValue("@CUS_NME", SqlDbType.VarChar).Value = omd.CUS_NME;
        //                            command.Parameters.AddWithValue("@RTE_CDE", SqlDbType.VarChar).Value = omd.RTE_CDE;
        //                            command.Parameters.AddWithValue("@SHP_QTY", SqlDbType.Int).Value = omd.SHP_QTY;
        //                            command.Parameters.AddWithValue("@MAN_ID", SqlDbType.Int).Value = omd.ManId;
        //                            var retVal = command.Parameters.Add("@CustomerIdOut", SqlDbType.Int);
        //                            retVal.Direction = ParameterDirection.Output;

        //                            adapter.InsertCommand = command;
        //                            adapter.InsertCommand.ExecuteNonQuery();

        //                            ret = omd;
        //                        }
        //                    }
        //                }
        //                break;
        //            case SPCmds.INSERTORDERDETAILS:
        //                OrderDetailsData odd = (OrderDetailsData)dat;
        //                //SQL = @"";
        //                Logger.Info($"{SP.ToString()} {odd.ToString()}");
        //                using (cnn = NewConnection())
        //                {
        //                    using (var adapter = new SqlDataAdapter())
        //                    {

        //                        using (var command = new SqlCommand(SP.ToString(), cnn))
        //                        {
        //                            command.CommandType = System.Data.CommandType.StoredProcedure;
        //                            command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = odd.ORD_NO;
        //                            command.Parameters.AddWithValue("@MDL_NO", SqlDbType.Int).Value = odd.MDL_NO;
        //                            command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = odd.DESC;

        //                            command.Parameters.AddWithValue("@CLR", SqlDbType.Int).Value = odd.CLR;
        //                            command.Parameters.AddWithValue("@WIDTH", SqlDbType.BigInt).Value = odd.WIDTH;
        //                            command.Parameters.AddWithValue("@HEIGHT", SqlDbType.BigInt).Value = odd.HEIGHT;
        //                            command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.Int).Value = odd.MDL_CNT;
        //                            command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.Int).Value = odd.WIN_CNT;
        //                            command.Parameters.AddWithValue("@Status", SqlDbType.VarChar).Value = "LOADED";
        //                            adapter.InsertCommand = command;
        //                            adapter.InsertCommand.ExecuteNonQuery();
        //                            ret = odd;
        //                        }
        //                    }
        //                }
        //                break;
        //            case SPCmds.INSERTORDEROPTIONS:
        //                OrderOptionsData oop = (OrderOptionsData)dat;
        //                //SQL = @"";
        //                Logger.Info($"{SP.ToString()} {oop.ToString()}");
        //                using (cnn = NewConnection())
        //                {
        //                    using (var adapter = new SqlDataAdapter())
        //                    {
        //                        try
        //                        {
        //                            using (var command = new SqlCommand(SP.ToString(), cnn))
        //                            {
        //                                command.CommandType = System.Data.CommandType.StoredProcedure;
        //                                command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = oop.ORD_NO;
        //                                command.Parameters.AddWithValue("@MDL_NO", SqlDbType.VarChar).Value = oop.MDL_NO;
        //                                command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = oop.DESC;

        //                                command.Parameters.AddWithValue("@CLR", SqlDbType.Int).Value = oop.CLR;
        //                                command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.Int).Value = oop.MDL_CNT;
        //                                // command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.Int).Value = oop.WIN_CNT;
        //                                command.Parameters.AddWithValue("@PAT_POS", SqlDbType.Int).Value = oop.PAT_POS;
        //                                command.Parameters.AddWithValue("@OPT_NUM", SqlDbType.SmallInt).Value = oop.OPT_NUM;
        //                                command.Parameters.AddWithValue("@OPT_TYPE", SqlDbType.Int).Value = oop.OPT_TYPE;
        //                                adapter.InsertCommand = command;
        //                                adapter.InsertCommand.ExecuteNonQuery();

        //                                ret = oop;
        //                            }
        //                        }
        //                        catch (Exception ex) { Logger.Error($"{SP.ToString()} Error: {ex.Message}"); }
        //                    }
        //                }
        //                break;
        //            default:
        //                throw new Exception("UM SQL Server Stored Procedure Not Found.");
        //        }
        //        //}
        //        //catch (Exception ex) { Logger.Info($"Insert Data Error {ex.Message}"); }                  
        //    }
        //    return ret;
        //}

        public bool InsertData(string sql, IMDMMessage msg) { throw new Exception("not impl"); }
        IMDMMessage MyQueryReader_OrderMaster(DbDataReader reader)
        {
            ManifestMasterData md = new ManifestMasterData();
            try
            {
                string sValue1 = "";
                int sValue2 = 0;

                sValue1 = reader.GetString(reader.GetOrdinal("WINSYSLITEORDER"));
                sValue2 = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                if (sValue1 != "0")
                {
                    md.Desc = sValue1;
                    md.ManifestId = sValue2;
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
                Logger.Info($"MyQueryReader_Driver: {md.ToString()}");
                //cb(md.ToArray());
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
                Logger.Info($"MyQueryReader_Trucks: {trk.ToString()}");
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
                    st.CustomerName = "";
                else
                    st.CustomerName = reader.GetString(reader.GetOrdinal("CUS_NME"));

                if (reader.IsDBNull(reader.GetOrdinal("DLR_NME")))
                    st.DealerName = "";
                else
                    st.DealerName = reader.GetString(reader.GetOrdinal("DLR_NME"));

                st.Address = reader.GetString(reader.GetOrdinal("SHP_ADDR"));
                st.PhoneNumber = reader.GetString(reader.GetOrdinal("SHP_TEL"));

                st.Description = reader.GetString(reader.GetOrdinal("DESC$"));
                st.Notes = reader.GetString(reader.GetOrdinal("NOTES"));
                st.TRK_CDE = reader.GetString(reader.GetOrdinal("TRK_CDE"));
                st.CustomerId = reader.GetInt32(reader.GetOrdinal("CustomerId"));
                //st.BillComplete = reader.GetBoolean(reader.GetOrdinal("BillComplete"));
                Logger.Info($"MyQueryReader_Stops: {st.ToString()}");
            }
            catch (Exception ex) { }
            return st;
        }
        public static isaCommand MyQueryReader_Orders(SqlDataReader reader, Func<byte[], Task> cb)
        {
            orders ord = new orders();
            try
            {
                //ManifestId, s.DSP_SEQ, om.CustomerId, od.WIN_CNT, od.DESC$, oo.MDL_CNT, oo.MODEL, oo.MDL_NO, oo.CLR, oo.DESC$
                ord.requestId = Guid.Empty.ToByteArray();
                ord.ManifestId = reader.GetInt64(reader.GetOrdinal("ManifestId"));
                ord.DSP_SEQ = reader.GetInt32(reader.GetOrdinal("DSP_SEQ"));
                ord.CustomerId = reader.GetInt32(reader.GetOrdinal("CustomerId"));
                // ord.DLR_NO = reader.GetInt64(reader.GetOrdinal("DLR_NO"));
                ord.ORD_NO = reader.GetInt64(reader.GetOrdinal("ORD_NO"));
                ord.CLR = reader.GetString(reader.GetOrdinal("CLR"));
                ord.MDL_CNT = reader.GetInt16(reader.GetOrdinal("MDL_CNT"));
                ord.MDL_NO = reader.GetString(reader.GetOrdinal("MDL_NO"));
                ord.WIN_CNT = reader.GetInt32(reader.GetOrdinal("WIN_CNT"));
                ord.DESC = reader.GetString(reader.GetOrdinal("DESC$"));
                // ord.Status = reader.GetBytes(reader.GetOrdinal("Status"), 0, ord.Status, 0, 1);
                
                ord.WIDTH = reader.GetDecimal(reader.GetOrdinal("WIDTH"));
                ord.HEIGHT= reader.GetDecimal(reader.GetOrdinal("HEIGHT"));
                ord.Status = reader.GetByte(reader.GetOrdinal("Status"));
                //Logger.Info($"MyQueryReader_Orders: {ord.ToString()}");
            }
            catch (Exception ex) { }
            return ord;
        }
        public static isaCommand MyQueryReader_OrderDetails(SqlDataReader reader, Func<byte[], Task> cb)
        {
            orderDetails od = new orderDetails();
            try
            {
                od.requestId = Guid.Empty.ToByteArray();
                od.ORD_NO = reader.GetInt64(reader.GetOrdinal("ORD_NO"));
                od.MDL_CNT = reader.GetInt16(reader.GetOrdinal("MDL_CNT"));
                od.PAT_POS = reader.GetByte(reader.GetOrdinal("PAT_POS"));
                od.MDL_NO = reader.GetString(reader.GetOrdinal("MDL_NO"));
                od.OPT_TYPE = reader.GetString(reader.GetOrdinal("OPT_TYPE"));
                od.OPT_NUM = reader.GetInt16(reader.GetOrdinal("OPT_NUM"));
                od.CLR = reader.GetString(reader.GetOrdinal("CLR"));
                od.DESC = reader.GetString(reader.GetOrdinal("DESCOO"));
                od.WIDTH = reader.GetDecimal(reader.GetOrdinal("WIDTH"));
                od.HEIGHT = reader.GetDecimal(reader.GetOrdinal("HEIGHT"));
                od.ScanTime = reader.GetDateTime(reader.GetOrdinal("ScanDateTime"));

                Logger.Info($"MyQueryReader_Orders: {od.ToString()}");
            }
            catch (Exception ex) { }
            return od;
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
                mm.DESC = reader.GetString(reader.GetOrdinal("DESC$"));
                mm.NOTES = reader.GetString(reader.GetOrdinal("NOTES"));
                mm.LINK = reader.GetInt64(reader.GetOrdinal("LINK"));

                //mm.command = eCommand.ManifestLoadComplete;
                Logger.Info($"myQueryData_GetManifest: {mm.ToString()}");
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
                case eCommand.OrderDetails:
                    SP = SPCmds.GETORDERDETAILS;
                    break;
                case eCommand.GenerateManifest:
                case eCommand.CheckManifest:
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
                                retVal = new DriverData() { RequestId = NewGuid(req.requestId) };
                                Logger.Info($"QueryData Drivers. {retVal.ToString()}");
                                break;
                            case eCommand.Manifest:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@DriverId", SqlDbType.Int).Value = req.id;
                                retVal = new ManifestMasterData() { RequestId = NewGuid(req.requestId) };
                                Logger.Info($"QueryData Manifest. {req.ToString()} ");
                                dat.command = eCommand.ManifestLoadComplete;
                                break;

                            case eCommand.Trucks:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd");
                                dat.command = eCommand.TrucksLoadComplete;
                                Logger.Info($"QueryData Trucks. {req.ToString()}");

                                retVal = new TruckData() { Command=eCommand.Trucks, RequestId = NewGuid(req.requestId) };
                                break;

                            case eCommand.Stops:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = req.id;
                                dat.command = eCommand.StopsLoadComplete;
                                Logger.Info($"QueryData Stops. Cmd:{req.command.ToString()}  reqId: {NewGuid(req.requestId).ToString()}  manId: {req.id}");
                                retVal = new StopData() { RequestId = NewGuid(req.requestId) };
                                break;

                            case eCommand.Orders:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = req.id;
                                command.Parameters.AddWithValue("@Stop", SqlDbType.Int).Value = req.Stop;
                                dat.command = eCommand.OrdersLoadComplete;
                                Logger.Info($"QueryData Orders. {req.ToString()}");
                                retVal = new OrderData() { RequestId = NewGuid(req.requestId) };
                                break;

                            case eCommand.OrderDetails:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = req.id;
                                dat.command = eCommand.OrderDetailsComplete;
                                Logger.Info($"QueryData Order Details. {req.ToString()}");
                                retVal = new OrderDetailsData() { RequestId = NewGuid(req.requestId) };
                                break;

                            case eCommand.OrdersLoad:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ManifestId", SqlDbType.Int).Value = req.id;
                                command.Parameters.AddWithValue("@Stop", SqlDbType.Int).Value = req.Stop;
                                dat.command = eCommand.OrdersLoadComplete;
                                retVal = new OrderData() { Command=eCommand.OrdersLoad, RequestId = NewGuid(req.requestId) };
                                Logger.Info($"QueryData OrdersLoad. {req.ToString()}");
                                break;

                            case eCommand.CheckManifest:
                                command.Parameters.AddWithValue("@Link", SqlDbType.BigInt).Value = ((manifestMaster)dat).LINK;
                                
                                Logger.Info($"QueryData {Enum.GetName(typeof(eCommand), dat.command)}  {dat.ToString()}  ");
                                retVal = new ManifestMasterData((manifestMaster)dat, 0);
                                dat.command = eCommand.CheckManifestComplete;
                                break;
                            case eCommand.GenerateManifest:
                                command.Parameters.AddWithValue("@Link", SqlDbType.BigInt).Value = ((manifestMaster)dat).LINK;
                                dat.command = eCommand.ManifestLoadComplete;
                                Logger.Info($"QueryData {Enum.GetName(typeof(eCommand), dat.command)} Manifest: {dat.ToString()}  ");
                                retVal = new ManifestMasterData((manifestMaster)dat, 0);
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
                                    Logger.Info($"QueryData Drivers. Cmd:{drv.ToString()} ");
                                    cb(drv.ToArray());
                                    break;
                                case eCommand.Manifest:
                                case eCommand.GenerateManifest:                                
                                    manifestMaster micmd = (manifestMaster)myQueryData_GetManifest(reader, cb);
                                    micmd.requestId = dat.requestId;
                                    micmd.command = eCommand.Manifest;
                                    Logger.Info($"QueryData Manifest/Generate Manifest Complete: {micmd.ToString()}");
                                    cb(micmd.ToArray());
                                    break;
                                case eCommand.CheckManifest:
                                    manifestMaster mcm = (manifestMaster)myQueryData_GetManifest(reader, cb);
                                    mcm.requestId = dat.requestId;
                                    mcm.command = eCommand.CheckManifest;
                                    Logger.Info($"QueryData CheckManifest: {mcm.ToString()}");
                                    cb(mcm.ToArray());
                                    break;
                                case eCommand.Trucks:
                                    trucks tcmd = (trucks)MyQueryReader_Trucks(reader, cb);
                                    tcmd.requestId = dat.requestId;
                                    tcmd.command = eCommand.Trucks;
                                    Logger.Info($"QueryData Trucks. {tcmd.ToString()}");
                                    cb(tcmd.ToArray());
                                    break;
                                case eCommand.Stops:
                                    stops scmd = (stops)MyQueryReader_Stops(reader, cb);
                                    scmd.requestId = dat.requestId;
                                    scmd.command = eCommand.Stops;
                                    Logger.Info($"QueryData Stops. {scmd.ToString()}");
                                    cb(scmd.ToArray());
                                    break;
                                case eCommand.Orders:
                                    var ocmd = (orders)MyQueryReader_Orders(reader, cb);
                                    ocmd.requestId = dat.requestId;
                                    ocmd.command = eCommand.Orders;
                                    Logger.Info($"QueryData Orders. {ocmd.ToString()}");
                                    cb(ocmd.ToArray());
                                    break;

                                case eCommand.OrdersLoad:
                                    var olcmd = (orders)MyQueryReader_Orders(reader, cb);
                                    olcmd.requestId = dat.requestId;
                                    olcmd.command = eCommand.OrdersLoad;
                                    Logger.Info($"QueryData OrdersLoad. {olcmd.ToString()}");
                                    cb(olcmd.ToArray());
                                    break;

                                case eCommand.OrderDetails:
                                    var odcmd = (orders)MyQueryReader_OrderDetails(reader, cb);
                                    odcmd.requestId = dat.requestId;
                                    odcmd.command = eCommand.OrderDetails;
                                    Logger.Info($"QueryData OrderDetails. {odcmd.ToString()}");
                                    cb(odcmd.ToArray());
                                    break;

                                default:
                                    throw new Exception("Query Data Type Not handled");
                            }
                        }

                        Logger.Info($"QueryData Completed: {dat.ToString()}");
                        cb(dat.ToArray());

                    }
                }
            }

            return retVal;
        }

    }
}
