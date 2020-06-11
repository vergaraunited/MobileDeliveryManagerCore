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
                                    command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.BigInt).Value = (long)mst.SHIP_DTE.FromGregorianToJulian();
                                    command.Parameters.AddWithValue("@DESC$", SqlDbType.VarChar).Value = mst.Desc;
                                    command.Parameters.AddWithValue("@LINK", SqlDbType.BigInt).Value = mst.LINK;
                                    command.Parameters.AddWithValue("@NOTES", SqlDbType.VarChar).Value = mst.NOTES;
                                    var retVal = command.Parameters.Add("@ManifestId", SqlDbType.BigInt);
                                    retVal.Direction = ParameterDirection.Output;

                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();
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
                                    command.Parameters.AddWithValue("@DSP_SEQ", SqlDbType.Int).Value = mdd.DSP_SEQ;
                                    command.Parameters.AddWithValue("@DLR_NO", SqlDbType.BigInt).Value = mdd.DLR_NO;
                                    command.Parameters.AddWithValue("@SHP_NME", SqlDbType.VarChar).Value = mdd.SHP_NME;
                                    command.Parameters.AddWithValue("@SHP_ADDR", SqlDbType.VarChar).Value = mdd.SHP_ADDR;
                                    command.Parameters.AddWithValue("@SHP_ADDR2", SqlDbType.VarChar).Value = mdd.SHP_ADDR2;
                                    command.Parameters.AddWithValue("@SHP_CSZ", SqlDbType.VarChar).Value = mdd.SHP_CSZ;
                                    command.Parameters.AddWithValue("@SHP_TEL", SqlDbType.VarChar).Value = mdd.SHP_TEL;
                                    command.Parameters.AddWithValue("@DIR_1", SqlDbType.VarChar).Value = mdd.DIR_1;
                                    command.Parameters.AddWithValue("@DIR_2", SqlDbType.VarChar).Value = mdd.DIR_2;
                                    command.Parameters.AddWithValue("@DIR_3", SqlDbType.VarChar).Value = mdd.DIR_3;
                                    command.Parameters.AddWithValue("@DIR_4", SqlDbType.VarChar).Value = mdd.DIR_4;
                                    command.Parameters.AddWithValue("@EXTRA_STOP", SqlDbType.SmallInt).Value = mdd.EXTRA_STOP;
                                    command.Parameters.AddWithValue("@UNITSONTRUCK", SqlDbType.SmallInt).Value = mdd.UNITSONTRUCK;
                                    command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = OrderStatus.New;
                                    command.Parameters.AddWithValue("@ManifestId", SqlDbType.BigInt).Value = mdd.ManId;

                                    var retCustId = command.Parameters.Add("@CustomerIdOut", SqlDbType.Int);
                                    retCustId.Direction = ParameterDirection.Output;
                                    //if (!(retCustId.Value is DBNull))
                                    //    cmdcnt = (int)retCustId.Value;

                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();

                                    yield return mdd;
                                }
                            }
                        }
                        break;
                    case SPCmds.INSERTSCANFILE:
                        ScanFileData scf = (ScanFileData)dat;
                        Logger.Info($"{SP.ToString()} {scf.ToString()}");
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    command.CommandType = CommandType.StoredProcedure;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.Int).Value = scf.ORD_NO;
                                    command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.SmallInt).Value = scf.MDL_CNT;
                                    command.Parameters.AddWithValue("@PAT_POS", SqlDbType.SmallInt).Value = scf.PAT_POS;
                                    command.Parameters.AddWithValue("@LOT_NO", SqlDbType.Int).Value = scf.LOT_NO;
                                    command.Parameters.AddWithValue("@BIN_NO", SqlDbType.SmallInt).Value = scf.BIN_NO;
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.VarChar).Value = scf.MDL_NO;
                                    command.Parameters.AddWithValue("@INVOICE_NO", SqlDbType.Int).Value = scf.INVOICE_NO;
                                    command.Parameters.AddWithValue("@DSP_SEQ", SqlDbType.TinyInt).Value = scf.DSP_SEQ;
                                    command.Parameters.AddWithValue("@TRK_CDE", SqlDbType.VarChar).Value = scf.TRK_CDE;
                                    command.Parameters.AddWithValue("@TRK_DTE", SqlDbType.Int).Value = scf.TRK_DTE;
                                    command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.SmallInt).Value = scf.WIN_CNT;
                                    command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Int).Value = scf.SHP_DTE;
                                    command.Parameters.AddWithValue("@SHP_TME", SqlDbType.Int).Value = scf.SHP_TME;
                                    command.Parameters.AddWithValue("@SHP_BY", SqlDbType.VarChar).Value = scf.SHP_BY;
                                    command.Parameters.AddWithValue("@LOCATION", SqlDbType.VarChar).Value = scf.LOCATION;
                                    command.Parameters.AddWithValue("@REASON", SqlDbType.VarChar).Value = scf.REASON;
                                    command.Parameters.AddWithValue("@MAN_ID", SqlDbType.BigInt).Value = scf.MAN_ID;

                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();

                                    yield return scf;
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
                                    command.Parameters.AddWithValue("@MAN_ID", SqlDbType.BigInt).Value = omd.ManId;
                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = omd.ORD_NO;
                                    command.Parameters.AddWithValue("@DLR_NO", SqlDbType.BigInt).Value = omd.DLR_NO;
                                    command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = omd.Status;
                                    command.Parameters.AddWithValue("@DLR_PO", SqlDbType.VarChar).Value = omd.DLR_PO;
                                    command.Parameters.AddWithValue("@ORD_DTE", SqlDbType.Int).Value = omd.ORD_DTE;
                                    command.Parameters.AddWithValue("@SHIP_DTE", SqlDbType.Int).Value = omd.SHIP_DTE;
                                    command.Parameters.AddWithValue("@CMNT1", SqlDbType.VarChar).Value = omd.CMNT1;
                                    command.Parameters.AddWithValue("@CMNT2", SqlDbType.VarChar).Value = omd.CMNT2;
                                    command.Parameters.AddWithValue("@DLR_NME", SqlDbType.VarChar).Value = omd.DLR_NME;
                                    command.Parameters.AddWithValue("@DLR_ADDR", SqlDbType.VarChar).Value = omd.DLR_ADDR;
                                    command.Parameters.AddWithValue("@DLR_ADDR2", SqlDbType.VarChar).Value = omd.DLR_ADDR2;
                                    command.Parameters.AddWithValue("@SHP_NME", SqlDbType.VarChar).Value = omd.SHP_NME;
                                    command.Parameters.AddWithValue("@SHP_ADDR", SqlDbType.VarChar).Value = omd.SHP_ADDR;
                                    command.Parameters.AddWithValue("@SHP_ADDR2", SqlDbType.VarChar).Value = omd.SHP_ADDR2;
                                    command.Parameters.AddWithValue("@SHP_CSZ", SqlDbType.VarChar).Value = omd.SHP_CSZ;
                                    command.Parameters.AddWithValue("@SHP_TEL", SqlDbType.VarChar).Value = omd.SHP_TEL;
                                    command.Parameters.AddWithValue("@SHP_CT", SqlDbType.SmallInt).Value = omd.SHP_CT;
                                    command.Parameters.AddWithValue("@SHP_ZIP", SqlDbType.VarChar).Value = omd.SHP_ZIP;
                                    command.Parameters.AddWithValue("@CUS_NME", SqlDbType.VarChar).Value = omd.CUS_NME;
                                    command.Parameters.AddWithValue("@CUS_ADDR", SqlDbType.VarChar).Value = omd.CUS_ADDR;
                                    command.Parameters.AddWithValue("@CUS_CSZ", SqlDbType.VarChar).Value = omd.CUS_CSZ;
                                    command.Parameters.AddWithValue("@CUS_TEL", SqlDbType.VarChar).Value = omd.CUS_TEL;
                                    command.Parameters.AddWithValue("@RTE_CDE", SqlDbType.VarChar).Value = omd.RTE_CDE;
                                    command.Parameters.AddWithValue("@ENT_BY", SqlDbType.VarChar).Value = omd.ENT_BY;
                                    command.Parameters.AddWithValue("@WIN_QTY", SqlDbType.SmallInt).Value = omd.WIN_QTY;
                                    command.Parameters.AddWithValue("@STK_QTY", SqlDbType.SmallInt).Value = omd.STK_QTY;
                                    command.Parameters.AddWithValue("@CMP_QTY", SqlDbType.SmallInt).Value = omd.CMP_QTY;
                                    command.Parameters.AddWithValue("@SHP_QTY", SqlDbType.SmallInt).Value = omd.SHP_QTY;
                                    command.Parameters.AddWithValue("@SHP_AMT", SqlDbType.Decimal).Value = omd.SHP_AMT;
                                    command.Parameters.AddWithValue("@MISC_TEXT", SqlDbType.VarChar).Value = omd.MISC_TEXT;

                                    var retCustId = command.Parameters.Add("@CustomerIdOut", SqlDbType.Int);
                                    retCustId.Direction = ParameterDirection.Output;
                                    var retDSP_SEQ = command.Parameters.Add("@StopOut", SqlDbType.Int);
                                    retDSP_SEQ.Direction = ParameterDirection.Output;
                                    var retORD_NO = command.Parameters.Add("@OrdNoOut", SqlDbType.BigInt);
                                    retORD_NO.Direction = ParameterDirection.Output;

                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();

                                    if (!(retCustId.Value is DBNull))
                                        cmdcnt = (int)retCustId.Value;
                                    if (!(retDSP_SEQ.Value is DBNull))
                                        cmdcnt = (int)retDSP_SEQ.Value;
                                    //if (!(retORD_NO.Value is DBNull))
                                    //    omd.ORD_NO = (int)retORD_NO.Value;

                                    yield return omd;

                                    //foreach( var scnfle in omd.scanFileData)
                                    //{
                                    //    foreach (var id in InsertData(SPCmds.INSERTSCANFILE, scnfle))
                                    //        Logger.Debug($"InsertScanFile: {scnfle.ToString()}");
                                    //}
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
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.VarChar).Value = odd.MDL_NO;
                                    command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = odd.DESC;
                                    command.Parameters.AddWithValue("@CLR", SqlDbType.VarChar).Value = odd.CLR;
                                    command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.SmallInt).Value = odd.MDL_CNT;
                                    command.Parameters.AddWithValue("@OPT_NUM", SqlDbType.SmallInt).Value = odd.OPT_NUM;
                                    command.Parameters.AddWithValue("@OPT_TYPE", SqlDbType.VarChar).Value = odd.OPT_TYPE;
                                    command.Parameters.AddWithValue("@PAT_POS", SqlDbType.TinyInt).Value = odd.PAT_POS;
                                    command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = odd.Status;
                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();
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



                                    command.Parameters.AddWithValue("@ORD_NO", SqlDbType.Int).Value = oop.ORD_NO;
                                    command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.SmallInt).Value = oop.MDL_CNT;
                                    command.Parameters.AddWithValue("@PAT_POS", SqlDbType.TinyInt).Value = oop.PAT_POS;
                                    command.Parameters.AddWithValue("@MDL_NO", SqlDbType.VarChar).Value = oop.MDL_NO;
                                    command.Parameters.AddWithValue("@OPT_TYPE", SqlDbType.VarChar).Value = oop.OPT_TYPE;
                                    command.Parameters.AddWithValue("@OPT_NUM", SqlDbType.SmallInt).Value = oop.OPT_NUM;
                                    command.Parameters.AddWithValue("@STOCK_ID", SqlDbType.VarChar).Value = oop.STOCK_ID;
                                    //command.Parameters.AddWithValue("@STOCK_CONFIGURATOR", SqlDbType.TinyInt).Value = oop.STOCK_CONFIGURATOR;
                                    command.Parameters.AddWithValue("@CALL_SIZE", SqlDbType.VarChar).Value = oop.CALL_SIZE;
                                    command.Parameters.AddWithValue("@CLR", SqlDbType.VarChar).Value = oop.CLR;
                                    command.Parameters.AddWithValue("@DESC$", SqlDbType.VarChar).Value = oop.DESC;
                                    command.Parameters.AddWithValue("@QTY", SqlDbType.SmallInt).Value = oop.QTY;
                                    //command.Parameters.AddWithValue("@CMP_QTY", SqlDbType.SmallInt).Value = oop.CMP_QTY;
                                    //command.Parameters.AddWithValue("@INV_QTY", SqlDbType.SmallInt).Value = oop.INV_QTY;
                                    //command.Parameters.AddWithValue("@SHP_QTY", SqlDbType.SmallInt).Value = oop.SHP_QTY;
                                    //command.Parameters.AddWithValue("@OPT_SOURCE", SqlDbType.VarChar).Value = oop.OPT_SOURCE;
                                    command.Parameters.AddWithValue("@PAT_ID", SqlDbType.SmallInt).Value = oop.PAT_ID;
                                    command.Parameters.AddWithValue("@WIDTH", SqlDbType.Decimal).Value = oop.WIDTH;
                                    command.Parameters.AddWithValue("@HEIGHT", SqlDbType.Decimal).Value = oop.HEIGHT;
                                    command.Parameters.AddWithValue("@COM", SqlDbType.Decimal).Value = oop.COM;
                                    command.Parameters.AddWithValue("@PRICE", SqlDbType.Decimal).Value = oop.PRICE;
                                    command.Parameters.AddWithValue("@DIS_PER", SqlDbType.Decimal).Value = oop.DIS_PER;
                                    command.Parameters.AddWithValue("@DIS_AMT", SqlDbType.Decimal).Value = oop.DIS_AMT;
                                    command.Parameters.AddWithValue("@DIS_UNT", SqlDbType.Decimal).Value = oop.DIS_UNT;
                                    command.Parameters.AddWithValue("@NET_AMT", SqlDbType.Decimal).Value = oop.NET_AMT;
                                    //command.Parameters.AddWithValue("@WTY", SqlDbType.VarChar).Value = oop.WTY;
                                    command.Parameters.AddWithValue("@CMT1", SqlDbType.VarChar).Value = oop.CMT1;
                                    command.Parameters.AddWithValue("@CMT2", SqlDbType.VarChar).Value = oop.CMT2;
                                    command.Parameters.AddWithValue("@NOTES", SqlDbType.VarChar).Value = oop.NOTES;
                                    command.Parameters.AddWithValue("@SHIPPING", SqlDbType.VarChar).Value = oop.SHIPPING;
                                    command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.SmallInt).Value = oop.SHP_DTE;
                                    command.Parameters.AddWithValue("@TRUCK", SqlDbType.VarChar).Value = oop.TRUCK;
                                    command.Parameters.AddWithValue("@SHP_SEQUENCE", SqlDbType.SmallInt).Value = oop.SHP_SEQUENCE;
                                    command.Parameters.AddWithValue("@TYPE", SqlDbType.VarChar).Value = oop.TYPE;
                                    //command.Parameters.AddWithValue("@REMAKE", SqlDbType.Int32).Value = omd.REMAKE;
                                    //command.Parameters.AddWithValue("@LINK_ORDER", SqlDbType.Int32).Value = omd.LINK_ORDER;
                                    //command.Parameters.AddWithValue("@LINK_MDL_CNT", SqlDbType.Int16).Value = omd.LINK_MDL_CNT;
                                    //command.Parameters.AddWithValue("@LINK_PAT_POS", SqlDbType.Byte).Value = omd.LINK_PAT_POS;
                                    //command.Parameters.AddWithValue("@LINK_WIN_CNT", SqlDbType.Int16).Value = omd.LINK_WIN_CNT;
                                    command.Parameters.AddWithValue("@PROD_DESC", SqlDbType.VarChar).Value = oop.PROD_DESC;
                                    command.Parameters.AddWithValue("@EXP_SZE", SqlDbType.Decimal).Value = oop.EXP_SZE;
                                    command.Parameters.AddWithValue("@LOT_NO", SqlDbType.Int).Value = oop.LOT_NO;
                                    command.Parameters.AddWithValue("@ORTIDX", SqlDbType.SmallInt).Value = oop.ORTIDX;
                                    command.Parameters.AddWithValue("@LOT_DTE", SqlDbType.Int).Value = oop.LOT_DTE;
                                    //command.Parameters.AddWithValue("@INVOICING_FLAG", SqlDbType.TinyInt).Value = oop.INVOICING_FLAG;
                                    command.Parameters.AddWithValue("@LOT_SEQ", SqlDbType.SmallInt).Value = oop.LOT_SEQ;
                                    //command.Parameters.AddWithValue("@ACCT", SqlDbType.Int).Value = oop.ACCT;
                                    command.Parameters.AddWithValue("@BIN", SqlDbType.SmallInt).Value = oop.BIN;
                                    command.Parameters.AddWithValue("@BGN_BIN", SqlDbType.SmallInt).Value = oop.BGN_BIN;
                                    command.Parameters.AddWithValue("@END_BIN", SqlDbType.SmallInt).Value = oop.END_BIN;
                                    //command.Parameters.AddWithValue("@MDE_CDE", SqlDbType.TinyInt).Value = oop.MDE_CDE;
                                    command.Parameters.AddWithValue("@EMAILED", SqlDbType.TinyInt).Value = oop.EMAILED;
                                    command.Parameters.AddWithValue("@ADD_DAYS", SqlDbType.SmallInt).Value = oop.ADD_DAYS;
                                    command.Parameters.AddWithValue("@DTE_ADDED", SqlDbType.Int).Value = oop.DTE_ADDED;

                                    //command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = oop.ORD_NO;
                                    //command.Parameters.AddWithValue("@MDL_CNT", SqlDbType.SmallInt).Value = oop.MDL_CNT;
                                    //command.Parameters.AddWithValue("@PAT_POS", SqlDbType.VarBinary).Value = oop.PAT_POS;
                                    //command.Parameters.AddWithValue("@MDL_NO", SqlDbType.VarChar).Value = oop.MDL_NO;
                                    //command.Parameters.AddWithValue("@OPT_TYPE", SqlDbType.VarChar).Value = oop.OPT_TYPE;
                                    //command.Parameters.AddWithValue("@OPT_NUM", SqlDbType.SmallInt).Value = oop.OPT_NUM;
                                    //command.Parameters.AddWithValue("@CLR", SqlDbType.VarChar).Value = oop.CLR;
                                    //command.Parameters.AddWithValue("@DESC", SqlDbType.VarChar).Value = oop.DESC;
                                    //// command.Parameters.AddWithValue("@WIN_CNT", SqlDbType.Int).Value = oop.WIN_CNT;
                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();

                                    yield return oop;
                                }
                            }
                        }
                        break;
                case SPCmds.COMPLETEORDER:
                        OrderMasterData ocs = (OrderMasterData)dat;
                        //ScanFileData sfd = new ScanFileData();
                        Logger.Info($"{SP.ToString()} {ocs.ToString()}");
                        ocs.status = MobileDeliveryGeneral.Definitions.status.Completed;
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    try
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
                                        int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();
                                    }
                                    catch (Exception ex) {
                                        Logger.Error($"COMPLETEORDER Exception {ex.Message}");
                                    }
                                    yield return ocs;
                                }
                            }
                        }
                        break;
                    case SPCmds.COMPLETESTOP:
                        StopData sd = (StopData)dat;
                        Logger.Info($"{SP.ToString()} {sd.ToString()}");
                        sd.Status = OrderStatus.Delivered;
                        using (cnn = NewConnection())
                        {
                            using (var adapter = new SqlDataAdapter())
                            {
                                using (var command = new SqlCommand(SP.ToString(), cnn))
                                {
                                    /*
                                    @DSP_SEQ bigint,
                                    @ManifestId smallint,
                                    @Status tinyint,
	                                @POD image,
                                    */
                                    command.CommandType = CommandType.StoredProcedure;
                                    //Completed
                                    command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = sd.Status;
                                    command.Parameters.AddWithValue("@POD", SqlDbType.Image).Value = sd.POD;
                                    command.Parameters.AddWithValue("@ManifestId", SqlDbType.TinyInt).Value = sd.ManifestId;
                                    command.Parameters.AddWithValue("@DSP_SEQ", SqlDbType.Int).Value = sd.DisplaySeq;

                                    var ScanTime = command.Parameters.Add("@ScanTime", SqlDbType.DateTime);
                                    ScanTime.Direction = ParameterDirection.Output;

                                    adapter.InsertCommand = command;
                                    int cmdcnt = adapter.InsertCommand.ExecuteNonQuery();
                                    try
                                    { 
                                        if (!(ScanTime.Value is DBNull))
                                            sd.ScanDateTime = (DateTime)ScanTime.Value;
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.Error($"COMPLETESTOP Exception {ex.Message}");
                                    }
                                    yield return sd;
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
            catch (Exception ex) { Logger.Error($"MyQueryReader_OrderMaster: {ex.Message} {md.ToString()}"); }
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
            catch (Exception ex) { Logger.Error($"MyQueryReader_Driver: {ex.Message} {md.ToString()}"); }
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
                trk.ShipDate = reader.GetInt64(reader.GetOrdinal("SHP_DTE"));
                trk.Description = reader.GetString(reader.GetOrdinal("DESC$"));
                trk.Notes = reader.GetString(reader.GetOrdinal("NOTES"));
                Logger.Info($"MyQueryReader_Trucks: {trk.ToString()}");
            }
            catch (Exception ex) { Logger.Error($"MyQueryReader_Trucks: {ex.Message} {trk.ToString()}"); }
            return trk;
        }
        public static isaCommand MyQueryReader_Stops(SqlDataReader reader, Func<byte[], Task> cb)
        {
            stops st = new stops();
            try
            {
                st.ManifestId = reader.GetInt64(reader.GetOrdinal("ManifestId"));

                st.DisplaySeq = reader.GetInt32(reader.GetOrdinal("DSP_SEQ"));

                if (!reader.IsDBNull(reader.GetOrdinal("DLR_NO")))
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
            catch (Exception ex) { Logger.Error($"MyQueryReader_Stops: {ex.Message} {st.ToString()}"); }
            return st;
        }
        public static isaCommand MyQueryReader_Orders(SqlDataReader reader, Func<byte[], Task> cb)
        {
            orders ordMst = new orders();
            try
            {
                ordMst.ORD_NO = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                ordMst.DLR_NO = reader.GetInt32(reader.GetOrdinal("DLR_NO"));
                ordMst.DLR_PO = reader.GetString(reader.GetOrdinal("DLR_PO"));
                ordMst.ORD_DTE = reader.GetInt32(reader.GetOrdinal("ORD_DTE"));
                ordMst.SHIP_DTE = reader.GetInt32(reader.GetOrdinal("SHIP_DTE"));
                ordMst.CMNT1 = reader.GetString(reader.GetOrdinal("CMNT1"));
                ordMst.CMNT2 = reader.GetString(reader.GetOrdinal("CMNT2"));
                ordMst.DLR_NME = reader.GetString(reader.GetOrdinal("DLR_NME"));
                ordMst.DLR_ADDR = reader.GetString(reader.GetOrdinal("DLR_ADDR"));
                ordMst.DLR_ADDR2 = reader.GetString(reader.GetOrdinal("DLR_ADDR2"));
                ordMst.SHP_NME = reader.GetString(reader.GetOrdinal("SHP_NME"));
                ordMst.SHP_ADDR = reader.GetString(reader.GetOrdinal("SHP_ADDR"));
                ordMst.SHP_ADDR2 = reader.GetString(reader.GetOrdinal("SHP_ADDR2"));
                ordMst.SHP_CSZ = reader.GetString(reader.GetOrdinal("SHP_CSZ"));
                ordMst.SHP_TEL = reader.GetString(reader.GetOrdinal("SHP_TEL"));
                ordMst.SHP_CT = reader.GetInt16(reader.GetOrdinal("SHP_CT"));
                ordMst.SHP_ZIP = reader.GetString(reader.GetOrdinal("SHP_ZIP"));
                ordMst.CUS_NME = reader.GetString(reader.GetOrdinal("CUS_NME"));
                ordMst.CUS_ADDR = reader.GetString(reader.GetOrdinal("CUS_ADDR"));
                ordMst.CUS_CSZ = reader.GetString(reader.GetOrdinal("CUS_CSZ"));
                ordMst.CUS_TEL = reader.GetString(reader.GetOrdinal("CUS_TEL"));
                ordMst.RTE_CDE = reader.GetString(reader.GetOrdinal("RTE_CDE"));
                //ordMst.ORD_TYPE = reader.GetString(reader.GetOrdinal("ORD_TYPE"));
                //ordMst.DEPOSIT = reader.GetDecimal(reader.GetOrdinal("DEPOSIT"));
                ordMst.ENT_BY = reader.GetString(reader.GetOrdinal("ENT_BY"));;

                //ordMst.ORD_AMT = reader.GetDecimal(reader.GetOrdinal("ORD_AMT"));
                ordMst.WIN_QTY = reader.GetInt16(reader.GetOrdinal("WIN_QTY"));
                //ordMst.PAR_QTY = reader.GetInt16(reader.GetOrdinal("PAR_QTY"));
                ordMst.STK_QTY = reader.GetInt16(reader.GetOrdinal("STK_QTY"));
                ordMst.CMP_QTY = reader.GetInt16(reader.GetOrdinal("CMP_QTY"));
                ordMst.SHP_QTY = reader.GetInt16(reader.GetOrdinal("SHP_QTY"));
                ordMst.SHP_AMT = reader.GetDecimal(reader.GetOrdinal("SHP_AMT"));
                ordMst.MISC_TEXT = reader.GetString(reader.GetOrdinal("MISC_TEXT"));
                ordMst.Status = reader.GetByte(reader.GetOrdinal("Status"));
            }
            catch (Exception ex) { Logger.Error($"MyQueryReader_Orders: {ex.Message} {ordMst.ToString()}"); }
            return ordMst;
        }
        public static isaCommand MyQueryReader_OrderDetails(SqlDataReader reader, Func<byte[], Task> cb)
        {
            orderDetails od = new orderDetails();
            try
            {
                od.requestId = Guid.Empty.ToByteArray();
                od.ORD_NO = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                od.MDL_CNT = reader.GetInt16(reader.GetOrdinal("MDL_CNT"));
                od.MDL_NO = reader.GetString(reader.GetOrdinal("MDL_NO"));
                od.CLR = reader.GetString(reader.GetOrdinal("CLR"));
                od.DESC = reader.GetString(reader.GetOrdinal("DESC$"));
                od.PAT_POS = reader.GetInt16(reader.GetOrdinal("PAT_POS"));
                od.OPT_TYPE = reader.GetString(reader.GetOrdinal("OPT_TYPE"));
                od.OPT_NUM = reader.GetInt16(reader.GetOrdinal("OPT_NUM"));
                int idx = reader.GetOrdinal("ScanDateTime");
                if (!reader.IsDBNull(idx))
                    od.ScanTime = reader.GetDateTime(idx);

                Logger.Info($"MyQueryReader_OrderDetails: {od.ToString()}");
            }
            catch (Exception ex) { Logger.Error($"MyQueryReader_OrderDetails: {ex.Message} {od.ToString()}"); }
            return od;
        }

        static isaCommand MyQueryReader_OrderOptions(SqlDataReader reader, Func<byte[], Task> cb)
        {
            orderOptions oo = new orderOptions();
            try
            {
                oo.ORD_NO = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                oo.MDL_CNT = reader.GetInt16(reader.GetOrdinal("MDL_CNT"));
                oo.PAT_POS = reader.GetByte(reader.GetOrdinal("PAT_POS"));
                oo.MODEL = reader.GetString(reader.GetOrdinal("MODEL"));
                oo.MDL_NO = reader.GetString(reader.GetOrdinal("MDL_NO"));
                oo.OPT_TYPE = reader.GetString(reader.GetOrdinal("OPT_TYPE"));
                oo.OPT_NUM = reader.GetInt16(reader.GetOrdinal("OPT_NUM"));
                oo.STOCK_ID = reader.GetString(reader.GetOrdinal("STOCK_ID"));
                oo.CALL_SIZE = reader.GetString(reader.GetOrdinal("CALL_SIZE"));
                oo.CLR = reader.GetString(reader.GetOrdinal("CLR"));
                oo.DESC = reader.GetString(reader.GetOrdinal("DESC$"));
                oo.QTY = reader.GetInt16(reader.GetOrdinal("QTY"));
                oo.CMP_QTY = reader.GetInt16(reader.GetOrdinal("CMP_QTY"));
                oo.PAT_ID = reader.GetInt16(reader.GetOrdinal("PAT_ID"));
                oo.WIDTH = reader.GetDecimal(reader.GetOrdinal("WIDTH"));
                oo.HEIGHT = reader.GetDecimal(reader.GetOrdinal("HEIGHT"));
                oo.COM = reader.GetDecimal(reader.GetOrdinal("COM"));
                oo.PRICE = reader.GetDecimal(reader.GetOrdinal("PRICE"));
                oo.DIS_PER = reader.GetDecimal(reader.GetOrdinal("DIS_PER"));
                oo.DIS_AMT = reader.GetDecimal(reader.GetOrdinal("DIS_AMT"));
                oo.DIS_UNT = reader.GetDecimal(reader.GetOrdinal("DIS_UNT"));
                oo.NET_AMT = reader.GetDecimal(reader.GetOrdinal("NET_AMT"));
                oo.CMT1 = reader.GetString(reader.GetOrdinal("CMT1"));
                oo.CMT2 = reader.GetString(reader.GetOrdinal("CMT2"));
                oo.NOTES = reader.GetString(reader.GetOrdinal("NOTES"));
                oo.SHIPPING = reader.GetString(reader.GetOrdinal("SHIPPING"));
                oo.SHP_DTE = reader.GetInt32(reader.GetOrdinal("SHP_DTE"));
                oo.TRUCK = reader.GetString(reader.GetOrdinal("TRUCK"));
                oo.SHP_SEQUENCE = reader.GetInt16(reader.GetOrdinal("SHP_SEQUENCE"));
                oo.TYPE = reader.GetString(reader.GetOrdinal("TYPE"));
                
                oo.PROD_DESC = reader.GetString(reader.GetOrdinal("PROD_DESC"));
                oo.EXP_SZE = reader.GetDecimal(reader.GetOrdinal("EXP_SZE"));
                oo.LOT_NO = reader.GetInt32(reader.GetOrdinal("LOT_NO"));
                oo.ORTIDX = reader.GetInt16(reader.GetOrdinal("ORTIDX"));
                oo.LOT_DTE = reader.GetInt32(reader.GetOrdinal("LOT_DTE"));
                oo.LOT_SEQ = reader.GetInt16(reader.GetOrdinal("LOT_SEQ"));
                oo.BIN = reader.GetInt16(reader.GetOrdinal("BIN"));
                oo.BGN_BIN = reader.GetInt16(reader.GetOrdinal("BGN_BIN"));
                oo.END_BIN = reader.GetInt16(reader.GetOrdinal("END_BIN"));
                oo.EMAILED = reader.GetByte(reader.GetOrdinal("EMAILED"));
                oo.ADD_DAYS = reader.GetInt16(reader.GetOrdinal("ADD_DAYS"));
                oo.DTE_ADDED = reader.GetInt32(reader.GetOrdinal("DTE_ADDED"));
                oo.requestId = Guid.Empty.ToByteArray();
                oo.ORD_NO = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                oo.MDL_CNT = reader.GetInt16(reader.GetOrdinal("MDL_CNT"));
                oo.MDL_NO = reader.GetString(reader.GetOrdinal("MDL_NO"));
                oo.CLR = reader.GetString(reader.GetOrdinal("CLR"));
                oo.DESC = reader.GetString(reader.GetOrdinal("DESC$"));
                oo.WIDTH = reader.GetDecimal(reader.GetOrdinal("WIDTH"));
                oo.HEIGHT = reader.GetDecimal(reader.GetOrdinal("HEIGHT"));
                //int idx = reader.GetOrdinal("ScanDateTime");
                //if (!reader.IsDBNull(idx))
                //    oo.ScanTime = reader.GetDateTime(idx);

                Logger.Info($"MyQueryReader_OrderOptions: {oo.ToString()}");
            }
            catch (Exception ex) { Logger.Error($"MyQueryReader_OrderOptions: {ex.Message} {oo.ToString()}"); }
            return oo;
        }

        static byte[] bArr = new byte[5000];

        public static isaCommand MyQueryReader_BillableOrders(SqlDataReader reader, Func<byte[], Task> cb)
        {
            accountReceivable ar = new accountReceivable();

            try
            {
                /* ManifestId, s.DSP_SEQ, s.DLR_NO, om., s.Status as Status_S, 
                 * s.POD, s.Timestamp, om.CustomerId,
			od.Status as Status_OD, od.ScanDateTime as ScanDateTime_OD, od.WIN_CNT*/

                ar.ManifestId = reader.GetInt32(reader.GetOrdinal("ManifestId"));
                ar.DSP_SEQ = reader.GetInt32(reader.GetOrdinal("DSP_SEQ"));
                ar.DLR_NO = reader.GetInt32(reader.GetOrdinal("DLR_NO"));
                ar.ORD_NO = reader.GetInt32(reader.GetOrdinal("ORD_NO"));
                ar.Status_S = reader.GetByte(reader.GetOrdinal("Status_S"));
                ar.POD = new byte[5000];

                
                var res = reader.GetBytes(reader.GetOrdinal("POD"), 0, bArr, 0, 5000);
                
                ar.POD = new byte[res+1];
                Buffer.BlockCopy(bArr,0, ar.POD, 0, (int)res);
                //bArr.CopyTo(ar.POD, 0);
                if (!reader.IsDBNull(reader.GetOrdinal("Timestamp")))
                    ar.Timestamp = reader.GetDateTime(reader.GetOrdinal("Timestamp"));
                //if(ldatetime>0)
                //    ar.Timestamp = DateTime.FromBinary(ldatetime);

                ar.CUS_NME = reader.GetString(reader.GetOrdinal("CUS_NME"));
                ar.DLR_NME = reader.GetString(reader.GetOrdinal("DLR_NME"));
                ar.RTE_CDE = reader.GetString(reader.GetOrdinal("RTE_CDE"));


                ar.Status_OD = reader.GetByte(reader.GetOrdinal("Status_OD"));

                if (!reader.IsDBNull(reader.GetOrdinal("ScanDateTime_OD")))
                    ar.ScanDateTime_OD = reader.GetDateTime(reader.GetOrdinal("ScanDateTime_OD"));
                //if(ldatetime>0)
                //    ar.ScanDateTime_OD = DateTime.FromBinary(ldatetime);

                ar.WIN_CNT = reader.GetInt32(reader.GetOrdinal("WIN_CNT"));

                Logger.Info($"myQueryData_GetShippedOrders Done: {ar.ToString()}");
            }

            catch (Exception ex) { }
            return ar;
        }

        public static isaCommand myQueryData_GetManifest(SqlDataReader reader, Func<byte[], Task> cb)
        {
            manifestMaster mm = new manifestMaster();
            try
            {
                mm.id = reader.GetInt64(reader.GetOrdinal("ManifestId"));
                mm.DriverId = reader.GetInt32(reader.GetOrdinal("DriverId"));
                mm.TRK_CDE = reader.GetString(reader.GetOrdinal("TRK_CDE")).GetBytes();
                mm.SHIP_DTE = reader.GetInt64(reader.GetOrdinal("SHP_DTE"));
                mm.DESC = reader.GetString(reader.GetOrdinal("DESC$"));
                mm.NOTES = reader.GetString(reader.GetOrdinal("NOTES"));
                mm.LINK = reader.GetInt64(reader.GetOrdinal("LINK"));
                mm.Status = (TruckManifestStatus)reader.GetInt16(reader.GetOrdinal("Status"));
                //mm.command = eCommand.ManifestLoadComplete;
                Logger.Info($"myQueryData_GetManifest: {mm.ToString()}");
            }
            catch (Exception ex) { Logger.Error($"myQueryData_GetManifest: {ex.Message} {mm.ToString()}"); }
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
                case eCommand.OrdersUpload:
                case eCommand.OrdersLoad:
                    SP = SPCmds.GETORDERS;
                    break;
                case eCommand.OrderDetails:
                    SP = SPCmds.GETORDERDETAILS;
                    break;
                case eCommand.OrderOptions:
                    SP = SPCmds.GETORDEROPTIONS;
                    break;
                case eCommand.GenerateManifest:
                case eCommand.CheckManifest:
                    SP = SPCmds.GETMANIFEST;
                    break;
                case eCommand.AccountReceivable:
                    SP = SPCmds.GETDELIVEREDORDERS;
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
                                //command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd");
                                command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = (long)(DateTime.Parse(req.date).FromGregorianToJulian());
                                dat.command = eCommand.DriversLoadComplete;
                                retVal = new DriverData() { RequestId = NewGuid(req.requestId) };
                                Logger.Info($"QueryData Drivers. {retVal.ToString()}");
                                break;
                            case eCommand.Manifest:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@DriverId", SqlDbType.Int).Value = req.id;
                                //command.Parameters.AddWithValue("@Status", SqlDbType.TinyInt).Value = req.
                                retVal = new ManifestMasterData() { RequestId = NewGuid(req.requestId) };
                                Logger.Info($"QueryData Manifest. {req.ToString()} ");
                                dat.command = eCommand.ManifestLoadComplete;
                                break;

                            case eCommand.Trucks:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.BigInt).Value = (long)(DateTime.Parse(req.date).FromGregorianToJulian());
                               // command.Parameters.AddWithValue("@SHP_DTE", SqlDbType.Date).Value = DateTime.Parse(req.date).Date.ToString("yyyy-MM-dd");
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

                            case eCommand.OrdersLoad:
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
                                retVal = new OrderDetailsData() { Command = eCommand.OrderDetails, RequestId = NewGuid(req.requestId) };
                                break;

                            case eCommand.OrderOptions:
                                req = (manifestRequest)dat;
                                command.Parameters.AddWithValue("@ORD_NO", SqlDbType.BigInt).Value = req.id;
                                dat.command = eCommand.OrderOptionsComplete;
                                Logger.Info($"QueryData Order Options. {req.ToString()}");
                                retVal = new OrderOptionsData() { Command = eCommand.OrderOptions, RequestId = NewGuid(req.requestId) };

                                break;

                            case eCommand.CheckManifest:
                                command.Parameters.AddWithValue("@Link", SqlDbType.BigInt).Value = ((manifestMaster)dat).LINK;
                                
                                Logger.Info($"CheckManifest {Enum.GetName(typeof(eCommand), dat.command)}  {dat.ToString()}  ");
                                retVal = new ManifestMasterData((manifestMaster)dat, 0);
                                dat.command = eCommand.CheckManifestComplete;
                                break;
                            case eCommand.GenerateManifest:
                                command.Parameters.AddWithValue("@Link", SqlDbType.BigInt).Value = ((manifestMaster)dat).LINK;
                                dat.command = eCommand.ManifestLoadComplete;
                                Logger.Info($"GenerateManifest {Enum.GetName(typeof(eCommand), dat.command)} Manifest: {dat.ToString()}  ");
                                retVal = new ManifestMasterData((manifestMaster)dat, 0);
                                break;
                            case eCommand.AccountReceivable:
                                dat.command = eCommand.AccountReceivable;
                                retVal = new AccountsReceivableData((accountReceivable)dat);
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

                                case eCommand.OrdersLoad:
                                    var olcmd = (orders)MyQueryReader_Orders(reader, cb);
                                    olcmd.requestId = dat.requestId;
                                    olcmd.command = eCommand.OrdersLoad;
                                    Logger.Info($"QueryData OrdersLoad. {olcmd.ToString()}");
                                    cb(olcmd.ToArray());
                                    break;

                                case eCommand.OrderDetails:
                                    var odcmd = (orderDetails)MyQueryReader_OrderDetails(reader, cb);
                                    odcmd.requestId = dat.requestId;
                                    odcmd.command = eCommand.OrderDetails;
                                    Logger.Info($"QueryData OrderDetails. {odcmd.ToString()}");
                                    cb(odcmd.ToArray());
                                    break;

                                case eCommand.OrderOptions:
                                    var oocmd = (orderDetails)MyQueryReader_OrderOptions(reader, cb);
                                    oocmd.requestId = dat.requestId;
                                    oocmd.command = eCommand.OrderOptions;
                                    Logger.Info($"QueryData OrderOptions. {oocmd.ToString()}");
                                    cb(oocmd.ToArray());
                                    break;
                                case eCommand.AccountReceivable:
                                    var acrcv = (accountReceivable)MyQueryReader_BillableOrders(reader, cb);
                                    acrcv.requestId = dat.requestId;
                                    acrcv.command = eCommand.AccountReceivable;
                                    Logger.Info($"QueryData Invocing PODs. {acrcv.ToString()}");

                                    byte[] barr = acrcv.ToArray();
                                    cb(barr);

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
