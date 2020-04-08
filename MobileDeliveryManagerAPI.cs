using MobileDeliveryGeneral.DataManager.Interfaces;
using System;
using MobileDeliveryGeneral.Data;
using static MobileDeliveryGeneral.Definitions.MsgTypes;
using MobileDeliveryGeneral.Interfaces.DataInterfaces;
using MobileDeliveryManager.UnitedMobileData;
//using MobileDataManagerWinsys.WinSysInterface;
using MobileDeliveryGeneral.Interfaces;
using System.Threading.Tasks;
using static MobileDeliveryGeneral.Definitions.enums;
using System.Collections.Generic;
using MobileDeliveryGeneral.Settings;
using MobileDeliveryClient.API;
using MobileDeliveryGeneral.Utilities;
using MobileDeliveryLogger;
using MobileDeliveryManger.UnitedMobileData;

namespace MobileDeliveryManager
{
    public class MobileDeliveryManagerAPI : isaMDM
    {
        UMDManifest UMDServer;
        public static string AppName;
        public ProcessMsgDelegateRXRaw pmRx { get; private set; }
        ReceiveMsgDelegate rm;
        SendMsgDelegate sm;
        SendMessages WinSysSM;
        UMDServerConnection conn;
        ManifestDetails drillDown;

        public MobileDeliveryManagerAPI()
        {
        }

        public void Init(UMDAppConfig config)
        {
            rm = new ReceiveMsgDelegate(ReceiveMessage);
            pmRx = new ProcessMsgDelegateRXRaw(HandleClientCmd);
            AppName = config.AppName;
            Logger.Debug($"{config.AppName} Connection init");

            if (config.srvSet == null)
            {
                Logger.Error($"{config.AppName} Missing Configuration Server Settings");
                throw new Exception($"{config.AppName} Missing Configuration Server Settings.");
            }



            conn = new UMDServerConnection(config.srvSet, ref sm, rm);
            conn.Connect();

            UMDServer = new UMDManifest(config.SQLConn);
 
            drillDown = new ManifestDetails(sm, rm, pmRx);
        }

        public isaCommand ReceiveMessage(isaCommand cmd)
        {
            switch (cmd.command)
            {
                case eCommand.Ping:
                    Logger.Debug($"ReceiveMessage - Received Ping / Replying Pong..");
                    WinSysSM.SendMessage(new Command() { command = eCommand.Pong });
                    break;
                case eCommand.Pong:
                    Logger.Debug($"ReceiveMessage - Received Pong");
                    break;
                case eCommand.LoadFiles:
                    Logger.Info($"ReceiveMessage - Copy Files from Winsys Server Paths top App Server Paths.  reqId: {cmd.requestId}");
                    //CopyFilesToServer(DateTime.Today);
                    Logger.Info($"ReceiveMessage - Replying LoadFilesComplete...");
                    WinSysSM.SendMessage(new Command() { command = eCommand.LoadFilesComplete });
                    //cbsend(new Command() { command = eCommand.LoadFilesComplete }.ToArray());
                    break;
                case eCommand.GenerateManifest:
                    Logger.Info($"ReceiveMessage - Generate Manifest from Winsys and SqlServer.  reqId: {cmd.requestId}");
                    manifestRequest req = (manifestRequest)cmd;
                    WinSysSM.SendMessage(req);
                    break;
                case eCommand.ManifestDetails:
                    Logger.Info($"ReceiveMessage - Generate Manifest Details from Winsys - API Drill Down.  reqId: {cmd.requestId}");
                    //drillDown.reportMDProgressChanged(50, cmd );
                    foreach (var mmd in UMDServer.Persist(SPCmds.INSERTMANIFESTDETAILS, new ManifestDetailsData((manifestDetails)cmd)))
                        drillDown.GetOrderMasterData((ManifestDetailsData)mmd);
                    break;
                case eCommand.ManifestDetailsComplete:
                    Logger.Info($"ReceiveMessage - ManifestDetailsComplete  reqId: {cmd.requestId}");
                    //drillDown.reportMDProgressChanged(100, cmd);
                    break;
                case eCommand.Orders:
                    Logger.Info($"ReceiveMessage - Orders  reqId: {cmd.requestId}");
                    foreach (var omd in UMDServer.Persist(SPCmds.INSERTORDER, new OrderMasterData((orderMaster)cmd)))
                    {
                        drillDown.GetOrderDetailsData((OrderMasterData)omd);
                        drillDown.GetOrderOptionsData((OrderMasterData)omd);
                    }
                        break;
                case eCommand.OrderOptions:
                    Logger.Info($"ReceiveMessage - OrderOptions  reqId: {cmd.requestId}");
                    UMDServer.Persist(SPCmds.INSERTORDEROPTIONS, new OrderOptionsData((orderOptions)cmd));
                       
                    break;
                case eCommand.OrderDetails:
                    Logger.Info($"ReceiveMessage - OrderDetails  reqId: {cmd.requestId}");
                    UMDServer.Persist(SPCmds.INSERTORDERDETAILS, new OrderDetailsData((orderDetails)cmd));
                      //  drillDown.GetOrderMasterData((ManifestDetailsData)mmd);
                    break;
                case eCommand.RunQuery:
                    //DriverData dd = (DriverData)GetDrivers(cbsend);
                    break;
                case eCommand.Drivers:
                    Logger.Info($"ReceiveMessage - Drivers   reqId: {cmd.requestId}");
                    break;
                case eCommand.OrderDetailsComplete:
                    Logger.Info($"ReceiveMessage - OrderDetailsComplete reqId: {cmd.requestId}");
                    break;
                case eCommand.OrderOptionsComplete:
                    Logger.Info($"ReceiveMessage - OrderOptionsComplete reqId: {cmd.requestId}");
                    break;
                case eCommand.OrderUpdatesComplete:
                    Logger.Info($"ReceiveMessage - OrderUpdatesComplete reqId: {cmd.requestId}");
                    break;
                default:
                    Logger.Error("ReceiveMessage - ERROR Unknown command.  Parse Error MDM-API");
                    break;
            }
            return cmd;
        }
        public void HandleClientCmd(byte[] bytes_cmd, Func<byte[], Task> cbsend)
        {
            isaCommand cmd = new Command().FromArray(bytes_cmd);
            switch (cmd.command)
            {
                case eCommand.Ping:
                    Logger.Debug("HandleClientCmd -  Received Ping / Replying Pong..");
                    cbsend(new Command() { command = eCommand.Pong }.ToArray());
                    break;
                case eCommand.Pong:
                    Logger.Debug("HandleClientCmd -  Received Pong");
                    break;
                case eCommand.LoadFiles:
                    Logger.Info("HandleClientCmd - Copy Files from Winsys Server Paths top App Server Paths.");
                    //CopyFilesToServer(DateTime.Today);
                    Logger.Info("HandleClientCmd - Replying LoadFilesComplete...");
                    cbsend(new Command() { command = eCommand.LoadFilesComplete }.ToArray());
                    break;
                case eCommand.GenerateManifest:
                    Logger.Info($"HandleClientCmd - ManifestDetails.{bytes_cmd.Length}");

                    manifestMaster mM = (manifestMaster)new manifestMaster().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd Generate Manifest from Winsys and SqlServer.  reqId: {new Guid(mM.requestId).ToString()}");
                    if (mM.LINK != 0)
                    {
                        ManifestMasterData mmd = (ManifestMasterData)UMDServer.QueryData(cbsend, mM);
                        Logger.Info($"API Manager GenerateManifest. " +
                                   $"Cmd:{mmd.Command.ToString()}  " +
                                   $"reqId: {mmd.RequestId.ToString()}  " +
                                   $"link: {mmd.LINK}  " +
                                   $"manId: {mmd.ManifestId}  ");

                    }
                    else
                    {
                        WinSysSM.SendMessage(cmd);
                    }
                    break;
                case eCommand.RunQuery:
                    Logger.Info($"HandleClientCmd - ManifestDetails.{bytes_cmd.Length}");

                    //DriverData dd = (DriverData)GetDrivers(cbsend);
                    break;
                case eCommand.Manifest:
                    Logger.Info($"HandleClientCmd - Manifest from Winsys and SqlServer.  reqId: {new Guid(cmd.requestId).ToString()}");
                    cbsend(cmd.ToArray());
                    break;
                case eCommand.Trucks:
                    Logger.Info($"HandleClientCmd - Trucks.{bytes_cmd.Length}");

                    isaCommand req = new manifestRequest().FromArray(bytes_cmd);
                    TruckData td = (TruckData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"HandleClientCmd - Trucks.  reqId: {td.RequestId.ToString()}");
                    break;
                case eCommand.ManifestDetails:
                    Logger.Info($"HandleClientCmd - ManifestDetails.{bytes_cmd.Length}");
                    isaCommand reqmd = new manifestRequest().FromArray(bytes_cmd);
                   // ManifestDetailsData mdd = 
                    manifestDetails manDet = new manifestDetails();
                    ManifestDetailsData manDetData = (ManifestDetailsData)manDet.FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - ManifestDetails.  reqId: {manDetData.RequestId.ToString()}");

                    foreach (var mmd in UMDServer.Persist(SPCmds.INSERTMANIFESTDETAILS, manDetData))
                    {
                        drillDown.GetOrderMasterData((ManifestDetailsData)mmd);
                        mmd.Command = eCommand.ManifestDetails;
                        mmd.RequestId = new Guid(manDet.requestId);
                        Logger.Info($"HandleClientCmd - ManifestDetails.  Sending GetOrderMasterData reqId: {mmd.RequestId.ToString()} {mmd.Command}");
                        cbsend(((manifestDetails)mmd).ToArray());
                    }
                    break;
                    
                case eCommand.Orders:
                    Logger.Info($"HandleClientCmd - ManifestDetails.{bytes_cmd.Length}");

                    var om = (orderMaster)new orderMaster().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - Orders.  reqId: {new Guid(om.requestId).ToString()}");
                    foreach (var omd in UMDServer.Persist(SPCmds.INSERTORDER, new OrderMasterData(om)))
                        drillDown.GetOrderDetailsData((OrderMasterData)omd);
                    break;
                case eCommand.OrderOptions:
                    Logger.Info($"HandleClientCmd - ManifestDetails.{bytes_cmd.Length}");

                    var oo = (orderOptions)new orderOptions().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - OrderOptions.  reqId: {new Guid(cmd.requestId).ToString()}");
                    foreach (var mmd in UMDServer.Persist(SPCmds.INSERTORDEROPTIONS, new OrderOptionsData(oo)))
                    {
                        var oopt = new orderOptions((OrderOptionsData)mmd);
                        oopt.command = eCommand.OrderOptionsComplete;
                        cbsend(oopt.ToArray());
                    }
                    break;
                case eCommand.OrderDetails:
                    Logger.Info($"HandleClientCmd - ManifestDetails.{bytes_cmd.Length}");

                    var odt = (orderDetails)new orderDetails().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - OrderDetails.  reqId: {new Guid(odt.requestId).ToString()}");
                    foreach (var mmd in UMDServer.Persist(SPCmds.INSERTORDERDETAILS, new OrderDetailsData(odt)))
                    {
                        orderDetails odd = new orderDetails((OrderDetailsData)mmd);
                        odd.command = eCommand.OrderDetailsComplete;
                        cbsend(odd.ToArray());
                    }    
                    break;

                case eCommand.Drivers:
                    Logger.Info($"HandleClientCmd - ManifestDetails.  reqId: {cmd.requestId}");
                    req = new manifestRequest().FromArray(bytes_cmd);
                    DriverData dd = (DriverData)UMDServer.QueryData(cbsend, req);
                    //drivers drvs = new drivers(dd);
                    //drvs.command = eCommand.DriversLoadComplete;
                    //cbsend(drvs.ToArray());

                    //cbsend(new Command() { command = eCommand.DriversLoadComplete, value = dd.DriverId.ToString() }.ToArray());
                    break;
                case eCommand.UploadManifest:
                    Logger.Info($"HandleClientCmd - UploadManifest. {bytes_cmd.Length}");

                    manifestRequest mreq = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - UploadManifest.  reqId: {new Guid(mreq.requestId).ToString()}");
                    //ManifestMasterData Mmd = (ManifestMasterData)cmd;
                    manifestMaster mm = (manifestMaster)new manifestMaster().FromArray(mreq.bData);
                    IEnumerable<IMDMMessage> mmdata = UMDServer.Persist(SPCmds.INSERTMANIFEST, new ManifestMasterData(mm, mm.id));
                    Logger.Info($"HandleClientCmd - UploadManifest Persisted. {mm.id}");

                    try
                    {
                        ManifestMasterData mmd=null;
                        foreach (var mmdIt in mmdata)
                        {
                            mmd = (ManifestMasterData)mmdIt;
                            Logger.Info($"HandleClientCmd - UploadManifest - Get ManifestDetails sending ManMastData /nTrk: {mmd.TRK_CDE}  manId:{((ManifestMasterData)mmdIt).ManifestId} reqId: {new Guid(mreq.requestId).ToString()}");
                            drillDown.GetManifestDetails(mmd, cbsend);
                        }
                        if (mmd != null)
                        {
                            manifestMaster mmRet = new manifestMaster(mmd);
                            mmRet.command = eCommand.ManifestLoadComplete;
                            Logger.Info($"HandleClientCmd - UploadManifest - Done (ManifestLoadComplete).  reqId: {new Guid(mreq.requestId).ToString()}");
                            cbsend(mmRet.ToArray());
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.Debug("HandleClientCmd - Error exception = " + e.Message);
                    }

                    break;
                case eCommand.Stops:
                    Logger.Info($"HandleClientCmd - Stops.  reqId: {cmd.requestId}");
                    req = new manifestRequest().FromArray(bytes_cmd);
                    StopData sd = (StopData)UMDServer.QueryData(cbsend, req);
                    stops stps = new stops(sd);
                    stps.command = eCommand.StopsLoadComplete;
                    cbsend(stps.ToArray());

                    break;
                case eCommand.OrdersLoad:
                    req = new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - OrdersLoad (Start QueryData).  reqId: {new Guid(req.requestId).ToString()}");
                    OrderData od = (OrderData)UMDServer.QueryData(cbsend, req);
                    orders odrs = new orders(od);
                    odrs.command = eCommand.OrdersLoadComplete;
                    Logger.Info($"HandleClientCmd - OrdersLoad (OrdersLoadComplete).  reqId: {new Guid(odrs.requestId).ToString()}");
                    cbsend(odrs.ToArray());
                    break;
                default:
                    Logger.Error("HandleClientCmd - ERROR Unknown command.  Parse Error MDM-API");
                    break;
            }
        }
        public bool SendMessage(isaCommand cmd) {
            return WinSysSM.SendMessage(cmd);
        }
        //public IMDMMessage CopyFilesToServer(DateTime mainfestDate, string Route="")
        //{
        //    //FileTransfer ft = new FileTransfer(WinsysTxFiles);
        //    FileCopy fc = ft.CopyFiles();

        //    //return fc;
        //}
    }
}
