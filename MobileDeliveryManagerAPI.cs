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
        ClientToServerConnection conn;
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



            conn = new ClientToServerConnection(config.srvSet.srvurl, config.srvSet.srvport, config.AppName, ref sm, rm);
            conn.Connect();

            UMDServer = new UMDManifest(config.SQLConn);
 
            drillDown = new ManifestDetails(sm, rm, pmRx);
        }

        Dictionary<Guid, Func<byte[], Task>> dRetCall = new Dictionary<Guid, Func<byte[], Task>>();
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
                    Logger.Info($"ReceiveMessage - Copy Files from Winsys Server Paths top App Server Paths:{cmd.ToString()}");
                    //CopyFilesToServer(DateTime.Today);
                    Logger.Info($"ReceiveMessage - Replying LoadFilesComplete...");
                    WinSysSM.SendMessage(new Command() { command = eCommand.LoadFilesComplete });
                    //cbsend(new Command() { command = eCommand.LoadFilesComplete }.ToArray());
                    break;
                case eCommand.GenerateManifest:
                    Logger.Info($"ReceiveMessage - Generate Manifest from Winsys and SqlServer:{cmd.ToString()}");
                    manifestRequest req = (manifestRequest)cmd;
                    WinSysSM.SendMessage(req);
                    break;
                case eCommand.ManifestDetails:
                    Logger.Info($"ReceiveMessage - Generate Manifest Details from Winsys - API Drill Down:{cmd.ToString()}");
                    manifestDetails manDet = (manifestDetails)cmd;

                    ManifestDetailsData manDetData = new ManifestDetailsData(manDet);
                    Logger.Info($"INSERTMANIFESTDETAILS (Persist): {manDetData.ToString()}");
                    foreach (ManifestDetailsData md in UMDServer.Persist(SPCmds.INSERTMANIFESTDETAILS, manDetData))
                    {
                        drillDown.GetOrderMasterData(md);
                        Logger.Info($"ManifestDetails. drillDown.GetOrderMasterData:{md.ToString()}");
                    }

                    //manDet.command = eCommand.ManifestDetailsComplete;
                    //dRetCall[NewGuid(cmd.requestId)](manDet.ToArray());
                    Logger.Info($"ManifestDetails Complete: {manDet.ToString()}");
                    break;
                    
                case eCommand.ManifestDetailsComplete:
                    Logger.Info($"ReceiveMessage - ManifestDetailsComplete:{cmd.ToString()}");
                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    //drillDown.reportMDProgressChanged(100, cmd);
                    break;
                case eCommand.Orders:
                    Logger.Info($"ReceiveMessage - Orders  reqId: {cmd.requestId}");
                    List<IMDMMessage> lstOrd = new List<IMDMMessage>();
                    OrderMasterData omd = new OrderMasterData(((orderMaster)cmd));
                    orderMaster om = new orderMaster(omd);
                    foreach (var omdit in UMDServer.Persist(SPCmds.INSERTORDER, omd))
                    {
                        Logger.Info($"INSERTORDER Complete: {omdit.ToString()}");
                        drillDown.GetOrderDetailsData((OrderMasterData)omd);
                        drillDown.GetOrderOptionsData((OrderMasterData)omd);
                    }

                    om.command = eCommand.OrdersLoadComplete;
                    dRetCall[NewGuid(om.requestId)](om.ToArray());

                    break;
                case eCommand.OrderOptions:
                    Logger.Info($"ReceiveMessage - OrderOptions  reqId:{cmd.ToString()}");
                   try{
                        orderOptions oo = (orderOptions)cmd;
                        OrderOptionsData ood = new OrderOptionsData(oo);

                        foreach(var oodit in UMDServer.Persist(SPCmds.INSERTORDEROPTIONS, ood))
                            Logger.Info($"INSERTORDEROPTIONS Complete: {oodit.ToString()}");

                        oo.command = eCommand.OrderOptionsComplete;
                        dRetCall[NewGuid(oo.requestId)](cmd.ToArray());
                    }
                    catch (Exception ex) { Logger.Info($"eCommand.OrderOptions {ex.Message}"); }

                    break;
                case eCommand.OrderDetails:
                    Logger.Info($"ReceiveMessage - OrderDetails:{cmd.ToString()}");
                    try
                    {
                        orderDetails od = (orderDetails)cmd;
                        OrderDetailsData odd = new OrderDetailsData(od);
                        
                        foreach (var odit in UMDServer.Persist(SPCmds.INSERTORDERDETAILS, odd))
                            Logger.Info($"INSERTORDERDETAILS Complete: {odit.ToString()}");

                        dRetCall[NewGuid(od.requestId)](cmd.ToArray());
                    }
                    catch (Exception ex) { Logger.Info($"eCommand.OrderDetails {ex.Message}"); }
                    break;
                case eCommand.RunQuery:
                    //DriverData dd = (DriverData)GetDrivers(cbsend);
                    break;
                case eCommand.Drivers:
                    Logger.Info($"ReceiveMessage - Drivers:{cmd.ToString()}");
                    break;
                case eCommand.OrderDetailsComplete:
                    Logger.Info($"ReceiveMessage - OrderDetailsComplete:{cmd.ToString()}");
                    // Can we detrmine the completed transaction at this point from the cmd in order to clean the completed tx for dRetCall
                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    break;
                case eCommand.OrderOptionsComplete:
                    Logger.Info($"ReceiveMessage - OrderOptionsComplete:{cmd.ToString()}");
                    // Can we detrmine the completed transaction at this point from the cmd in order to clean the completed tx for dRetCall
                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    break;
                case eCommand.OrderUpdatesComplete:
                    Logger.Info($"ReceiveMessage - OrderUpdatesComplete:{cmd.ToString()}");
                    
                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    break;
                case eCommand.OrdersLoadComplete:
                    Logger.Info($"ReceiveMessage - OrdersLoadComplete:{cmd.ToString()}");

                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    
                    break;
                case eCommand.ManifestLoadComplete:
                    Logger.Info($"ReceiveMessage - OrdersLoadComplete:{cmd.ToString()}");
                    Logger.Info($"ReceiveMessage - OrderDetailsComplete:{cmd.ToString()}");

                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
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
                    Logger.Info("HandleClientCmd - Copy Files from Winsys Server Paths top App Server Paths:{cmd.ToString()}");
                    //CopyFilesToServer(DateTime.Today);
                    Logger.Info("HandleClientCmd - Replying LoadFilesComplete...");
                    cbsend(new Command() { command = eCommand.LoadFilesComplete }.ToArray());
                    break;
                case eCommand.GenerateManifest:
                    Logger.Info($"HandleClientCmd - Generate Manifest:{cmd.ToString()}");

                    manifestMaster mM = (manifestMaster)new manifestMaster().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd Generate Manifest from Winsys and SqlServer:{mM.ToString()}");
                    if (mM.LINK != 0)
                    {
                        ManifestMasterData mmd1 = (ManifestMasterData)UMDServer.QueryData(cbsend, mM);
                        Logger.Info($"API Manager GenerateManifest QueryData Complete. {mmd1.ToString()}");
                    }
                    else
                    {
                        
                        WinSysSM.SendMessage(cmd);
                    }
                    break;
                case eCommand.CheckManifest:
                    Logger.Info($"HandleClientCmd - CheckManifest: {cmd.ToString()}");
                    manifestMaster mMst = (manifestMaster)new manifestMaster().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd Check Manifest in ManagerAPI/SqlServer: {mMst.ToString()}");
                    ManifestMasterData mamd = (ManifestMasterData)UMDServer.QueryData(cbsend, mMst);
                    Logger.Info($"API Manager Check Manifest. {mamd.ToString()}");
                    break;
                case eCommand.RunQuery:
                    Logger.Info($"HandleClientCmd - ManifestDetails: {cmd.ToString()}");
                    break;
                case eCommand.Manifest:
                    Logger.Info($"HandleClientCmd - Manifest: {cmd.ToString()}");
                    cbsend(cmd.ToArray());
                    break;
                case eCommand.ManifestDetails:
                    throw new Exception("This should be handled by the Receive Message handler.");                   
                case eCommand.Orders:
                    Logger.Info($"HandleClientCmd - Orders: {cmd.ToString()}");
                    orderMaster om = (orderMaster)new orderMaster().FromArray(bytes_cmd);

                    Logger.Info($"Persist INSERTORDER {om.ToString()}");
                    foreach (OrderMasterData omd in UMDServer.Persist(SPCmds.INSERTORDER, new OrderMasterData(om)))
                    {

                        Logger.Info($"Orders - drillDown.GetOrderDetailsData: {omd.ToString()}");
                        drillDown.GetOrderDetailsData(omd);
                        omd.Command = eCommand.OrdersLoadComplete;
                    }


                    Logger.Info($"INSERTORDER Complete. OrdersLoadComplete: {om.ToString()}");
                    om.command = eCommand.ManifestDetailsComplete;
                    cbsend(om.ToArray());

                    break;

                case eCommand.CompleteStop:
                    Logger.Info($"HandleClientCmd - Orders: {cmd.ToString()}");

                    manifestRequest mreq = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    Logger.Info($"HandleClientCmd - CompleteStop:  {mreq.ToString()}");

                    orderMaster cs = (orderMaster)new orderMaster().FromArray(mreq.bData);

                    Logger.Info($"Persist COMPLETESTOP {cs.ToString()}");

                    var no = new OrderMasterData(cs);
                    foreach (OrderMasterData omd in UMDServer.Persist(SPCmds.COMPLETESTOP, no))
                    {
                        Logger.Info($"Persisted COMPLETESTOP {omd.ToString()}");
                        cbsend(new orderMaster(omd).ToArray());
                    }

                    break;
                //case eCommand.ResetCompletedOrder:
                //    Logger.Info($"HandleClientCmd - ResetCompletedOrder: {cmd.ToString()}");

                //    var mrq = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                //    Logger.Info($"HandleClientCmd - ResetCompletedOrder:  {mrq.ToString()}");

                //    var cs1 = (orderMaster)new orderMaster().FromArray(mrq.bData);

                //    Logger.Info($"Persist RESETSTOP {cs1.ToString()}");

                //    var no1 = new OrderMasterData(cs1);
                //    foreach (OrderMasterData omd in UMDServer.Persist(SPCmds.RESETSTOP, no1))
                //    {
                //        Logger.Info($"Persisted RESETSTOP {omd.ToString()}");
                //        cbsend(new orderMaster(omd).ToArray());
                //    }

                //    break;
                case eCommand.OrderOptions:
                    throw new Exception("This should be handled by the Receive Message handler.");
                case eCommand.OrderDetails:
                    Logger.Info($"HandleClientCmd - OrderDetails:{cmd.ToString()}");

                    var odt = (orderDetails)new orderDetails().FromArray(bytes_cmd);
                    
                    OrderDetailsData odd = (OrderDetailsData)UMDServer.QueryData(cbsend, odt);
                    Logger.Info($"API Manager GetOrderDetailsData. {odd.ToString()}");
                    break;

                case eCommand.UploadManifest:
                    Logger.Info($"HandleClientCmd - UploadManifest:{cmd.ToString()}");

                    mreq = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    Logger.Info($"HandleClientCmd - UploadManifest:  {mreq.ToString()}");

                    manifestMaster mm = (manifestMaster)new manifestMaster().FromArray(mreq.bData);

                    dRetCall.Add(NewGuid(mm.requestId), cbsend);
                    Logger.Info($"UploadManifest cb dRetCall:  {mm.ToString()}");

                    try
                    {
                        foreach (ManifestMasterData mmdit in UMDServer.Persist(SPCmds.INSERTMANIFEST, new ManifestMasterData(mm, mm.id)))
                        {
                            Logger.Info($"HandleClientCmd - UploadManifest Persisted:{mmdit.ToString()}");
                            Logger.Info($"UploadManifest - Get ManifestDetails: {mmdit.ToString()}");
                            drillDown.GetManifestDetails(mmdit, cbsend);
                        }
                        mm.command = eCommand.UploadManifestComplete;
                        Logger.Info($"UploadManifest - ManifestLoadComplete: {mm.ToString()}");
                        cbsend(mm.ToArray());
                    }
                    catch (Exception e)
                    {
                        Logger.Debug("HandleClientCmd - Error exception = " + e.Message);
                        dRetCall.Remove(NewGuid(mm.requestId));
                    }
                    break;
                case eCommand.Trucks:
                    isaCommand req = new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - Trucks: {req.ToString()}");
                    TruckData td = (TruckData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"HandleClientCmd - Trucks:  {td.ToString()}");
                    break;
                case eCommand.Drivers:
                    req = new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - Drivers: {req.ToString()}");
                    DriverData dd = (DriverData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"HandleClientCmd - Drivers:  {dd.ToString()}");
                    break;
                case eCommand.Stops:
                    req = new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd Stops:  {req.ToString()}");
                    StopData sd = (StopData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"Stops QueryData:  {sd.ToString()}");
                    break;
                case eCommand.OrdersLoad:
                    req = new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - OrdersLoad (Start QueryData): {req.ToString()}");
                    OrderData od = (OrderData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"OrdersLoad QueryData: {od.ToString()}");
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
