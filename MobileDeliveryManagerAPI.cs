using MobileDeliveryGeneral.DataManager.Interfaces;
using System;
using MobileDeliveryGeneral.Data;
using static MobileDeliveryGeneral.Definitions.MsgTypes;
using MobileDeliveryGeneral.Interfaces.DataInterfaces;
using MobileDeliveryManager.UnitedMobileData;
using MobileDeliveryGeneral.Interfaces;
using System.Threading.Tasks;
using static MobileDeliveryGeneral.Definitions.enums;
using System.Collections.Generic;
using MobileDeliveryGeneral.Settings;
using MobileDeliveryClient.API;
using MobileDeliveryGeneral.Utilities;
using MobileDeliveryLogger;
using MobileDeliveryManger.UnitedMobileData;
using System.Linq;
using MobileDeliverySettings.Settings;
using MobileDeliveryGeneral.ExtMethods;

namespace MobileDeliveryManager
{
    public class MobileDeliveryManagerAPI : isaMDM
    {
        UMDManifest UMDServer;
        public static string AppName;
        public ProcessMsgDelegateRXRaw pmRx { get; private set; }
        ReceiveMsgDelegate rm;
        SendMsgDelegate sm;
        ClientToServerConnection conn;
        ManifestDetails drillDown;
        Dictionary<string, List<ManifestDetailsData>> dManDetails = new Dictionary<string, List<ManifestDetailsData>>();
        Dictionary<string, List<OrderMasterData>> dOrdersMaster = new Dictionary<string, List<OrderMasterData>>();
        Dictionary<string, List<OrderModelData>> dOrdersModel = new Dictionary<string, List<OrderModelData>>();
        Dictionary<Guid, List<OrderDetailsData>> dOrdersDetails = new Dictionary<Guid, List<OrderDetailsData>>();
        object dOrdLock = new object();

        public MobileDeliveryManagerAPI()
        {}

        public void Init(UMDAppConfig config, ev_name_hook ev)
        {
            UMDServer = new UMDManifest(config.SQLConn);
            drillDown = new ManifestDetails(sm, rm, pmRx);
            ConnectToWinsys(config, ev);
            Console.Title = $"{AppName}";
        }
        void ConnectToWinsys(UMDAppConfig config, ev_name_hook e) {
            rm = new ReceiveMsgDelegate(ReceiveMessage);
            pmRx = new ProcessMsgDelegateRXRaw(HandleClientCmd);
            AppName = config.AppName;
            Logger.Debug($"{config.AppName} Connection init");

            if (config.srvSet == null)
            {
                Logger.Error($"{config.AppName} Missing Configuration Server Settings");
                throw new Exception($"{config.AppName} Missing Configuration Server Settings.");
            }

            //Connecting to the WinsysAPI
            config.srvSet.url = config.srvSet.clienturl;
            config.srvSet.port = config.srvSet.clientport;
            config.srvSet.name += " as a client To WinSys server.";
            conn = new ClientToServerConnection(config.srvSet, ref sm, rm, ev);
            conn.Connect();
        }
        Dictionary<Guid, Func<byte[], Task>> dRetCall = new Dictionary<Guid, Func<byte[], Task>>();
        public isaCommand ReceiveMessage(isaCommand cmd)
        {
            switch (cmd.command)
            {
                case eCommand.Ping:
                    Logger.Debug($"ReceiveMessage - Received Ping / Replying Pong..");
                    sm(new Command() { command = eCommand.Pong });
                    break;
                case eCommand.Pong:
                    Logger.Debug($"ReceiveMessage - Received Pong");
                    break;
                case eCommand.LoadFiles:
                    Logger.Info($"ReceiveMessage - Copy Files from Winsys Server Paths top App Server Paths:{cmd.ToString()}");
                    //CopyFilesToServer(DateTime.Today);
                    Logger.Info($"ReceiveMessage - Replying LoadFilesComplete...");
                    sm(new Command() { command = eCommand.LoadFilesComplete });
                    //cbsend(new Command() { command = eCommand.LoadFilesComplete }.ToArray());
                    break;
                case eCommand.GenerateManifest:
                    Logger.Info($"ReceiveMessage - Generate Manifest from Winsys and SqlServer:{cmd.ToString()}");
                    manifestRequest req = (manifestRequest)cmd;
                    sm(req);
                    break;
                case eCommand.Manifest:
                    Logger.Info($"ReceiveMessage - Manifest from Winsys: {cmd.ToString()}");
                    manifestMaster manMst = (manifestMaster)cmd;

                    ManifestMasterData manMsData = new ManifestMasterData(manMst,manMst.id);
                    Logger.Info($"Get Manifest (Query Wimsys, Forward result to requesting client): {manMsData.ToString()}");
                    dRetCall[NewGuid(cmd.requestId)](manMst.ToArray());

                    Logger.Info($"Manifest Load Complete: {manMst.ToString()}");
                    break;
                case eCommand.ManifestLoadComplete:
                    Logger.Info($"ReceiveMessage - OrdersLoadComplete:{cmd.ToString()}");
                    Logger.Info($"ReceiveMessage - OrderDetailsComplete:{cmd.ToString()}");

                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    //sm(req);
                    break;
                case eCommand.ManifestDetails:
                    Logger.Info($"ReceiveMessage - Generate Manifest Details from Winsys - API Drill Down:{cmd.ToString()}");
                    manifestDetails manDet = (manifestDetails)cmd;

                    ManifestDetailsData manDetData = new ManifestDetailsData(manDet);
                    Logger.Info($"INSERTMANIFESTDETAILS (Persist): {manDetData.ToString()}");

                    if(!dManDetails.ContainsKey(manDetData.RequestId.ToString() + manDetData.ManId.ToString()))
                        dManDetails.Add(manDetData.RequestId.ToString() + manDetData.ManId.ToString(), new List<ManifestDetailsData>());

                    foreach (var omdit in UMDServer.Persist(SPCmds.INSERTMANIFESTDETAILS, manDetData))
                        dManDetails[manDetData.RequestId.ToString() + manDetData.ManId.ToString()].Add(manDetData);

                    Logger.Info($"ManifestDetails Complete: {manDet.ToString()}");
                    break;
                    
                case eCommand.ManifestDetailsComplete:
                    Logger.Info($"ReceiveMessage - ManifestDetailsComplete:{cmd.ToString()}");
                    req = (manifestRequest)cmd;
                    List<ManifestDetailsData> mdd=null;
                    if (dManDetails.ContainsKey(NewGuid(req.requestId).ToString()+req.id.ToString()))
                    {
                        mdd = dManDetails[NewGuid(req.requestId).ToString() + req.id.ToString()];
                        dManDetails.Remove(NewGuid(req.requestId).ToString()+req.id.ToString());
                    }
                    if (!dManDetails.ContainsKey(NewGuid(req.requestId).ToString()) && mdd !=null  && mdd.Count >0) 
                        drillDown.GetOrderMasterData(new manifestRequest() { valist = req.valist, requestId = req.requestId, date=req.date, id=req.id });

                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    //drillDown.reportMDProgressChanged(100, cmd);
                    break;
                case eCommand.OrdersUpload:
                    Logger.Info($"ReceiveMessage - Orders  reqId: {cmd.requestId}");
                    List<IMDMMessage> lstOrd = new List<IMDMMessage>();

                    var omd = new OrderMasterData((orders)cmd);
                    orderMaster om = new orderMaster(omd);
                    //omd.Status = OrderStatus.Shipped;

                    lock (dOrdLock)
                    {
                        if (!dOrdersMaster.ContainsKey(omd.RequestId.ToString() + omd.ManId.ToString()))
                            dOrdersMaster.Add(omd.RequestId.ToString() + omd.ManId.ToString(), new List<OrderMasterData>());
                    }
                    if (!dOrdersMaster[omd.RequestId.ToString() + omd.ManId.ToString()].Contains(omd))
                        foreach (var omdit in UMDServer.Persist(SPCmds.INSERTORDER, omd))
                            lock (dOrdLock)
                                dOrdersMaster[omd.RequestId.ToString() + omd.ManId.ToString()].Add((OrderMasterData)omdit);

                    break;
                case eCommand.OrderModel:
                    Logger.Info($"ReceiveMessage - OrderModel  reqId: {cmd.requestId}");
                    orders ords = (orders)cmd;
                    OrderModelData omda = new OrderModelData(ords);

                    lock (dOrdLock)
                    {
                        if (!dOrdersModel.ContainsKey(NewGuid(ords.requestId).ToString() + ords.ManifestId.ToString()))
                            dOrdersModel.Add(NewGuid(ords.requestId).ToString() + ords.ManifestId.ToString(), new List<OrderModelData>());
                    }
                    if (!dOrdersModel[omda.RequestId.ToString() + ords.ManifestId.ToString()].Contains(omda))
                    {
                        foreach (var omdit in UMDServer.Persist(SPCmds.INSERTORDER, omda))
                        {
                            lock (dOrdLock)
                                dOrdersModel[omda.RequestId.ToString() + ords.ManifestId.ToString()].Add((OrderModelData)omdit);
                        }
                    }
                    dRetCall[NewGuid(cmd.requestId)](ords.ToArray());
                    break;
                    
                case eCommand.OrdersLoad:
                    Logger.Info($"ReceiveMessage - OrdersLoad  reqId: {cmd.requestId}");
                    var ord = (orders)cmd;
                    OrderMasterData omdata = new OrderMasterData(ord);
                    
                    lock (dOrdLock)
                    {
                        if (!dOrdersMaster.ContainsKey(NewGuid(ord.requestId).ToString() + ord.ManifestId.ToString()))
                            dOrdersMaster.Add(NewGuid(ord.requestId).ToString() + ord.ManifestId.ToString(), new List<OrderMasterData>());
                    }
                    if (!dOrdersMaster[omdata.RequestId.ToString() + ord.ManifestId.ToString()].Contains(omdata))
                    {
                        foreach (var omdit in UMDServer.Persist(SPCmds.INSERTORDER, omdata))
                        {
                            lock (dOrdLock)
                                dOrdersMaster[omdata.RequestId.ToString() + ord.ManifestId.ToString()].Add((OrderMasterData)omdit);
                        }
                    }
                    dRetCall[NewGuid(cmd.requestId)](ord.ToArray());
                    break;
                case eCommand.OrderModelLoadComplete:
                    Logger.Info($"ReceiveMessage - OrderModelLoadComplete: {cmd.ToString()}");
                    req = (manifestRequest)cmd;

                    if (dOrdersModel.ContainsKey(NewGuid(cmd.requestId).ToString() + req.id))
                    {
                        var dt = dOrdersModel.FirstOrDefault().Value[0].SHP_DTE;
                        var dts = dOrdersModel.Select(sd => sd.Value.Select(v => v.SHP_DTE));
                        foreach (var itm in dts)
                            Logger.Debug($"{itm}");
                        List<OrderModelData> lMOrd = dOrdersModel[NewGuid(cmd.requestId).ToString() + req.id.ToString()].Distinct().ToList();
                        req.valist = new List<long>();
                        req.valist.AddRange(lMOrd.Select(o => (long)o.ORD_NO).ToList());
                        lMOrd.ForEach(x => Logger.Info($"ordldcmp{x.ORD_NO}"));
                        drillDown.GetDrillDownData(req.valist, eCommand.OrderDetails, req.requestId, 1);
                        drillDown.GetDrillDownData(req.valist, eCommand.OrderOptions, req.requestId);
                        drillDown.GetDrillDownData(req.valist, new manifestRequest()
                        {
                            command = eCommand.ScanFile,
                            requestId = req.requestId,
                            id = req.id,
                            valist = req.valist
                        }, 3);
                        drillDown.GetDrillDownData(req.valist, eCommand.ScanFile, req.requestId, 3);
                        
                        lock (dOrdLock)
                            dOrdersModel.Remove(NewGuid(cmd.requestId).ToString() + req.id);
                    }
                    else
                        Logger.Info($"No Orders for {cmd.ToString()}");
                    //                        throw new Exception("OrdersLoadComplete - response not mapped in dOrderMaster.  " +
                    //                          "Request Id: {NewGuid(cmd.requestId).ToString()} , id: {req.id}. ");
                    if (dRetCall.ContainsKey(NewGuid(cmd.requestId)))
                        dRetCall[NewGuid(cmd.requestId)](req.ToArray());

                    break;
                case eCommand.OrdersLoadComplete:
                    Logger.Info($"ReceiveMessage - OrdersLoadComplete:{cmd.ToString()}");
                    req = (manifestRequest)cmd;

                    if (dOrdersMaster.ContainsKey(NewGuid(cmd.requestId).ToString() + req.id))
                    {
                        var dt = dOrdersMaster.FirstOrDefault().Value[0].SHP_DTE;
                        var dts = dOrdersMaster.Select(sd => sd.Value.Select(v => v.SHP_DTE));
                        foreach (var itm in dts)
                            Logger.Debug($"{itm}");
                        List<OrderMasterData> lMOrd = dOrdersMaster[NewGuid(cmd.requestId).ToString() + req.id.ToString()].Distinct().ToList();
                        req.valist = new List<long>();
                        req.valist.AddRange(lMOrd.Select(o => (long)o.ORD_NO).ToList());
                        lMOrd.ForEach(x => Logger.Info($"ordldcmp{x.ORD_NO}"));
                        drillDown.GetDrillDownData(req.valist, eCommand.OrderDetails, req.requestId, 1);
                        drillDown.GetDrillDownData(req.valist, eCommand.OrderOptions, req.requestId);
                        drillDown.GetDrillDownData(req.valist, new manifestRequest()
                        {
                            command = eCommand.ScanFile,
                            requestId = req.requestId,
                            id = req.id,
                            valist = req.valist
                        }, 3);
                        //drillDown.GetDrillDownData(req.valist, eCommand.ScanFile, req.requestId, 3);
                        //drillDown.GetDrillDownScanFile( new ManifestMasterData() { RequestId = NewGuid(req.requestId), SHIP_DTE = ExtensionMethods.FromJulianToGregorianDT(dt, "yyyy-MM-dd").Date });
                        lock (dOrdLock)
                            dOrdersMaster.Remove(NewGuid(cmd.requestId).ToString() + req.id);
                    }
                    else
                        Logger.Info($"No Orders for {cmd.ToString()}");
                    //                        throw new Exception("OrdersLoadComplete - response not mapped in dOrderMaster.  " +
                    //                          "Request Id: {NewGuid(cmd.requestId).ToString()} , id: {req.id}. ");
                    if (dRetCall.ContainsKey(NewGuid(cmd.requestId)))
                        dRetCall[NewGuid(cmd.requestId)](req.ToArray());

                    break;     
                    
                case eCommand.OrderDetails:
                    Logger.Info($"ReceiveMessage - OrderDetails:{cmd.ToString()}");
                    try
                    {
                        orderDetails od = (orderDetails)cmd;
                        OrderDetailsData odd = new OrderDetailsData(od);

                        lock (dOrdLock)
                        {
                            if (!dOrdersDetails.ContainsKey(odd.RequestId))
                                dOrdersDetails.Add(odd.RequestId, new List<OrderDetailsData>());
                        }
                        foreach (var odit in UMDServer.Persist(SPCmds.INSERTORDERDETAILS, odd))
                        {


                            Logger.Info($"INSERTORDERDETAILS Complete: {odit.ToString()}");
                        }

                        //dRetCall[NewGuid(od.requestId)](cmd.ToArray());
                    }
                    catch (Exception ex) { Logger.Error($"eCommand.OrderDetails {ex.Message}"); }

                    break;

                case eCommand.OrderDetailsComplete:
                    Logger.Info($"ReceiveMessage - OrderDetailsComplete:{cmd.ToString()}");
                    // Can we detrmine the completed transaction at this point from the cmd in order to clean the completed tx for dRetCall
                    req = (manifestRequest)cmd;

                    lock (dOrdLock)
                    {
                        if (dOrdersDetails.ContainsKey(NewGuid(cmd.requestId)))
                        {
                            //                        drillDown.GetOrderOptionsData(dOrdersDetails[NewGuid(cmd.requestId)].Distinct().ToList());
                            //drillDown.GetOrderOptionsData(ordersToRequestDetails[NewGuid(cmd.requestId)]);
                            dOrdersDetails.Remove(NewGuid(cmd.requestId));
                        }
                    }

                    //dRetCall[NewGuid(cmd.requestId)](req.ToArray());

                    break;

                case eCommand.OrderOptions:
                    Logger.Info($"ReceiveMessage - OrderOptions  reqId:{cmd.ToString()}");
                    try
                    {
                        orderOptions oo = (orderOptions)cmd;
                        OrderOptionsData ood = new OrderOptionsData(oo);

                        foreach (var oodit in UMDServer.Persist(SPCmds.INSERTORDEROPTIONS, ood))
                            Logger.Info($"INSERTORDEROPTIONS Complete: {oodit.ToString()}");

                        //oo.command = eCommand.OrderOptionsComplete;
                        //dRetCall[NewGuid(oo.requestId)](cmd.ToArray());
                    }
                    catch (Exception ex) { Logger.Error($"eCommand.OrderOptions {ex.Message}"); }

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
                case eCommand.StopsLoadComplete:
                    Logger.Info($"ReceiveMessage - StopsLoadComplete:{cmd.ToString()}");

                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    break;

                case eCommand.ScanFile:
                    Logger.Info($"ReceiveMessage Cached Success - ScanFile:{cmd.ToString()}");
                    scanFile sf = (scanFile)cmd;
                    ScanFileData sfd = new ScanFileData(sf);

                    foreach (var scnfle in UMDServer.Persist(SPCmds.INSERTSCANFILE, sfd))
                        Logger.Info($"INSERTSCNFLE Complete: {scnfle.ToString()}");

                    dRetCall[NewGuid(cmd.requestId)](sf.ToArray());

                    //if(cmd.requestId==null)
                    //    dRetCall.FirstOrDefault().Value(sf.ToArray());
                    //else
                    //    dRetCall[NewGuid(cmd.requestId)](sf.ToArray());

                    break;

                case eCommand.ScanFileComplete:
                    Logger.Info($"ReceiveMessage - ScanFileComplete:{cmd.ToString()}");

                    req = (manifestRequest)cmd;
                    dRetCall[NewGuid(cmd.requestId)](req.ToArray());
                    break;

                default:
                    Logger.Error($"ReceiveMessage - ERROR Unknown command.  Parse Error MDM-API {cmd.command}");
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
                        dRetCall.Add(NewGuid(mM.requestId), cbsend);
                        sm(mM);
                    }

                    break;

                case eCommand.CheckManifest:
                    Logger.Info($"HandleClientCmd - CheckManifest: {cmd.ToString()}");
                    manifestMaster mMst = (manifestMaster)new manifestMaster().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd Check Manifest in ManagerAPI/SqlServer: {mMst.ToString()}");
                    ManifestMasterData mamd = (ManifestMasterData)UMDServer.QueryData(cbsend, mMst);
                    Logger.Info($"API Manager Check Manifest. {mamd.ToString()}");

                    break;


                case eCommand.Manifest:
                    Logger.Info($"HandleClientCmd - Manifest: {cmd.ToString()}");
                    cbsend(cmd.ToArray());

                    break;

                case eCommand.OrdersUpload:
                    Logger.Info($"HandleClientCmd - Orders: {cmd.ToString()}");
                    orderMaster om = (orderMaster)new orderMaster().FromArray(bytes_cmd);
                    List<OrderMasterData> ordList = new List<OrderMasterData>();
                    Logger.Info($"Persist INSERTORDER {om.ToString()}");
//                    om.Status = OrderStatus.Shipped;
                    foreach (OrderMasterData omd in UMDServer.Persist(SPCmds.INSERTORDER, new OrderMasterData(om)))
                    {
                        ordList.Add(omd);
                        Logger.Info($"Orders - drillDown.GetOrderDetailsData: {omd.ToString()}");      
                        omd.Command = eCommand.OrdersLoadComplete;
                    }

                    //foreach (var ord in ordList)
                    //{
                    //    drillDown.GetOrderDetailsData(ord);
                    //    drillDown.GetOrderOptionsData(ord);
                    //}
                    drillDown.GetOrderDetailsData(ordList);
                    drillDown.GetOrderOptionsData(ordList);

                    Logger.Info($"INSERTORDER Complete. OrdersLoadComplete: {om.ToString()}");
                    om.command = eCommand.ManifestDetailsComplete;
                    cbsend(om.ToArray());

                    break;

                case eCommand.CompleteOrder:
                    Logger.Info($"HandleClientCmd - Close Order: {cmd.ToString()}");

                    manifestRequest mreq = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    Logger.Info($"HandleClientCmd - CompleteOrder:  {mreq.ToString()}");

                    orderMaster cs = (orderMaster)new orderMaster().FromArray(mreq.bData);

                    Logger.Info($"Persist COMPLETEORDER {cs.ToString()}");

                    var no = new OrderMasterData(cs);
                    foreach (OrderMasterData omd in UMDServer.Persist(SPCmds.COMPLETEORDER, no))
                    {
                        Logger.Info($"Persisted COMPLETEORDER {omd.ToString()}");
                        cbsend(new orderMaster(omd).ToArray());
                    }

                    break;

                case eCommand.CompleteStop:

                    Logger.Info($"HandleClientCmd - Close Stop for Orders: {cmd.ToString()}");

                    manifestRequest mreqs = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    Logger.Info($"HandleClientCmd - CompleteStop:  {mreqs.ToString()}");

                    StopData sd = new StopData();
                    sd.POD = mreqs.bData;
                    sd.ManifestId = mreqs.id;
                    sd.DisplaySeq = (int)mreqs.DATA;
                    sd.Orders = new List<OrderMasterData>();
                   //temp - uncomment!
                    // mreqs.valist.ForEach(v => sd.Orders.Add(new OrderMasterData() { DSP_SEQ=sd.DisplaySeq, ManifestId=sd.ManifestId, ORD_NO = v }));

                    Logger.Info($"Persist COMPLETESTOP {sd.ToString()}");

                    foreach (StopData omd in UMDServer.Persist(SPCmds.COMPLETESTOP, sd))
                    {
                        Logger.Info($"Persisted COMPLETESTOP {sd.ToString()}");
                        cbsend(new stops(sd).ToArray());
                    }

                    break;

                case eCommand.OrderOptions:

                    //throw new Exception("This should be handled by the Receive Message handler.");
                    Logger.Info($"HandleClientCmd - OrderOptions:{cmd.ToString()}");
                    manifestRequest mreqoo = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    OrderOptionsData ood = (OrderOptionsData)UMDServer.QueryData(cbsend, mreqoo);
                    Logger.Info($"API Manager QueryData OrderOptionsData. {ood.ToString()}");
                    cbsend(cmd.ToArray());
                    break;

                case eCommand.OrderDetails:

                    Logger.Info($"HandleClientCmd - OrderDetails:{cmd.ToString()}");
                    manifestRequest mreqod = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    OrderDetailsData odd = (OrderDetailsData)UMDServer.QueryData(cbsend, mreqod);
                    Logger.Info($"API Manager OrderDetailsData QueryData. {odd.ToString()}");
                    break;

                case eCommand.ScanFile:
                    Logger.Info($"HandleClientCmd - Get SQL Server ScanFile:{cmd.ToString()}");
                    manifestRequest mreqsf = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);
                    ScanFileData sfd = (ScanFileData)UMDServer.QueryData(cbsend, mreqsf);
                    Logger.Info($"HandleClientCmd - Query SCNFLE Complete: {sfd.ToString()}");
                    break;

                case eCommand.UploadManifest:

                    Logger.Info($"HandleClientCmd - UploadManifest:{cmd.ToString()}");

                    mreq = (manifestRequest)new manifestRequest().FromArray(bytes_cmd);

                    Logger.Info($"HandleClientCmd - UploadManifest:  {mreq.ToString()}");

                    manifestMaster mm = (manifestMaster)new manifestMaster().FromArray(mreq.bData);

                    if (!dRetCall.ContainsKey(NewGuid(mm.requestId)))
                        dRetCall.Add(NewGuid(mm.requestId), cbsend);

                    Logger.Info($"UploadManifest cb dRetCall:  {mm.ToString()}");

                    try
                    {
                        foreach (ManifestMasterData mmdit in UMDServer.Persist(SPCmds.INSERTMANIFEST, new ManifestMasterData(mm, mm.id)))
                        {
                            Logger.Info($"HandleClientCmd - UploadManifest Persisted:{mmdit.ToString()}");
                            Logger.Info($"UploadManifest - Get ManifestDetails: {mmdit.ToString()}");

                            if (!dManDetails.ContainsKey(mmdit.RequestId.ToString() + mmdit.ManifestId.ToString()))
                                dManDetails.Add(mmdit.RequestId.ToString() + mmdit.ManifestId.ToString(), new List<ManifestDetailsData>());

                            drillDown.GetManifestDetails(mmdit);
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
                    StopData sdt = (StopData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"Stops QueryData:  {sdt.ToString()}");
                    break;
                case eCommand.OrdersLoad:
                    req = new manifestRequest().FromArray(bytes_cmd);
                    Logger.Info($"HandleClientCmd - OrdersLoad (Start QueryData): {req.ToString()}");
                    OrderMasterData od = (OrderMasterData)UMDServer.QueryData(cbsend, req);
                    Logger.Info($"OrdersLoad QueryData: {od.ToString()}");
                    break;
                case eCommand.AccountReceivable:
                    req = new manifestRequest().FromArray(bytes_cmd);
                    manifestRequest mr = (manifestRequest)req;
                    accountReceivable arecv = (accountReceivable)new accountReceivable().FromArray(mr.bData);                    Logger.Info($"HandleClientCmd - AccountRecievable");
                    AccountsReceivableData ard = (AccountsReceivableData)
                        UMDServer.QueryData(cbsend, arecv);
                    Logger.Info($"AccountsReceivables Complete.");
                    break;
                default:
                    Logger.Error("HandleClientCmd - ERROR Unknown command.  Parse Error MDM-API");
                    break;
            }
        }
    }
}

