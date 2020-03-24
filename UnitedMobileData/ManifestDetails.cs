using MobileDeliveryLogger;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using UMDGeneral.Data;
using UMDGeneral.Interfaces;
using static UMDGeneral.Definitions.MsgTypes;

namespace MobileDeliveryManger.UnitedMobileData
{
    public class ManifestDetails
    {
        #region fields
        DateTime dt;
        SendMsgDelegate sm;
        ReceiveMsgDelegate rm;
        #endregion

        #region collections
        public List<ManifestDetailsData> manifestDetailData = new List<ManifestDetailsData>();
        public List<OrderMasterData> orderMasterData = new List<OrderMasterData>();
        public List<OrderDetailsData> orderDetailData = new List<OrderDetailsData>();
        public List<OrderOptionsData> orderOptionsData = new List<OrderOptionsData>();
        #endregion

        public ManifestDetails (SendMsgDelegate SendMessage, ReceiveMsgDelegate ReceiveMessage, ProcessMsgDelegateRXRaw pm)
        {
            sm = SendMessage;
            rm = ReceiveMessage;
        }

        private void ProcessMessage(ManifestMasterData inp, Func<byte[], Task> cbsend)
        {
            throw new NotImplementedException();
        }
        public void GetTruckData(ManifestDetailsData mdd)
        {
            var req = new manifestRequest()
            {
                command = eCommand.Trucks,
                requestId = mdd.RequestId.ToByteArray(),
                valist = new List<long>() { mdd.DLR_NO }
            };
            sm(req);
        }
        public void GetManifestDetails(ManifestMasterData manMaster, Func<byte[], Task> cbsend)
        {
            dt = manMaster.SHIP_DTE;
            var req = new manifestRequest()
            {
                command = eCommand.ManifestDetails,
                requestId = manMaster.RequestId.ToByteArray(),
                id = manMaster.ManifestId, valist = new List<long>() { manMaster.LINK }
            };
            Logger.Info($"Upload Manifest - GetManifestDetails DrillDown/n sending manifestRequest/n/t{req.command.ToString()}" +
               $"/n/tmanId:{req.id} reqId: {req.requestId}");

            sm(req);
        }
        public void GetOrderMasterData(ManifestDetailsData mdd)
        {
            var req = new manifestRequest()
            {
                command = eCommand.Orders,
                requestId = mdd.RequestId.ToByteArray(),
                valist = new List<long>() { mdd.DLR_NO }
            };
            sm(req);
        }
        public void GetOrderDetailsData(OrderMasterData omd)
        {
            var req = new manifestRequest()
            {
                command = eCommand.OrderDetails,
                requestId = omd.RequestId.ToByteArray(),
                valist = new List<long>() { omd.ORD_NO }
            };
            sm(req);
        }
        public void GetOrderOptionsData(OrderMasterData ood)
        {
            var req = new manifestRequest()
            {
                command = eCommand.OrderOptions,
                requestId = ood.RequestId.ToByteArray(),
                valist = new List<long>() { ood.ORD_NO }
            };
            sm(req);
        }
    }
}
