using MobileDeliveryLogger;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MobileDeliveryGeneral.Data;
using MobileDeliveryGeneral.Interfaces;
using static MobileDeliveryGeneral.Definitions.MsgTypes;
using System.Linq;
using MobileDeliveryGeneral.ExtMethods;

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
        public void GetManifestDetails(ManifestMasterData manMaster)
        {
            dt = manMaster.SHIP_DTE;
            var req = new manifestRequest()
            {
                command = eCommand.ManifestDetails,
                requestId = manMaster.RequestId.ToByteArray(),
                id = manMaster.ManifestId,
                date = dt.ToString("yyyy-MM-dd"),
                valist = new List<long>() { manMaster.LINK }
            };
            Logger.Info($"Upload Manifest - GetManifestDetails DrillDown/n sending manifestRequest/n/t{req.command.ToString()}" +
               $"/n/tmanId:{req.id} reqId: {req.requestId}");

            sm(req);
        }
        //public void GetOrderMasterData(List<ManifestDetailsData> mddLst)
        //public void GetOrderMasterData(ManifestMasterData mdd)
        //{
        //    var req = new manifestRequest()
        //    {
        //        command = eCommand.Orders,
        //        requestId = mdd.RequestId.ToByteArray(),
        //        id=mdd.ManifestId
        //    };
        //    sm(req);
        //}

        public void GetOrderMasterData(manifestRequest mr)
        {
            mr.command = eCommand.OrdersLoad;
            sm(mr);
        }

        public void GetOrderMasterData(List<long> dlrnums, byte [] reqid)
        {
            GetOrderMasterData(new manifestRequest()
            {
                command = eCommand.OrdersLoad,
                requestId = reqid, 
                valist = dlrnums
            });
        }

        public void GetOrderMasterData(ManifestMasterData mmd)
        {
            var req = new manifestRequest()
            {
                command = eCommand.OrdersLoad,
                requestId = mmd.RequestId.ToByteArray(),
                id = mmd.ManifestId,
                date = mmd.SHIP_DTE.ToString("yyyy-MM-dd")
            };
            sm(req);
        }

        public void GetOrderDetailsData(List<OrderMasterData> omd)
        {
            int prevTake = 0;
            int batchCount = 10;
            if (omd.Count < batchCount)
                batchCount = omd.Count;

            while (prevTake < omd.Count)
            {
                sm(new manifestRequest()
                {
                    command = eCommand.OrderDetails,
                    requestId = omd[0].RequestId.ToByteArray(),
                    valist = new List<long>(omd.OrderByDescending(b => b.ORD_NO).Distinct().Skip(prevTake).Take(batchCount).ToList().Select(a => (long)a.ORD_NO))
                });
                prevTake += batchCount;
            }
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

        public void GetOrderOptionsData(OrderDetailsData ood)
        {
            var req = new manifestRequest()
            {
                command = eCommand.OrderOptions,
                requestId = ood.RequestId.ToByteArray(),
                valist = new List<long>() { ood.ORD_NO }
            };
            sm(req);
        }
        public void GetOrderOptionsData(List<OrderMasterData> ood)
        {
            int prevTake = 0;
            int batchCount = 6;
            if (ood.Count < batchCount)
                batchCount = ood.Count;

            while (prevTake < ood.Count)
            {
                sm(new manifestRequest()
                {
                    command = eCommand.OrderOptions,
                    requestId = ood[0].RequestId.ToByteArray(),
                    valist = new List<long>(ood.OrderByDescending(b => b.ORD_NO).Distinct().Skip(prevTake).Take(batchCount).ToList().Select(a => (long)a.ORD_NO))
                });
                prevTake += batchCount;
            }
        }

        public void GetOrderOptionsData(List<OrderDetailsData> ood)
        {

            int prevTake = 0;
            int batchCount = 2;
            if (ood.Count < batchCount)
                batchCount = ood.Count;

            while (prevTake < ood.Count)
            {
                sm(new manifestRequest()
                {
                    command = eCommand.OrderOptions,
                    requestId = ood[0].RequestId.ToByteArray(),
                    valist = new List<long>(ood.OrderByDescending(b => b.ORD_NO).Distinct().Skip(prevTake).Take(batchCount).ToList().Select(a => a.ORD_NO))
                });
                prevTake += batchCount;
            } 
        }
        public void GetDrillDownScanFile(ManifestMasterData mmd)
        {
            var req = new manifestRequest()
            {
                command = eCommand.ScanFile,
                requestId = mmd.RequestId.ToByteArray(),
                id = mmd.ManifestId,
                date = mmd.SHIP_DTE.ToString("yyyy-MM-dd")
            };
            sm(req);
        }

        public void GetDrillDownData(List<long> ids, manifestRequest req, int nSplitSize = 0)
        {
            // SpliList limits the number of order ids in the SQL query condition.  Defaults to 30 (_nSize).
            foreach (var reqListIds in ids.SplitList(nSplitSize))
            {
                sm(new manifestRequest()
                {
                    command = req.command,
                    requestId = req.requestId,
                    id = req.id,
                    valist = reqListIds
                });
            }
        }

        public void GetDrillDownData(List<long> ids, eCommand cmd, byte[] reqId, int nSplitSize=0)
        {
            manifestRequest mr = new manifestRequest()
            {
                command = cmd,
                requestId = reqId,
                valist = ids
            };

            GetDrillDownData(ids, mr, nSplitSize);
        }
    }
}
