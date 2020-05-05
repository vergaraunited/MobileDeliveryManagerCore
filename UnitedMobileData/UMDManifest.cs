using MobileDataManager.UnitedMobileData;
using MobileDeliveryLogger;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MobileDeliveryGeneral.Data;
using MobileDeliveryGeneral.Interfaces;
using MobileDeliveryGeneral.Interfaces.DataInterfaces;
using MobileDeliveryGeneral.Threading;
using static MobileDeliveryGeneral.Definitions.enums;
using static MobileDeliveryGeneral.Definitions.MsgTypes;

namespace MobileDeliveryManager.UnitedMobileData
{
    public class UMDManifest {
        static UMDDB UMD_Data;
        string cnn;
        DateTime dt;
        SendMsgDelegate sm;
        ReceiveMsgDelegate rm;

        #region collections
      //  public List<ManifestMasterData> manifestMasterData = new List<ManifestMasterData>();
      ////  public List<ManifestDetailsData> manifestDetailData = new List<ManifestDetailsData>();
      //  public List<OrderMasterData> orderMasterData = new List<OrderMasterData>();
      //  public List<OrderDetailsData> orderDetailData = new List<OrderDetailsData>();    
      //  public List<OrderOptionsData> orderOptionsData = new List<OrderOptionsData>();
        #endregion

        #region backgroundworkers
        UMBackgroundWorker<ManifestMasterData> manifestMaster;
        UMBackgroundWorker<ManifestDetailsData> manifestDetails;
        UMBackgroundWorker<OrderMasterData> orderMaster;
        UMBackgroundWorker<OrderDetailsData> orderDetails;
        UMBackgroundWorker<OrderOptionsData> orderOptions;
        #endregion

        #region progressupdates
        UMBackgroundWorker<ManifestMasterData>.ProgressChanged<ManifestMasterData> pcManifest;
        UMBackgroundWorker<ManifestDetailsData>.ProgressChanged<ManifestDetailsData> pcManifestDetails;
        UMBackgroundWorker<OrderMasterData>.ProgressChanged<OrderMasterData> pcOrders;
        UMBackgroundWorker<OrderDetailsData>.ProgressChanged<OrderDetailsData> pcOrderDetails;
        UMBackgroundWorker<OrderOptionsData>.ProgressChanged<OrderOptionsData> pcOrderOptions;
        #endregion

        public UMDManifest(string con)
        {
            Logger.Info($"UMDManifest::SQLConn: {con}");
            cnn = con;
            UMD_Data = new UMDDB(cnn);
        }
        public IMDMMessage QueryData(Func<byte[], Task> cb, isaCommand dat)
        {
            return UMD_Data.QueryData(cb, dat);
        }

        public IEnumerable<IMDMMessage> Persist(SPCmds sp, IMDMMessage md)
        {
            Logger.Info($"Persist {md.ToString()}");
            foreach (var it in UMD_Data.InsertData(sp, md))
                yield return it;
        }
    }
}
