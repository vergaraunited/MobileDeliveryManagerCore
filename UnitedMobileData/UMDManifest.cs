using MobileDataManager.UnitedMobileData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using UMDGeneral.Data;
using UMDGeneral.Interfaces;
using UMDGeneral.Interfaces.DataInterfaces;
using UMDGeneral.Threading;
using static UMDGeneral.Definitions.enums;
using static UMDGeneral.Definitions.MsgTypes;

namespace MobileDeliveryManager.UnitedMobileData
{
    public class UMDManifest {
        static UMDDB UMD_Data;
        string cnn = @"Data Source=UWD-CPT;Initial Catalog=UWDMobileDelivery;User ID=wtssa;Password=1United$WTS";
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

        public UMDManifest()
        {
            UMD_Data = new UMDDB(cnn);
        }

        //public IEnumerable<IMDMMessage> Persist(SPCmds sp, IMDMMessage md)
        //{
        //    // UMD_Data.InsertData(sp, md);
        //    foreach (var it in UMD_Data.InsertData(sp, md))
        //    {
        //        yield return it;
        //    }
        //}

        public IMDMMessage QueryData(Func<byte[], Task> cb, isaCommand dat)
        {
            return UMD_Data.QueryData(cb, dat);
        }

        public IEnumerable<IMDMMessage> Persist(SPCmds sp, IMDMMessage md)
        {
            // UMD_Data.InsertData(sp, md);
            foreach (var it in UMD_Data.InsertData(sp, md))
            {
                yield return it;
            }
        }

        public IEnumerable<IMDMMessage> UploadManifest(IMDMMessage manItem)
        {
            foreach (var it in Persist(SPCmds.INSERTMANIFEST, manItem))
                yield return it;
        }
        public IEnumerable<IMDMMessage> UploadManifestDetails(IMDMMessage manDetItem)
        {
            foreach (var it in Persist(SPCmds.INSERTMANIFESTDETAILS, manDetItem))
                yield return it;
        }
        public IEnumerable<IMDMMessage> UploadOrders(IMDMMessage manOrdItem)
        {
            foreach (var it in Persist(SPCmds.INSERTORDER, manOrdItem))
                yield return it;
        }
        public IEnumerable<IMDMMessage> UploadOrderDetails(IMDMMessage manOrdDetItem)
        {
            foreach (var it in Persist(SPCmds.INSERTORDERDETAILS, manOrdDetItem))
                yield return it;
        }
        public IEnumerable<IMDMMessage> UploadOrderOptions(IMDMMessage manOrdOptItem)
        {
            foreach (var it in Persist(SPCmds.INSERTORDEROPTIONS, manOrdOptItem))
                yield return it;
        }
    }
}
