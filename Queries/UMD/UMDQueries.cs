using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MobileDeliveryManger.Queries.UMD
{
    public class UMDQueries
    {
        static string InsertManifest =
            @"Insert into Manifest values ('ManifestId', 'DriverId', 'TRK_CDE', SHP_DTE', 'DESC$', 'NOTES')" +
            " Where ";
    }
}
