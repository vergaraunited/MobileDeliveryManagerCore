using System;
using MobileDeliveryGeneral.Interfaces;
using MobileDeliveryLogger;
using MobileDeliveryGeneral.Settings;
using System.Threading;
using MobileDeliveryServer;
using System.Threading.Tasks;
using MobileDeliveryClient.API;
using MobileDeliverySettings.Settings;

namespace MobileDeliveryManager
{
    class UMDApplicationStartup
    {
        static Logger logger;
        static ManualResetEvent _quitEvent = new ManualResetEvent(false);

        static void Main(string[] args)
        {
            var config = WinformReadSettings.GetSettings(typeof(UMDApplicationStartup));

            logger = new Logger(config.AppName + "_" + config.srvSet.port.ToString(), config.LogPath, config.LogLevel);
            Logger.Info($"Starting {config.AppName} {config.srvSet.port.ToString()} {config.Version} {DateTime.Now}");
            Logger.Info($"Logfile: {config.LogPath}");

            Console.CancelKeyPress += (sender, eArgs) => {
                _quitEvent.Set();
                eArgs.Cancel = true;
            };
            MobileDeliveryManagerAPI det = new MobileDeliveryManagerAPI();

            var ev_name_hook = new ev_name_hook(a => Console.Title = a);
            //det.Subscribe(ev_name_hook);
            Task.Run(() => { det.Init(config, ev_name_hook); });
            Logger.Info($"Connection details {config.AppName}:/n/tUrl:/t{config.srvSet.url}/n/tPort:/t{config.srvSet.port}");
            Server srv = new Server(config.AppName, config.srvSet.url, config.srvSet.port.ToString(), config.LogLevel);
            ProcessMsgDelegateRXRaw pmRx = new ProcessMsgDelegateRXRaw(det.HandleClientCmd);
            srv.Start(pmRx);

            Console.Title = $"{MobileDeliveryManagerAPI.AppName}";

            // kick off asynchronous stuff 
            _quitEvent.WaitOne();
            // cleanup/shutdown and quit
        }

        public static void SetTitle(string val)
        {
            Console.Title=($"{val}");
        }
    }
}
