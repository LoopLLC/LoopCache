using LoopCacheLib;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace LoopCacheService
{
    public partial class ListenerService : ServiceBase
    {
        private CacheListener listener;

        public ListenerService()
        {
            InitializeComponent();

            AppDomain.CurrentDomain.UnhandledException += UnhandledException;
        }

        private void UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            try
            {
                CacheHelper.LogError("Unhandled Exception.", (Exception)e.ExceptionObject);
            }
            catch { }

            try
            {
                CacheHelper.LogTrace("Unhandled Exception Trace: {0}", e.ExceptionObject);
            }
            catch { }

            if (listener != null)
            {
                this.listener.Stop();
            }
        }

        protected override void OnStart(string[] args)
        {
            string pathToConfigFile = ConfigurationManager.AppSettings["CacheConfigFile"];
            
            this.listener = new CacheListener(pathToConfigFile);

            var task = listener.StartAsync();
        }

        protected override void OnStop()
        {
            if (this.listener != null)
            {
                this.listener.Stop();
            }
        }

    }
}
