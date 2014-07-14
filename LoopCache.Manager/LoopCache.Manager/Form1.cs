using LoopCache.Client;
using LoopCache.Admin;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace LoopCache.Manager
{
    public partial class Form1 : Form
    {
        //private Cache Cache { get; set; }
        private CacheAdmin CacheAdmin { get; set; }

        private long OneGB = (long)(1024 * 1024 * 1024);

        public Form1()
        {
            InitializeComponent();
            this.SetControls();
        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                string hostName = this.txtMasterHostName.Text;
                int port = int.Parse(this.txtMasterPort.Text);
                //this.Cache = new Cache(hostName, port);
                this.CacheAdmin = new CacheAdmin(hostName, port);

                this.UpdateRingStatus();
                this.SetControls();

            });
        }

        private void btnPush_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                this.txtStatus.Text = "Working...";
                this.txtException.Text = string.Empty;
                int start = int.Parse(this.txtSeed.Text);
                int count = int.Parse(this.txtObjectCount.Text);
                bool multiThread = this.chkMultithread.Checked;
                var invoker = new UpdateStatsDelegate(Push);
                invoker.BeginInvoke(start, count, multiThread, UpdatePerformanceStatsCallBack, this);
            });
        }

        private void btnPull_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                this.txtStatus.Text = "Working...";
                this.txtException.Text = string.Empty;
                int start = int.Parse(this.txtSeed.Text);
                int count = int.Parse(this.txtObjectCount.Text);
                bool multiThread = this.chkMultithread.Checked;
                var invoker = new UpdateStatsDelegate(Pull);
                invoker.BeginInvoke(start, count, multiThread, UpdatePerformanceStatsCallBack, this);                
            });
        }

        private void btnClear_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                var invoker = new UpdateStatsDelegate(Clear);
                invoker.BeginInvoke(0, 0, true, UpdatePerformanceStatsCallBack, this);
            });
        }

        private void btnAddNode_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                string hostName = this.txtHostName.Text;
                int port = int.Parse(this.txtPort.Text);
                double modiifer = double.Parse(this.txtModifier.Text);
                long maxBytes = (long)(OneGB * modiifer);
                this.CacheAdmin.AddNode(hostName, port, maxBytes);

                this.UpdateRingStatus();

                this.txtPort.Text = (port + 1).ToString();
            });
        }

        private void btnRemoveNode_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                string hostName = this.txtHostName.Text;
                int port = int.Parse(this.txtPort.Text);
                this.CacheAdmin.RemoveNode(hostName, port);

                this.UpdateRingStatus();

                this.txtPort.Text = (port - 1).ToString();
            });
        }

        private void btnUpdateNode_Click(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                string hostName = this.txtHostName.Text;
                int port = int.Parse(this.txtPort.Text);
                double modiifer = double.Parse(this.txtModifier.Text);
                long maxBytes = (long)(OneGB * modiifer);

                this.CacheAdmin.ChangeNode(hostName, port, maxBytes);

                this.UpdateRingStatus();
            });
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            this.DoThisCatchExceptions(() =>
            {
                this.UpdateRingStatus();
            });
        }

        private delegate Stats UpdateStatsDelegate(int start, int count, bool multiThread);

        private delegate void GetConfigDelagate();

        private Stats Clear(int start, int count, bool multiThread)
        {
            Stats s = new Stats();
            this.CacheAdmin.Clear();
            return s;
        }

        private Stats Push(int start, int count, bool multiThread)
        {
            List<Tuple<string, Customer>> customers = new List<Tuple<string, Customer>>();

            for (int i = 0; i < count; i++)
            {
                string key = string.Concat("Customer:", start);
                Customer cust = new Customer();
                cust.Number = Guid.NewGuid();
                cust.FirstName = string.Concat("Jane", start);
                cust.LastName = string.Concat("Doe", start);
                cust.DOB = DateTime.Now;

                customers.Add(new Tuple<string, Customer>(key, cust));
                start++;
            }

            Stats s = new Stats();
            Stopwatch watch = new Stopwatch();
            watch.Start();

            if (multiThread)
            {
                Parallel.ForEach(customers, customer =>
                {
                    try
                    {
                        bool success = this.CacheAdmin.Set(customer.Item1, customer.Item2);

                        if (success)
                            Interlocked.Increment(ref s.successCount);
                        else
                            Interlocked.Increment(ref s.failCount);
                    }
                    catch (Exception ex)
                    {
                        s.exceptions.Add(ex);
                        Interlocked.Increment(ref s.failCount);
                    }
                    finally
                    {
                        Interlocked.Increment(ref s.totalCount);
                    }
                });
            }
            else
            {
                foreach (var customer in customers)
                {
                    try
                    {
                        bool success = this.CacheAdmin.Set(customer.Item1, customer.Item2);

                        if (success)
                            s.successCount++;
                        else
                            s.failCount++;
                    }
                    catch (Exception ex)
                    {
                        s.exceptions.Add(ex);
                        s.failCount++;
                    }
                    finally
                    {
                        s.totalCount++;
                    }
                }
            }

            watch.Stop();

            s.totalCount = count;
            s.span = watch.Elapsed;

            return s;
        }

        private Stats Pull(int start, int count, bool multiThread)
        {
            List<string> keys = new List<string>();

            for (int i = 0; i < count; i++)
            {
                string key = string.Concat("Customer:", start);
                keys.Add(key);
                start++;
            }

            Stats s = new Stats();
            Stopwatch watch = new Stopwatch();
            watch.Start();

            if (multiThread)
            {
                Parallel.ForEach(keys, key =>
                {
                    try
                    {
                        var customer = this.CacheAdmin.Get(key);

                        if (customer == null)
                            Interlocked.Increment(ref s.failCount);
                        else
                            Interlocked.Increment(ref s.successCount);
                    }
                    catch (Exception ex)
                    {
                        s.exceptions.Add(ex);
                        Interlocked.Increment(ref s.failCount);
                    }
                    finally
                    {
                        Interlocked.Increment(ref s.totalCount);
                    }
                });
            }
            else
            {
                foreach (var key in keys)
                {
                    try
                    {
                        var customer = this.CacheAdmin.Get(key);

                        if (customer == null)
                            s.failCount++;
                        else
                            s.successCount++;
                    }
                    catch (Exception ex)
                    {
                        s.exceptions.Add(ex);
                        s.failCount++;
                    }
                    finally
                    {
                        s.totalCount++;
                    }
                }
            }

            watch.Stop();

            s.totalCount = count;
            s.span = watch.Elapsed;

            return s;
        }

        private void UpdateRingStatus()
        {
            var invoker = new GetConfigDelagate(this.CacheAdmin.GetConfig);
            invoker.BeginInvoke(UpdateRingCallBack, this);
        }

        private void UpdatePerformanceStatsCallBack(IAsyncResult ar)
        {
            var result = ar as System.Runtime.Remoting.Messaging.AsyncResult;
            var myDelegate = result.AsyncDelegate as UpdateStatsDelegate;
            Stats stats = myDelegate.EndInvoke(ar);

            MethodInvoker uiUpdater = delegate
            {
                StringBuilder builder = new StringBuilder();
                builder.AppendFormat("Success.....{0}\r\n", stats.successCount);
                builder.AppendFormat("Failure.....{0}\r\n", stats.failCount);
                builder.AppendFormat("Total.......{0}\r\n", stats.totalCount);
                builder.AppendFormat("Time(sec)...{0:0.00}\r\n", stats.span.TotalSeconds);
                builder.AppendFormat("Per Second..{0:0.00}\r\n", stats.OPS);

                this.txtStatus.Text = builder.ToString();

                builder = new StringBuilder();

                int exNum = 1;
                int lineNum = 0;

                foreach (Exception ex in stats.exceptions)
                {
                    lineNum = new StackTrace(ex, true).GetFrame(0).GetFileLineNumber();

                    builder.Append(exNum);
                    builder.Append(". ");
                    builder.Append(lineNum);
                    builder.Append(" - ");
                    builder.Append(ex.TargetSite);
                    builder.Append(" - ");
                    builder.Append(ex.Message);
                    builder.Append("\r\n");
                    exNum++;
                }

                this.txtException.Text = builder.ToString();
            };

            if (this.txtStatus.InvokeRequired)
                Invoke(uiUpdater);
            else
                uiUpdater();
        }

        private void UpdateRingCallBack(IAsyncResult ar)
        {
            var result = ar as System.Runtime.Remoting.Messaging.AsyncResult;
            var myDelegate = result.AsyncDelegate as GetConfigDelagate;
            myDelegate.EndInvoke(ar);

            MethodInvoker uiUpdater = delegate
            {
                this.lstNodes.Items.Clear(); 
                StringBuilder builder;
                Node n;

                double maxGB;
                double latGB;

                List<Node> nodes = CacheAdmin.GetRingStats();

                foreach (var node in nodes)
                {
                    maxGB = ((double)node.MaxNumBytes / (double)OneGB);
                    latGB = ((double)node.LatestRAMBytes / (double)OneGB);

                    double per = 0;

                    if (node.MaxNumBytes > 0)
                        per = (latGB / maxGB);

                    builder = new StringBuilder();
                    builder.Append(node.Name);
                    builder.Append(" - ");
                    builder.Append(node.Status);
                    builder.Append(" - ");
                    builder.Append(latGB.ToString("0.00"));
                    builder.Append("/");
                    builder.Append(maxGB.ToString("0.00"));
                    builder.Append(per.ToString("(0.00%)"));
                    builder.Append(" - ");
                    builder.Append(node.NumObjects);

                    this.lstNodes.Items.Add(builder.ToString());
                }
            };

            if (this.lstNodes.InvokeRequired)
                Invoke(uiUpdater);
            else
                uiUpdater();
        }

        private void SetControls()
        {
            bool connected = (this.CacheAdmin != null);
            this.btnAddNode.Enabled = connected;
            this.btnUpdateNode.Enabled = connected;
            this.btnRemoveNode.Enabled = connected;
            this.btnPushData.Enabled = connected;
            this.btnPull.Enabled = connected;
            this.btnClear.Enabled = connected;
            this.timer1.Enabled = connected;
            this.timer1.Interval = 5000;
            this.pnlMaster.Text = (connected ? "Master (Set)" : "Master (Not Set)");

            if (connected)
                this.timer1.Start();
            else
                this.timer1.Stop();
        }

        private void DoThisCatchExceptions(Action action)
        {

            Cursor.Current = Cursors.WaitCursor;
            Application.DoEvents();

            try
            {
                action();
            }
            catch (Exception e)
            {
                this.txtException.Text = e.Message;
            }

            Cursor.Current = Cursors.Default;
        }

        [Serializable]
        private class Customer
        {
            public Guid Number { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string FullName { get { return string.Join(" ", this.FirstName, this.LastName); } }
            public DateTime DOB { get; set; }
        }

        private class Stats
        {
            public int successCount = 0;
            public int failCount = 0;
            public int totalCount = 0;
            public TimeSpan span = new TimeSpan();
            public List<Exception> exceptions = new List<Exception>();
            public double OPS
            {
                get { return (totalCount / span.TotalSeconds); }
            }
        }
    }
}
