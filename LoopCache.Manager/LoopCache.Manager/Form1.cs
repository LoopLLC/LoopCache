using LoopCache.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
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
        private Cache Cache { get; set; }
        private long OneGB = (long)(1024 * 1024 * 1024);

        public Form1()
        {
            InitializeComponent();
            this.SetControls();
        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                string hostName = this.txtMasterHostName.Text;
                int port = int.Parse(this.txtMasterPort.Text);
                this.Cache = new Cache(hostName, port);

                this.UpdateRingStatus();
                this.SetControls();

            });
        }

        private void btnPush_Click(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                this.txtException.Text = string.Empty;

                int seed = int.Parse(this.txtSeed.Text);

                List<Tuple<string, Customer>> customers = new List<Tuple<string, Customer>>();

                int count = int.Parse(this.txtObjectCount.Text);

                List<Exception> exceptions = new List<Exception>();

                for (int i = 0; i < count; i++)
                {
                    string key = string.Concat("Customer:", seed);
                    Customer cust = new Customer();
                    cust.Number = seed;
                    cust.Name = string.Concat("Jane Doe ", seed);

                    customers.Add(new Tuple<string, Customer>(key, cust));
                    seed++;
                }

                Stats s = new Stats();
                System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
                watch.Start();

                if (this.chkMultithread.Checked)
                {
                    Parallel.ForEach(customers, customer =>
                    {
                        try
                        {
                            bool success = this.Cache.Set(customer.Item1, customer.Item2);

                            if (success)
                                Interlocked.Increment(ref s.successCount);
                            else
                                Interlocked.Increment(ref s.failCount);
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            Interlocked.Increment(ref s.failCount);
                        }

                        Interlocked.Increment(ref s.totalCount);
                    });
                }
                else
                {
                    foreach (var customer in customers)
                    {
                        try
                        {
                            bool success = this.Cache.Set(customer.Item1, customer.Item2);

                            if (success)
                                s.successCount++;
                            else
                                s.failCount++;
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            s.failCount++;
                        }

                        s.totalCount++;
                    }
                }

                watch.Stop();

                s.totalCount = count;
                s.span = watch.Elapsed;

                this.UpdatePerformanceStatus(s);
                this.UpdateExceptions(exceptions);
            });
        }

        private void btnPull_Click(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                this.txtException.Text = string.Empty;

                int seed = int.Parse(this.txtSeed.Text);

                List<string> keys = new List<string>();

                int count = int.Parse(this.txtObjectCount.Text);

                for (int i = 0; i < count; i++)
                {
                    string key = string.Concat("Customer:", seed);
                    keys.Add(key);
                    seed++;
                }

                List<Exception> exceptions = new List<Exception>();

                Stats s = new Stats();
                System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
                watch.Start();

                if (this.chkMultithread.Checked)
                {
                    Parallel.ForEach(keys, key =>
                    {
                        try
                        {
                            var customer = this.Cache.Get(key);

                            if (customer == null)
                                Interlocked.Increment(ref s.successCount);
                            else
                                Interlocked.Increment(ref s.failCount);
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            Interlocked.Increment(ref s.failCount);
                        }

                        Interlocked.Increment(ref s.totalCount);
                    });
                }
                else
                {
                    foreach (var key in keys)
                    {
                        try
                        {
                            var customer = this.Cache.Get(key);

                            if (customer == null)
                                s.failCount++;
                            else
                                s.successCount++;
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            s.failCount++;
                        }

                        s.totalCount++;
                    }
                }

                watch.Stop();

                s.totalCount = count;
                s.span = watch.Elapsed;

                this.UpdatePerformanceStatus(s);
                this.UpdateExceptions(exceptions);
            });
        }

        private void btnClear_Click(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                this.Cache.Clear();
            });
        }

        private void btnAddNode_Click(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                string hostName = this.txtHostName.Text;
                int port = int.Parse(this.txtPort.Text);
                double multipler = double.Parse(this.txtMultiplier.Text);
                long MaxBytes = (long)(OneGB * multipler);

                this.Cache.Master.AddNode(hostName, port, MaxBytes);

                this.UpdateRingStatus();

                this.txtPort.Text = (port + 1).ToString();
            });
        }

        private void btnRemoveNode_Click(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                string hostName = this.txtHostName.Text;
                int port = int.Parse(this.txtPort.Text);
                this.Cache.Master.RemoveNode(hostName, port);

                this.UpdateRingStatus();

                this.txtPort.Text = (port - 1).ToString();
            });
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            this.DoThis(() =>
            {
                this.UpdateRingStatus();
            });
        }

        private void UpdateExceptions(List<Exception> exs)
        {
            StringBuilder builder = new StringBuilder();

            foreach ( Exception ex in exs)
            {
                builder.Append(ex.Message);
                builder.Append("\r\n");
            }

            this.txtException.Text = builder.ToString();
        }

        private void UpdatePerformanceStatus(Stats stats)
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("Success.....{0}\r\n", stats.successCount);
            builder.AppendFormat("Failure.....{0}\r\n", stats.failCount);
            builder.AppendFormat("Total.......{0}\r\n", stats.totalCount);
            builder.AppendFormat("Time(sec)...{0:0.00}\r\n", stats.span.TotalSeconds);
            builder.AppendFormat("Per Second..{0:0.00}\r\n", stats.OPS);

            this.txtStatus.Text = builder.ToString();
        }

        private void UpdateRingStatus()
        {
            this.Cache.Master.GetConfig();

            this.lstNodes.Items.Clear();
            StringBuilder builder;

            foreach (var node in this.Cache.Master.Nodes)
            {
                Node n = node.Value;

                n.GetStats();

                builder = new StringBuilder();
                builder.Append(n.Name);
                builder.Append(" - ");
                builder.Append(n.Status);
                builder.Append(" - ");
                builder.Append(((double)n.MaxNumBytes / (double)OneGB).ToString("0.00"));
                builder.Append("GB");

                double per = 0;

                if (n.MaxNumBytes > 0)
                    per = ((double)n.LatestRAMBytes / (double)n.MaxNumBytes);

                builder.Append(per.ToString("(0.00%)"));
                builder.Append(" - ");
                builder.Append(n.NumObjects);

                this.lstNodes.Items.Add(builder.ToString());
            }
        }

        private void SetControls()
        {
            bool connected = (this.Cache != null);
            this.btnAddNode.Enabled = connected;
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

        private void DoThis( Action action )
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
            public int Number { get; set; }
            public string Name { get; set; }
        }

        private class Stats
        {
            public int successCount = 0;
            public int failCount = 0;
            public int exceptionCount = 0;
            public int totalCount = 0;
            public TimeSpan span = new TimeSpan();
            public double OPS
            {
                get { return (totalCount / span.TotalSeconds); }
            }
        }
    }
}
